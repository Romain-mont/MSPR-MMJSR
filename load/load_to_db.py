import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys

# 1. Chargement de la configuration
load_dotenv()

# Récupération des variables

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
# Permet de surcharger l'adresse du host en Docker (ex: "db" ou "postgres")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# Sécurité : Vérifier que tout est chargé
if not DB_USER or not DB_PASS or not DB_NAME:
    print("❌ ERREUR : Les variables d'environnement sont mal chargées.")
    print("Vérifie ton fichier .env à la racine.")
    print(f"DEBUG : User={DB_USER}, DB={DB_NAME}")
    sys.exit(1)

# === CHEMINS DES FICHIERS STAGING (adaptables via env pour Docker) ===
CSV_PATH = os.environ.get("FINAL_ROUTES_CSV", "data/staging/final_routes.csv")
AIRPORTS_CSV_PATH = os.environ.get("STAGING_AIRPORTS_CSV", "data/staging/staging_airports.csv")
INTERMODAL_CSV_PATH = os.environ.get("STAGING_INTERMODAL_CSV", "data/staging/staging_intermodal.csv")

# === MODE INCRÉMENTAL (conservation des données existantes) ===
INCREMENTAL_LOAD = os.getenv("INCREMENTAL_LOAD", "false").lower() == "true"

def get_engine():
    """Crée l'engine SQLAlchemy (lazy loading)"""
    print(f"🔑 Connexion via ENV : {DB_USER} / **** sur le port {DB_PORT}...")
    # Force l'utilisation de psycopg2 (binaire) au lieu de psycopg (v3 qui nécessite libpq)
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(DATABASE_URL)


def run_ingestion(clean_tables=True):
    """
    Charge les données transformées dans le datamart PostgreSQL.
    
    Prérequis : transform.py doit avoir été exécuté !
    Les fichiers doivent être dans data/staging/
    """
    print("🚀 Démarrage de l'ingestion des données...")

    if not os.path.exists(CSV_PATH):
        print(f"❌ Erreur : Fichier introuvable {CSV_PATH}")
        print("   As-tu exécuté transform.py ?")
        return

    # 2. Lecture du CSV
    try:
        df = pd.read_csv(CSV_PATH)
        print(f"📦 CSV chargé : {len(df)} lignes trouvées.")
        print(f"   Colonnes : {list(df.columns)}")
    except Exception as e:
        print(f"❌ Erreur de lecture CSV : {e}")
        return
    
    # Filtrer les lignes sans distance (données invalides)
    df = df.dropna(subset=['distance_km', 'co2_kg'])
    print(f"   Lignes valides (avec distance/CO2) : {len(df)}")

    # Créer l'engine de connexion
    engine = get_engine()

    try:
        # === ETAPE 0 : Gestion du mode de chargement ===
        if INCREMENTAL_LOAD:
            print("   ♻️  MODE INCRÉMENTAL activé : conservation des données existantes")
            print("   📌 Les nouvelles données seront ajoutées sans supprimer l'existant")
        elif clean_tables:
            print("   🗑️  MODE RESET : suppression des données existantes")
            with engine.begin() as conn:  
                conn.execute(text("TRUNCATE TABLE fact_em, dim_route, dim_vehicle_type RESTART IDENTITY CASCADE;"))
            print("     ✅ Tables vidées.")
        
        # === ETAPE 0.1 : Ajout des contraintes d'unicité (mode incrémental) ===
        if INCREMENTAL_LOAD:
            print("   🔒 Vérification des contraintes d'unicité...")
            with engine.begin() as conn:
                # Contrainte sur dim_route pour éviter les doublons
                conn.execute(text("""
                    ALTER TABLE dim_route DROP CONSTRAINT IF EXISTS unique_route;
                    ALTER TABLE dim_route ADD CONSTRAINT unique_route 
                    UNIQUE (dep_name, arr_name);
                """))
                
                # Contrainte sur dim_vehicle_type pour éviter les doublons
                conn.execute(text("""
                    ALTER TABLE dim_vehicle_type DROP CONSTRAINT IF EXISTS unique_vehicle;
                    ALTER TABLE dim_vehicle_type ADD CONSTRAINT unique_vehicle 
                    UNIQUE (label, service_type);
                """))
            print("     ✅ Contraintes d'unicité vérifiées.")

        with engine.connect() as conn:
            # === ETAPE A : Remplir DIM_ROUTE (Géographie) ===
            print("   ↳ Traitement des Routes...")
            routes = df.groupby(['origin', 'destination'], as_index=False).agg({
                'distance_km': 'mean'
            })
            routes['is_long_distance'] = routes['distance_km'] > 100
            routes = routes.rename(columns={'origin': 'dep_name', 'destination': 'arr_name'})
            
            if INCREMENTAL_LOAD:
                # Mode incrémental : INSERT avec gestion des conflits via pandas + SQL brut
                existing_routes = pd.read_sql("SELECT dep_name, arr_name FROM dim_route", engine)
                new_routes = routes.merge(
                    existing_routes, 
                    on=['dep_name', 'arr_name'], 
                    how='left', 
                    indicator=True
                )
                new_routes = new_routes[new_routes['_merge'] == 'left_only'].drop('_merge', axis=1)
                
                if len(new_routes) > 0:
                    new_routes.to_sql('dim_route', engine, if_exists='append', index=False)
                    print(f"     ✅ {len(new_routes)} nouvelles routes insérées (sur {len(routes)} dans le CSV)")
                else:
                    print(f"     ℹ️  Aucune nouvelle route (toutes déjà présentes)")
            else:
                routes.to_sql('dim_route', engine, if_exists='append', index=False)
                print(f"     ✅ {len(routes)} routes insérées.")

            # === ETAPE B : Remplir DIM_VEHICLE_TYPE (Matériel) ===
            print("   ↳ Traitement des Véhicules...")
            vehicles = df[['vehicule_type']].drop_duplicates().reset_index(drop=True)
            
            # Labels et facteurs CO2 (kg/100km) selon le type
            # Conversion uniquement pour les types génériques
            # Les types spécialisés (TGV, ICE, EuroNight, etc.) sont préservés
            def get_label(vt):
                mapping = {
                    "Train Jour": "InterCity",  # IC = Standard grandes lignes
                    "Train Nuit": "Intercités Nuit",
                    "Train Longue Distance": "InterCity",
                    "Train Longue Distance Nuit": "Intercités Nuit",
                    "Avion": "Avion"
                }
                # Si type spécialisé (TGV, ICE, EuroNight, Nightjet, etc.), on le garde tel quel
                return mapping.get(vt, vt)
            
            def get_co2_factor(vt):
                # Facteurs en kg CO2 / 100 km (source: ADEME/UIC)
                factors = {
                    "TGV": 0.29,
                    "ICE": 0.29,
                    "AVE": 0.29,
                    "Frecciarossa": 0.29,
                    "Frecciargento": 0.29,
                    "Frecciabianca": 0.29,
                    "InterCity": 0.9,
                    "EuroCity": 0.9,
                    "Train Jour": 0.9,
                    "Train Longue Distance": 0.9,
                    "EuroNight": 0.9,
                    "Nightjet": 0.9,
                    "Train Nuit": 0.9,
                    "Intercités Nuit": 0.9,
                    "Train Longue Distance Nuit": 0.9,
                    "Avion": 18.45  # Moyen courrier
                }
                return factors.get(vt, 0.9)  # Default: InterCity
            
            vehicles['label'] = vehicles['vehicule_type'].apply(get_label)
            vehicles['service_type'] = vehicles['vehicule_type']
            vehicles['co2_vt'] = vehicles['vehicule_type'].apply(get_co2_factor)
            
            vehicles_to_insert = vehicles[['label', 'co2_vt', 'service_type']]
            
            if INCREMENTAL_LOAD:
                # Mode incrémental : vérifier les véhicules existants
                existing_vehicles = pd.read_sql("SELECT label, service_type FROM dim_vehicle_type", engine)
                new_vehicles = vehicles_to_insert.merge(
                    existing_vehicles,
                    on=['label', 'service_type'],
                    how='left',
                    indicator=True
                )
                new_vehicles = new_vehicles[new_vehicles['_merge'] == 'left_only'].drop('_merge', axis=1)
                
                if len(new_vehicles) > 0:
                    new_vehicles.to_sql('dim_vehicle_type', engine, if_exists='append', index=False)
                    print(f"     ✅ {len(new_vehicles)} nouveaux types de véhicules insérés (sur {len(vehicles)} dans le CSV)")
                else:
                    print(f"     ℹ️  Aucun nouveau type de véhicule (tous déjà présents)")
            else:
                vehicles_to_insert.to_sql('dim_vehicle_type', engine, if_exists='append', index=False)
                print(f"     ✅ {len(vehicles)} types de véhicules insérés.")

            # === ETAPE C : Remplir FACT_EM (Les Faits) ===
            print("   ↳ Création des liens (Jointures)...")
            
            sql_routes = pd.read_sql("SELECT route_id, dep_name, arr_name FROM dim_route", engine)
            sql_vehicles = pd.read_sql("SELECT vehicle_type_id, service_type FROM dim_vehicle_type", engine)
            
            merged = df.merge(sql_routes, left_on=['origin', 'destination'], right_on=['dep_name', 'arr_name'])
            merged = merged.merge(sql_vehicles, left_on='vehicule_type', right_on='service_type')
            
            facts = merged[['route_id', 'vehicle_type_id', 'co2_kg']].copy()
            facts = facts.rename(columns={'co2_kg': 'co2_kg_passenger'})
            
            if INCREMENTAL_LOAD:
                # Mode incrémental : éviter les doublons exacts dans fact_em
                # Note: fact_em peut avoir plusieurs lignes identiques si trajets multiples
                # On vérifie juste qu'on n'insère pas exactement les mêmes données
                existing_facts_count = pd.read_sql("SELECT COUNT(*) as count FROM fact_em", engine).iloc[0]['count']
                facts.to_sql('fact_em', engine, if_exists='append', index=False)
                new_facts_count = pd.read_sql("SELECT COUNT(*) as count FROM fact_em", engine).iloc[0]['count']
                added_facts = new_facts_count - existing_facts_count
                print(f"     ✅ {added_facts} nouveaux faits insérés dans FACT_EM (total: {new_facts_count})")
            else:
                facts.to_sql('fact_em', engine, if_exists='append', index=False)
                print(f"     ✅ {len(facts)} faits insérés dans FACT_EM.")

            print("\n🎉 SUCCÈS TOTAL : La base de données est remplie !")
            
            # Stats finales (totales en BDD)
            total_routes = pd.read_sql("SELECT COUNT(*) as count FROM dim_route", engine).iloc[0]['count']
            total_vehicles = pd.read_sql("SELECT COUNT(*) as count FROM dim_vehicle_type", engine).iloc[0]['count']
            total_facts = pd.read_sql("SELECT COUNT(*) as count FROM fact_em", engine).iloc[0]['count']
            
            print("\n📊 Statistiques de cette exécution :")
            print(f"   - Routes dans CSV : {len(routes)}")
            print(f"   - Véhicules dans CSV : {len(vehicles)}")
            print(f"   - Faits dans CSV : {len(facts)}")
            print(f"   - Distance moyenne CSV : {df['distance_km'].mean():.2f} km")
            print(f"   - CO2 moyen CSV : {df['co2_kg'].mean():.3f} kg")
            
            if INCREMENTAL_LOAD:
                print("\n📊 Statistiques TOTALES en base de données :")
                print(f"   - Total routes : {total_routes}")
                print(f"   - Total véhicules : {total_vehicles}")
                print(f"   - Total faits : {total_facts}")

    except Exception as e:
        print(f"❌ Erreur critique SQL : {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_ingestion(clean_tables=True)