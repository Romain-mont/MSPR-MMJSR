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

def get_engine():
    """Crée l'engine SQLAlchemy (lazy loading)"""
    print(f"🔑 Connexion via ENV : {DB_USER} / **** sur le port {DB_PORT}...")
    DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
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
        # === ETAPE 0 : Nettoyage ===
        if clean_tables:
            print("   🧹 Nettoyage des tables existantes...")
            with engine.begin() as conn:  
                conn.execute(text("TRUNCATE TABLE fact_em, dim_route, dim_vehicle_type RESTART IDENTITY CASCADE;"))
            print("     ✅ Tables vidées.")

        with engine.connect() as conn:
            # === ETAPE A : Remplir DIM_ROUTE (Géographie) ===
            print("   ↳ Traitement des Routes...")
            routes = df.groupby(['origin', 'destination'], as_index=False).agg({
                'distance_km': 'mean'
            })
            routes['is_long_distance'] = routes['distance_km'] > 200
            routes = routes.rename(columns={'origin': 'dep_name', 'destination': 'arr_name'})
            
            routes.to_sql('dim_route', engine, if_exists='append', index=False)
            print(f"     ✅ {len(routes)} routes insérées.")

            # === ETAPE B : Remplir DIM_VEHICLE_TYPE (Matériel) ===
            print("   ↳ Traitement des Véhicules...")
            vehicles = df[['vehicule_type']].drop_duplicates().reset_index(drop=True)
            
            # Labels et facteurs CO2 (kg/100km) selon le type
            def get_label(vt):
                if vt == "Train Jour":
                    return "TGV"
                elif vt == "Train Nuit":
                    return "Intercités Nuit"
                elif vt == "Avion":
                    return "Avion"
                else:
                    return vt
            
            def get_co2_factor(vt):
                # Facteurs en kg CO2 / 100 km
                if vt == "Train Jour":
                    return 0.29   # TGV
                elif vt == "Train Nuit":
                    return 0.9    # Intercité
                elif vt == "Avion":
                    return 18.45  # Moyen courrier (moyenne)
                else:
                    return 0.9    # Default : Intercité
            
            vehicles['label'] = vehicles['vehicule_type'].apply(get_label)
            vehicles['service_type'] = vehicles['vehicule_type']
            vehicles['co2_vt'] = vehicles['vehicule_type'].apply(get_co2_factor)
            
            vehicles_to_insert = vehicles[['label', 'co2_vt', 'service_type']]
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
            
            facts.to_sql('fact_em', engine, if_exists='append', index=False)
            print(f"     ✅ {len(facts)} faits insérés dans FACT_EM.")

            print("\n🎉 SUCCÈS TOTAL : La base de données est remplie !")
            
            # Stats finales
            print("\n📊 Statistiques :")
            print(f"   - Routes : {len(routes)}")
            print(f"   - Véhicules : {len(vehicles)}")
            print(f"   - Faits : {len(facts)}")
            print(f"   - Distance moyenne : {df['distance_km'].mean():.2f} km")
            print(f"   - CO2 moyen : {df['co2_kg'].mean():.3f} kg")

    except Exception as e:
        print(f"❌ Erreur critique SQL : {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_ingestion(clean_tables=True)