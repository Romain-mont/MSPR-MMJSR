import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv  # N'oublie pas cet import
import os
import sys

# 1. Chargement de la configuration
load_dotenv() # Charge le fichier .env à la racine

# Récupération des variables (avec valeurs par défaut au cas où)
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = "127.0.0.1" # On force localhost pour le script Python
DB_PORT = os.getenv("DB_PORT", "5432") 
DB_NAME = os.getenv("DB_NAME")

# Sécurité : Vérifier que tout est chargé
if not DB_USER or not DB_PASS or not DB_NAME:
    print("❌ ERREUR : Les variables d'environnement sont mal chargées.")
    print("Vérifie ton fichier .env à la racine.")
    print(f"DEBUG : User={DB_USER}, DB={DB_NAME}")
    sys.exit(1)

CSV_PATH = "data/MobilityDatabase/data/Europe_Rail_Database.csv"
# (Vérifie bien que ce chemin est toujours le bon chez toi !)

print(f"🔑 Connexion via ENV : {DB_USER} / **** sur le port {DB_PORT}...")

# Connexion à la base via SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# ... Le reste de la fonction run_ingestion ne change pas ...

def run_ingestion(clean_tables=True):
    print("🚀 Démarrage de l'ingestion des données...")

    if not os.path.exists(CSV_PATH):
        print(f"❌ Erreur : Fichier introuvable {CSV_PATH}")
        return

    # 2. Lecture du CSV (Tout d'un coup - Plus sûr pour les relations)
    try:
        df = pd.read_csv(CSV_PATH)
        print(f"📦 CSV chargé : {len(df)} lignes trouvées.")
    except Exception as e:
        print(f"❌ Erreur de lecture CSV : {e}")
        return

    try:
        # === ETAPE 0 : Nettoyage (Une seule fois au début) ===
        if clean_tables:
            print("   🧹 Nettoyage des tables existantes...")
            with engine.begin() as conn:  # begin() fait un auto-commit à la fin
                conn.execute(text("TRUNCATE TABLE fact_em, dim_route, dim_vehicle_type RESTART IDENTITY CASCADE;"))
            print("     ✅ Tables vidées et commité.")

        with engine.connect() as conn:
            # === ETAPE A : Remplir DIM_ROUTE (Géographie) ===
            print("   ↳ Traitement des Routes...")
            # Agréger pour éviter les doublons : garder la distance moyenne par paire origine/destination
            routes = df.groupby(['origin', 'destination'], as_index=False).agg({
                'distance_km': 'mean'  # Moyenne des distances (gère les variations de gares)
            })
            routes['is_long_distance'] = routes['distance_km'] > 200
            routes = routes.rename(columns={'origin': 'dep_name', 'destination': 'arr_name'})
            
            # Insertion
            routes.to_sql('dim_route', engine, if_exists='append', index=False)
            print(f"     ✅ {len(routes)} routes insérées.")

            # === ETAPE B : Remplir DIM_VEHICLE_TYPE (Matériel) ===
            print("   ↳ Traitement des Véhicules...")
            vehicles = df[['train_type']].drop_duplicates().reset_index(drop=True)
            vehicles['label'] = vehicles['train_type'].apply(lambda x: "TGV/Intercités" if x == "Jour" else "Intercités Nuit")
            vehicles['service_type'] = vehicles['train_type']
            vehicles['co2_vt'] = vehicles['train_type'].apply(lambda x: 4.5 if x == "Jour" else 12.0)
            
            vehicles_to_insert = vehicles[['label', 'co2_vt', 'service_type']]
            vehicles_to_insert.to_sql('dim_vehicle_type', engine, if_exists='append', index=False)
            print(f"     ✅ {len(vehicles)} types de véhicules insérés.")

            # === ETAPE C : Remplir FACT_EM (Les Faits) ===
            print("   ↳ Création des liens (Jointures)...")
            
            # Récupération des IDs générés par Postgres
            sql_routes = pd.read_sql("SELECT route_id, dep_name, arr_name FROM dim_route", engine)
            sql_vehicles = pd.read_sql("SELECT vehicle_type_id, service_type FROM dim_vehicle_type", engine)
            
            # Jointures Pandas pour remplacer les noms par des IDs
            merged = df.merge(sql_routes, left_on=['origin', 'destination'], right_on=['dep_name', 'arr_name'])
            merged = merged.merge(sql_vehicles, left_on='train_type', right_on='service_type')
            
            # Préparation finale
            facts = merged[['route_id', 'vehicle_type_id', 'co2_kg']].copy()
            facts = facts.rename(columns={'co2_kg': 'co2_kg_passenger'})
            
            facts.to_sql('fact_em', engine, if_exists='append', index=False)
            print(f"     ✅ {len(facts)} faits insérés dans FACT_EM.")

            print("\n🎉 SUCCÈS TOTAL : La base de données est remplie !")

    except Exception as e:
        print(f"❌ Erreur critique SQL : {e}")

if __name__ == "__main__":
    run_ingestion(clean_tables=True)