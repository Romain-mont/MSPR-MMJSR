import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

if not DB_USER or not DB_PASS or not DB_NAME:
    print("ERREUR : variables d'environnement manquantes (.env)")
    print(f"DEBUG : User={DB_USER}, DB={DB_NAME}")
    sys.exit(1)

STAGING_DIR = os.environ.get("FINAL_OUTPUT_DIR", "data/staging")

DIM_ROUTE_CSV   = os.environ.get("DIM_ROUTE_CSV",   os.path.join(STAGING_DIR, "staging_dim_route.csv"))
DIM_VEHICLE_CSV = os.environ.get("DIM_VEHICLE_CSV", os.path.join(STAGING_DIR, "staging_dim_vehicle_type.csv"))
DIM_STATION_CSV = os.environ.get("DIM_STATION_CSV", os.path.join(STAGING_DIR, "staging_dim_station_frequentation.csv"))
FACT_CSV        = os.environ.get("FACT_CSV",        os.path.join(STAGING_DIR, "staging_fact_route_analysis.csv"))


def get_engine():
    print(f"Connexion : {DB_USER} @ {DB_HOST}:{DB_PORT}/{DB_NAME}")
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def run_ingestion(clean_tables=True):
    print("Démarrage de l'ingestion...")
    engine = get_engine()

    def load_csv(path, name):
        if not os.path.exists(path):
            print(f"WARN : fichier introuvable {path}, table {name} ignorée")
            return None
        df = pd.read_csv(path)
        print(f"{name} : {len(df)} lignes chargées")
        return df

    df_route   = load_csv(DIM_ROUTE_CSV,   "dim_route")
    df_vehicle = load_csv(DIM_VEHICLE_CSV, "dim_vehicle_type")
    df_station = load_csv(DIM_STATION_CSV, "dim_station_frequentation")
    df_fact    = load_csv(FACT_CSV,        "fact_route_analysis")

    if df_route is None or df_fact is None:
        print("ERREUR : fichiers dim_route ou fact_route_analysis manquants, arrêt.")
        return

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE dim_route ADD COLUMN IF NOT EXISTS dep_city VARCHAR(255);
                ALTER TABLE dim_route ADD COLUMN IF NOT EXISTS arr_city VARCHAR(255);
            """))

        if clean_tables:
            print("MODE RESET : suppression des données existantes...")
            with engine.begin() as conn:
                conn.execute(text("""
                    TRUNCATE TABLE fact_route_analysis, dim_route,
                                   dim_vehicle_type, dim_station_frequentation
                    RESTART IDENTITY CASCADE;
                """))
            print("Tables vidées.")

        # 1. DIM_STATION_FREQUENTATION
        if df_station is not None:
            df_station = df_station.dropna(subset=["station_name"])
            df_station.to_sql("dim_station_frequentation", engine, if_exists="append", index=False)
            print(f"{len(df_station)} stations insérées.")

        # 2. DIM_ROUTE
        df_route = df_route.dropna(subset=["dep_name", "arr_name"])
        df_route["distance_km"] = pd.to_numeric(df_route["distance_km"], errors="coerce")
        df_route.to_sql("dim_route", engine, if_exists="append", index=False)
        print(f"{len(df_route)} routes insérées.")

        # 3. DIM_VEHICLE_TYPE
        if df_vehicle is not None:
            def get_co2_factor(vt):
                factors = {
                    "TGV": 0.29, "ICE": 0.29, "AVE": 0.29,
                    "Frecciarossa": 0.29, "Frecciargento": 0.29,
                    "InterCity": 0.9, "EuroCity": 0.9,
                    "EuroNight": 0.9, "Nightjet": 0.9,
                    "Train Jour": 0.9, "Train Nuit": 0.9,
                    "Train Longue Distance": 0.9, "Train Longue Distance Nuit": 0.9,
                    "Intercités Nuit": 0.9,
                }
                return factors.get(str(vt), 0.9)

            df_vehicle["co2_vt"] = df_vehicle["service_type"].apply(get_co2_factor)
            df_vehicle.to_sql("dim_vehicle_type", engine, if_exists="append", index=False)
            print(f"{len(df_vehicle)} types de véhicules insérés.")

        # 4. FACT_ROUTE_ANALYSIS
        print("Construction de la table de faits...")
        with engine.connect() as conn:
            sql_routes   = pd.read_sql("SELECT route_id, dep_name, arr_name FROM dim_route", conn)
            sql_vehicles = pd.read_sql("SELECT vehicle_type_id, service_type FROM dim_vehicle_type", conn)
            sql_stations = pd.read_sql("SELECT station_id, station_name FROM dim_station_frequentation", conn) \
                if df_station is not None else pd.DataFrame(columns=["station_id", "station_name"])

        df_fact = df_fact.merge(
            sql_routes, left_on=["origin", "destination"],
            right_on=["dep_name", "arr_name"], how="left"
        )

        if "vehicule_type" in df_fact.columns:
            df_fact = df_fact.merge(
                sql_vehicles, left_on="vehicule_type",
                right_on="service_type", how="left"
            )

        if len(sql_stations) > 0:
            df_fact = df_fact.merge(
                sql_stations.rename(columns={"station_id": "origin_station_id", "station_name": "_s_o"}),
                left_on="origin", right_on="_s_o", how="left"
            ).drop(columns=["_s_o"], errors="ignore")
            df_fact = df_fact.merge(
                sql_stations.rename(columns={"station_id": "dest_station_id", "station_name": "_s_d"}),
                left_on="destination", right_on="_s_d", how="left"
            ).drop(columns=["_s_d"], errors="ignore")
        else:
            df_fact["origin_station_id"] = None
            df_fact["dest_station_id"]   = None

        for col in ["co2_train_kg", "co2_avion_kg", "co2_saved_kg", "traffic_share_pct", "distance_km"]:
            if col in df_fact.columns:
                df_fact[col] = pd.to_numeric(df_fact[col], errors="coerce")

        if "is_substitutable" in df_fact.columns:
            df_fact["is_substitutable"] = pd.to_numeric(
                df_fact["is_substitutable"], errors="coerce"
            ).fillna(0).astype(int)

        fact_cols = [
            "route_id", "vehicle_type_id",
            "origin_station_id", "dest_station_id",
            "co2_train_kg", "co2_avion_kg", "co2_saved_kg",
            "traffic_share_pct", "is_substitutable"
        ]
        df_fact_insert = df_fact[[c for c in fact_cols if c in df_fact.columns]]
        df_fact_insert = df_fact_insert.dropna(subset=["route_id"])
        df_fact_insert.to_sql("fact_route_analysis", engine, if_exists="append", index=False)
        print(f"{len(df_fact_insert)} faits insérés dans fact_route_analysis.")

        print("SUCCÈS : base de données chargée !")
        print(f"  Routes    : {len(df_route)}")
        print(f"  Véhicules : {len(df_vehicle) if df_vehicle is not None else 0}")
        print(f"  Stations  : {len(df_station) if df_station is not None else 0}")
        print(f"  Faits     : {len(df_fact_insert)}")

    except Exception as e:
        print(f"ERREUR SQL : {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_ingestion(clean_tables=True)
