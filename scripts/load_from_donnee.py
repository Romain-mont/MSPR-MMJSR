"""
Charge staging_fact_route_analysis.csv (46k lignes, format flat/dénormalisé)
directement dans PostgreSQL en construisant les dim tables à la volée.
"""
import pandas as pd
from sqlalchemy import create_engine, text
import os

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "mspr_db")
DB_USER = os.getenv("DB_USER", "mspr_user")
DB_PASS = os.getenv("DB_PASSWORD", "mspr_password")
CSV     = os.getenv("FACT_CSV", "/app/staging_fact_route_analysis.csv")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

print(f"Lecture : {CSV}")
df = pd.read_csv(CSV)
print(f"{len(df)} lignes lues")

with engine.begin() as conn:
    conn.execute(text("""
        TRUNCATE TABLE fact_route_analysis, dim_route,
                       dim_vehicle_type, dim_station_frequentation
        RESTART IDENTITY CASCADE;
    """))
print("Tables vidées")

# ── 1. dim_vehicle_type ───────────────────────────────────────────────────────
vt = pd.DataFrame({"service_type": df["vehicule_type"].unique()})
vt["label"] = vt["service_type"]
vt.to_sql("dim_vehicle_type", engine, if_exists="append", index=False)
print(f"{len(vt)} types de véhicules insérés")

# ── 2. dim_station_frequentation ──────────────────────────────────────────────
orig_stations = df[["origin", "origin_station_traffic", "origin_city_population"]].rename(
    columns={"origin": "station_name", "origin_station_traffic": "annual_station_traffic",
             "origin_city_population": "city_population"})
dest_stations = df[["destination", "dest_station_traffic", "dest_city_population"]].rename(
    columns={"destination": "station_name", "dest_station_traffic": "annual_station_traffic",
             "dest_city_population": "city_population"})
stations = pd.concat([orig_stations, dest_stations]).drop_duplicates("station_name").dropna(subset=["station_name"])
stations.to_sql("dim_station_frequentation", engine, if_exists="append", index=False)
print(f"{len(stations)} stations insérées")

# ── 3. dim_route ──────────────────────────────────────────────────────────────
routes = df[["origin", "destination", "distance_km", "origin_city", "destination_city"]].rename(columns={
    "origin": "dep_name", "destination": "arr_name",
    "origin_city": "dep_city", "destination_city": "arr_city"
}).drop_duplicates(subset=["dep_name", "arr_name"])
routes.to_sql("dim_route", engine, if_exists="append", index=False)
print(f"{len(routes)} routes insérées")

# ── 4. fact_route_analysis ────────────────────────────────────────────────────
with engine.connect() as conn:
    sql_routes   = pd.read_sql("SELECT route_id, dep_name, arr_name FROM dim_route", conn)
    sql_vehicles = pd.read_sql("SELECT vehicle_type_id, service_type FROM dim_vehicle_type", conn)
    sql_stations = pd.read_sql("SELECT station_id, station_name FROM dim_station_frequentation", conn)

df = df.merge(sql_routes, left_on=["origin", "destination"],
              right_on=["dep_name", "arr_name"], how="left")
df = df.merge(sql_vehicles, left_on="vehicule_type", right_on="service_type", how="left")
df = df.merge(sql_stations.rename(columns={"station_id": "origin_station_id", "station_name": "_so"}),
              left_on="origin", right_on="_so", how="left").drop(columns=["_so"], errors="ignore")
df = df.merge(sql_stations.rename(columns={"station_id": "dest_station_id", "station_name": "_sd"}),
              left_on="destination", right_on="_sd", how="left").drop(columns=["_sd"], errors="ignore")

for col in ["co2_train_kg", "co2_avion_kg", "co2_saved_kg", "distance_km"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
df["is_substitutable"] = pd.to_numeric(df["is_substitutable"], errors="coerce").fillna(0).astype(int)

fact_cols = ["route_id", "vehicle_type_id", "origin_station_id", "dest_station_id",
             "co2_train_kg", "co2_avion_kg", "co2_saved_kg", "is_substitutable",
             "trip_count_corridor", "trip_count_origin", "service_share"]
insert = df[[c for c in fact_cols if c in df.columns]].dropna(subset=["route_id"])
insert.to_sql("fact_route_analysis", engine, if_exists="append", index=False)

print(f"{len(insert)} faits insérés")
print("SUCCÈS")
