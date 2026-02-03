import os
import shutil
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# === CONFIGURATION ===
DATA_DIR = "./data/raw"
GLOBAL_STAGING_DIR = "./data/staging_global"
INPUT_PATH = "data/Europe_Rail_Database"
OUTPUT_PATH = "data/Europe_Rail_Database_clean"

# === FONCTIONS DE NETTOYAGE ===

def clean_initial():
    """Nettoyage initial complet des répertoires"""
    print("Nettoyage initial...")
    for d in [DATA_DIR, GLOBAL_STAGING_DIR, "./temp_unzip", "./data/temp_parquet"]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    print("Répertoires initialisés")

def clean_temp_files(zip_path, temp_unzip_path):
    """Nettoyage des fichiers temporaires après traitement"""
    if os.path.exists(zip_path):
        try:
            os.remove(zip_path)
        except:
            pass
    
    if os.path.exists(temp_unzip_path):
        shutil.rmtree(temp_unzip_path, ignore_errors=True)

def clean_final():
    """Nettoyage final complet"""
    print("Nettoyage final...")
    shutil.rmtree(GLOBAL_STAGING_DIR, ignore_errors=True)
    print("Nettoyage terminé")

def _get_spark():
    return SparkSession.builder \
        .appName("EuroRailClean") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def run_clean():
    """Nettoyage grossier des datasets inexploitables"""
    print("=== LANCEMENT CLEAN GROSSIER ===")

    if not os.path.exists(INPUT_PATH):
        print(f"Dossier d'entrée introuvable : {INPUT_PATH}")
        return False

    spark = _get_spark()
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
        initial_count = df.count()

        # Suppression des lignes inexploitables (valeurs nulles)
        required_cols = [
            "provider",
            "country",
            "trip_id",
            "origin",
            "destination",
            "dep_time_first",
            "arr_time_last",
            "distance_km",
        ]
        existing_required = [c for c in required_cols if c in df.columns]
        df_clean = df.dropna(subset=existing_required)

        # Filtre distances invalides
        if "distance_km" in df_clean.columns:
            df_clean = df_clean.filter(col("distance_km") > 0)

        # Déduplication grossière
        if "trip_id" in df_clean.columns:
            df_clean = df_clean.dropDuplicates(["trip_id"])

        cleaned_count = df_clean.count()
        print(f"Lignes initiales : {initial_count}")
        print(f"Lignes conservées : {cleaned_count}")

        df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)
        print(f"Sortie clean grossier : {OUTPUT_PATH}")
        return True
    except Exception as e:
        print(f"Erreur clean grossier : {e}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    run_clean()


# === BACK-ON-TRACK (INTÉGRATION CLEAN LOCAL) ===
def process_backontrack_local(
    agencies_path="./data/BOT/back_on_track_agencies.csv",
    routes_path="./data/BOT/back_on_track_routes.csv",
    trips_path="./data/BOT/back_on_track_trips.csv",
    stops_path="./data/BOT/back_on_track_stops.csv",
    trip_stops_path="./data/BOT/back_on_track_trip_stop.csv",
    output_dir="./data/processed",
):
    """Nettoyage et transformation Back-on-Track (local)"""
    print("Début de l'assemblage du puzzle Back-on-Track...")

    try:
        agencies = pd.read_csv(agencies_path)
        routes = pd.read_csv(routes_path)
        trips = pd.read_csv(trips_path)
        stops = pd.read_csv(stops_path)
        trip_stops = pd.read_csv(trip_stops_path)

        trips = trips.loc[:, ~trips.columns.str.contains('^Unnamed')]
        trip_stops = trip_stops.loc[:, ~trip_stops.columns.str.contains('^Unnamed')]

        print(f"{len(agencies)} agences, {len(routes)} routes, {len(trips)} trips")
        print(f"{len(stops)} gares, {len(trip_stops)} arrêts")

    except FileNotFoundError as e:
        print(f"Fichier manquant : {e}")
        return False

    route_type_mapping = {
        0: "Tramway",
        1: "Métro",
        2: "Train régional",
        3: "Bus",
        100: "Train longue distance",
        101: "Train grande vitesse",
        102: "Train de nuit",
    }

    print("Fusion des données...")
    routes_full = routes.merge(agencies, on='agency_id', how='left')
    trips_full = trips.merge(routes_full, on='route_id', how='left')
    data = trip_stops.merge(trips_full, on='trip_id', how='left')
    data = data.merge(stops, on='stop_id', how='left')

    print(f"Dataset fusionné : {len(data)} lignes")

    print("Création des trajets origine-destination...")
    data = data.sort_values(['trip_id', 'stop_sequence'])
    origins = data.groupby('trip_id').first().reset_index()
    destinations = data.groupby('trip_id').last().reset_index()

    origins['origin_country'] = origins['countries'].apply(
        lambda x: str(x).split(',')[0].strip() if pd.notna(x) else 'XX'
    )
    destinations['destination_country'] = destinations['countries'].apply(
        lambda x: str(x).split(',')[-1].strip() if pd.notna(x) else 'XX'
    )

    df_final = pd.DataFrame({
        'trip_id': origins['trip_id'],
        'agency_name': origins['agency_name'],
        'route_name': origins['route_long_name'].fillna(
            origins['route_short_name']
        ).fillna(
            origins['trip_origin'] + " - " + origins['trip_headsign']
        ),
        'train_type': origins['route_type'].map(route_type_mapping).fillna("Train de nuit"),
        'service_type': origins.apply(
            lambda x: 'International' if ',' in str(x.get('countries', '')) else 'National',
            axis=1
        ),
        'origin_stop_name': origins['stop_name'],
        'origin_country': origins['origin_country'],
        'destination_stop_name': destinations['stop_name'],
        'destination_country': destinations['destination_country'],
        'departure_time': origins['departure_time'],
        'arrival_time': destinations['arrival_time'],
        'distance_km': origins.get('distance', None),
        'duration_h': origins.get('duration', None),
        'emission_gco2e_pkm': origins.get('co2_per_km', None),
        'total_emission_kgco2e': origins.get('emissions_co2e', None),
        'source_dataset': 'Back-on-Track'
    })

    df_final = df_final.dropna(subset=['origin_stop_name', 'destination_stop_name'])
    print(f"{len(df_final)} trajets complets créés")

    print("Traitement des valeurs manquantes...")
    missing_before = df_final.isnull().sum()

    mean_emissions = df_final.groupby('train_type')['emission_gco2e_pkm'].mean()

    def _get_emission_value(row):
        if pd.notna(row['emission_gco2e_pkm']):
            return row['emission_gco2e_pkm']
        mean_val = mean_emissions.get(row['train_type'])
        if pd.notna(mean_val):
            return mean_val
        if row['train_type'] == 'Train de nuit':
            return 14.0
        return 17.0

    df_final['emission_gco2e_pkm'] = df_final.apply(_get_emission_value, axis=1)

    df_final['total_emission_kgco2e'] = df_final.apply(
        lambda x: round((x['distance_km'] * x['emission_gco2e_pkm']) / 1000, 2)
        if pd.notna(x['distance_km']) and pd.notna(x['emission_gco2e_pkm'])
        else x['total_emission_kgco2e'],
        axis=1
    )

    df_final['agency_name'] = df_final['agency_name'].fillna('Opérateur non renseigné')
    df_final['route_name'] = df_final['route_name'].fillna('Non renseigné')

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "clean_backontrack_final.csv")
    df_final.to_csv(output_path, index=False)
    print(f"Fichier sauvegardé : {output_path}")

    quality_report = pd.DataFrame({
        'Champ': df_final.columns,
        'Valeurs_manquantes': df_final.isnull().sum().values,
        'Taux_completude_%': ((len(df_final) - df_final.isnull().sum()) / len(df_final) * 100).round(1).values
    })
    quality_report_path = os.path.join(output_dir, "quality_report_backontrack.csv")
    quality_report.to_csv(quality_report_path, index=False)
    print(f"Rapport qualité sauvegardé : {quality_report_path}")
    return True
