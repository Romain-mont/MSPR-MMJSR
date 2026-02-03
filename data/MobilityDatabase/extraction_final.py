import requests
import os
import re
import zipfile
import math
import shutil
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round as spark_round, udf, first, last, lit
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window

# === CONFIGURATION ===
load_dotenv()

# Initialiser SparkSession (Optimisée pour le Local)
spark = SparkSession.builder \
    .appName("EuroRailETL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

API_URL = "https://api.mobilitydatabase.org/v1"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

# Pays cibles
TARGET_COUNTRIES = ["FR", "CH", "DE"] # Ajoute les pays que tu veux
DATA_DIR = "./data/raw"
OUTPUT_FILE = "data/Europe_Rail_Database.csv"
GLOBAL_STAGING_DIR = "./data/staging_global"

# Filtres API
EXCLUDE_KEYWORDS = ["bus", "shuttle", "tram", "metro", "urbain", "autocar", "car"]
INCLUDE_KEYWORDS = ["sncf", "db", "sbb", "cff", "renfe", "trenitalia", "national", "fernverkehr", "tgv", "intercity"]

# === 1. FONCTIONS MÉTIERS ===

def calculate_distance(lat1, lon1, lat2, lon2):
    try:
        R = 6371
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c
    except: return 0.0

def gtfs_time_to_hours(time_str):
    try:
        if not isinstance(time_str, str): return 0.0
        parts = list(map(int, time_str.split(':')))
        return parts[0] + parts[1]/60 + parts[2]/3600
    except: return 0.0

def determine_train_type(departure_h, duration_h):
    dep_mod = departure_h % 24
    if (dep_mod >= 22 or dep_mod <= 5) and duration_h > 4:
        return "Nuit"
    return "Jour"

def calculate_co2(distance_km, train_type):
    factor = 14.0 if train_type == "Nuit" else 4.0
    return round(distance_km * factor / 1000, 2)

# === 2. TRANSFORMATION (DIRECTE) ===

def process_single_gtfs(zip_path, provider_name, country_code):
    """
    Décompresse, Transforme et retourne le DataFrame (Lazy) prêt à être écrit.
    Ne fait AUCUNE écriture intermédiaire.
    """
    print(f"   ⚙️ Traitement de {os.path.basename(zip_path)}...")
    
    # Dossier temporaire pour les CSV extraits
    temp_csv_dir = f"./temp_unzip/{provider_name}_{country_code}"
    os.makedirs(temp_csv_dir, exist_ok=True)
    
    try:
        # 1. Extraction ZIP
        with zipfile.ZipFile(zip_path, 'r') as z:
            required = ['routes.txt', 'trips.txt', 'stops.txt', 'stop_times.txt']
            if not all(f in z.namelist() for f in required): return None
            z.extractall(temp_csv_dir)
        
        # 2. Lecture CSV (Lazy)
        routes = spark.read.option("header", "true").option("inferSchema", "false").csv(f"{temp_csv_dir}/routes.txt")
        trips = spark.read.option("header", "true").option("inferSchema", "false").csv(f"{temp_csv_dir}/trips.txt")
        stops = spark.read.option("header", "true").option("inferSchema", "false").csv(f"{temp_csv_dir}/stops.txt")
        stop_times = spark.read.option("header", "true").option("inferSchema", "false").csv(f"{temp_csv_dir}/stop_times.txt")

        # Conversions de types
        stops = stops.withColumn('stop_lat', col('stop_lat').cast(DoubleType())) \
                     .withColumn('stop_lon', col('stop_lon').cast(DoubleType()))
        routes = routes.withColumn('route_type', col('route_type').cast(DoubleType()))
        stop_times = stop_times.withColumn('stop_sequence', col('stop_sequence').cast(DoubleType()))

        # Filtres (Train uniquement)
        rail_routes = routes.filter((col('route_type') == 2) | ((col('route_type') >= 100) & (col('route_type') <= 117)))
        if rail_routes.count() == 0: return None

        rail_trips = trips.join(rail_routes.select('route_id'), 'route_id', 'inner')
        if rail_trips.count() == 0: return None
        
        rail_stop_times = stop_times.join(rail_trips.select('trip_id'), 'trip_id', 'inner')
        
        # Agrégation (Début/Fin)
        window_asc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').asc())
        window_desc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').desc())
        
        trip_bounds = rail_stop_times.withColumn('start_stop_id', first('stop_id').over(window_asc)) \
                                     .withColumn('end_stop_id', first('stop_id').over(window_desc)) \
                                     .withColumn('dep_time_first', first('departure_time').over(window_asc)) \
                                     .withColumn('arr_time_last', first('arrival_time').over(window_desc)) \
                                     .select('trip_id', 'start_stop_id', 'end_stop_id', 'dep_time_first', 'arr_time_last') \
                                     .distinct()

        # Enrichissement (Gares)
        merged = trip_bounds.join(stops.select(col('stop_id').alias('start_stop_id'), col('stop_name').alias('origin'), col('stop_lat').alias('lat1'), col('stop_lon').alias('lon1')), 'start_stop_id') \
                            .join(stops.select(col('stop_id').alias('end_stop_id'), col('stop_name').alias('destination'), col('stop_lat').alias('lat2'), col('stop_lon').alias('lon2')), 'end_stop_id')

        # Calculs UDF
        dist_udf = udf(calculate_distance, DoubleType())
        time_udf = udf(gtfs_time_to_hours, DoubleType())
        type_udf = udf(determine_train_type, StringType())
        co2_udf = udf(calculate_co2, DoubleType())

        merged = merged.withColumn('distance_km', dist_udf(col('lat1'), col('lon1'), col('lat2'), col('lon2'))) \
                       .withColumn('dep_dec', time_udf(col('dep_time_first'))) \
                       .withColumn('arr_dec', time_udf(col('arr_time_last'))) \
                       .withColumn('duration_h', spark_round(col('arr_dec') - col('dep_dec'), 2)) \
                       .withColumn('train_type', type_udf(col('dep_dec'), col('duration_h'))) \
                       .withColumn('co2_kg', co2_udf(col('distance_km'), col('train_type')))
        
        merged = merged.filter(col('distance_km') > 50) # Filtre distance min

        # Métadonnées
        final_df = merged.withColumn('provider', lit(provider_name)).withColumn('country', lit(country_code)) \
                         .select('provider', 'country', 'trip_id', 'origin', 'destination', 'dep_time_first', 'arr_time_last', 'duration_h', 'distance_km', 'train_type', 'co2_kg')
        
        return final_df

    except Exception as e:
        print(f"      ⚠️ Erreur logique: {e}")
        return None
    
    # Note: On ne supprime PAS temp_unzip ici, on le fait dans le main après l'écriture

# === 3. TÉLÉCHARGEMENT & ORCHESTRATION ===

def get_token():
    if not REFRESH_TOKEN: return None
    try:
        r = requests.post(f"{API_URL}/tokens", json={"refresh_token": REFRESH_TOKEN})
        return r.json().get("access_token")
    except: return None

def sanitize(name):
    return re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_").strip()[:40]

def is_interesting(feed):
    txt = (f"{feed.get('provider')} {feed.get('feed_name')}").lower()
    if any(k in txt for k in EXCLUDE_KEYWORDS): return False
    if any(k in txt for k in INCLUDE_KEYWORDS): return True
    return True

if __name__ == "__main__":
    print("=== 🚄 LANCEMENT PIPELINE EURO-RAIL (Version Direct-Write) ===")
    
    # Nettoyage initial complet
    for d in [DATA_DIR, GLOBAL_STAGING_DIR, "./temp_unzip", "./data/temp_parquet"]:
        shutil.rmtree(d, ignore_errors=True)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    token = get_token()
    processed_count = 0

    if token:
        for country in TARGET_COUNTRIES:
            print(f"\n🌍 PAYS : {country}")
            try:
                r = requests.get(f"{API_URL}/gtfs_feeds", headers={"Authorization": f"Bearer {token}"}, params={"country_code": country, "limit": 100})
                feeds = r.json()
            except: continue
            
            for feed in feeds:
                if not is_interesting(feed): continue
                
                dataset = feed.get("latest_dataset")
                if not dataset: continue
                
                provider = sanitize(feed.get("provider", "Unknown"))
                zip_path = f"{DATA_DIR}/{country}_{provider}_{feed.get('id')}.zip"
                temp_unzip_path = f"./temp_unzip/{provider}_{country}"

                try:
                    # 1. Télécharger
                    if not os.path.exists(zip_path):
                        print(f"   ⬇️ {provider}...", end=" ", flush=True)
                        with open(zip_path, 'wb') as f:
                            f.write(requests.get(dataset.get("hosted_url")).content)
                        print("OK")

                    # 2. Obtenir le DataFrame (Lazy)
                    df = process_single_gtfs(zip_path, provider, country)

                    # 3. ÉCRIRE DIRECTEMENT DANS LE STAGING GLOBAL (Action bloquante)
                    # C'est ici que Spark lit les CSV et écrit le Parquet final
                    if df is not None:
                        df.write.mode("append").parquet(GLOBAL_STAGING_DIR)
                        print(f"      ✅ Traitement terminé et sauvegardé.")
                        processed_count += 1
                    else:
                        print(f"      ⚪ Pas de données pertinentes.")

                except Exception as e:
                    print(f"      ❌ Erreur: {e}")

                finally:
                    # 4. Nettoyage immédiat
                    if os.path.exists(zip_path): os.remove(zip_path) # Supprime le ZIP
                    shutil.rmtree(temp_unzip_path, ignore_errors=True) # Supprime les CSV dézippés

    # === 4. CONSOLIDATION FINALE ===
    print("\n📦 GÉNÉRATION DU FICHIER FINAL...")
    
    if processed_count > 0:
        try:
            # On relit simplement le dossier global qui contient tout
            full_df = spark.read.parquet(GLOBAL_STAGING_DIR)
            print(f"📊 Total lignes : {full_df.count()}")
            
            full_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_FILE.replace('.csv', ''))
            print(f"🎉 SUCCESS ! Base générée : {OUTPUT_FILE}")
        except Exception as e:
            print(f"❌ Erreur finale: {e}")
    else:
        print("⚠️ Aucune donnée.")

    # Ménage final
    shutil.rmtree(GLOBAL_STAGING_DIR, ignore_errors=True)
    spark.stop()