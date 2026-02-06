"""
EXTRACTION COMBINÉE - Mobility Database + Back on Track + OurAirports
======================================================================
Ce fichier extrait UNIQUEMENT les données brutes des 3 sources :
- Mobility Database : API GTFS (fichiers ZIP)
- Back on Track : Google Sheets CSV
- OurAirports : CSV des aéroports mondiaux

Aucune transformation n'est effectuée ici.
Les données sont stockées dans des dossiers distincts pour traitement ultérieur.
Utilise PySpark pour le traitement des données.
"""

import requests
import os
import re
import zipfile
import shutil
import datetime as dt
from pathlib import Path
from urllib.request import urlopen, Request
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# === INITIALISATION SPARK ===
spark = SparkSession.builder \
    .appName("DataExtraction") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# === CONFIGURATION ===
load_dotenv()

# API Mobility Database
API_URL = "https://api.mobilitydatabase.org/v1"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
TARGET_COUNTRIES = ["FR", "CH", "DE"]

# Dossiers de sortie (données brutes)
MOBILITY_RAW_DIR = "./data/raw/mobility_gtfs"
BACKONTRACK_RAW_DIR = "./data/raw/backontrack_csv"
AIRPORTS_RAW_DIR = "./data/raw/airports"

# Filtres API (pour limiter les téléchargements)
EXCLUDE_KEYWORDS = ["bus", "shuttle", "tram", "metro", "urbain", "autocar", "car"]
INCLUDE_KEYWORDS = ["sncf", "db", "sbb", "cff", "renfe", "trenitalia", "national", "fernverkehr", "tgv", "intercity"]

# Back on Track Google Sheets ID
BACKONTRACK_SHEET_ID = "15zsK-lBuibUtZ1s2FxVHvAmSu-pEuE0NDT6CAMYL2TY"
BACKONTRACK_ONGLETS = ["agencies", "routes", "trips", "stops", "calendar", "trip_stop"]

# OurAirports URL
AIRPORTS_DATA_URL = "https://ourairports.com/data/airports.csv"


# ===========================
# MAPPING DES COLONNES
# ===========================
# REQUIRED_COLUMNS = ['origin', 'destination', 'vehicule_type', 'aero_lat', 'aero_long', 
#                     'station_lat', 'station_long', 'category', 'departure_time', 'arrival_time']
COLUMN_MAPPINGS = {
    "airports": {
        "latitude_deg": "aero_lat",
        "longitude_deg": "aero_long",
        "type": "category",
        "name": "airport_name",
        "iata_code": "iata_code",
        "iso_country": "country_code"
    },
    "stops": {
        "stop_lat": "station_lat",
        "stop_lon": "station_long",
        "stop_name": "station_name"
    },
    "stop_times": {
        "departure_time": "departure_time",
        "arrival_time": "arrival_time"
    },
    "routes": {
        "route_type": "vehicule_type"
    }
}

def apply_column_mapping(df, mapping_key):
    """Applique le renommage des colonnes selon le mapping"""
    if mapping_key not in COLUMN_MAPPINGS:
        return df
    
    mapping = COLUMN_MAPPINGS[mapping_key]
    for old_col, new_col in mapping.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    return df

# ===========================
# PARTIE 1 : MOBILITY DATABASE
# ===========================

def get_token():
    """Authentification API Mobility Database"""
    if not REFRESH_TOKEN: 
        print("⚠️ REFRESH_TOKEN manquant dans .env")
        return None
    try:
        r = requests.post(f"{API_URL}/tokens", json={"refresh_token": REFRESH_TOKEN})
        r.raise_for_status()
        return r.json()["access_token"]
    except Exception as e:
        print(f"❌ Erreur authentification : {e}")
        return None


def sanitize(name):
    """Nettoie les noms de fichiers"""
    return re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_").strip()[:40]


def is_interesting(feed):
    """Filtre les flux GTFS pertinents (trains uniquement)"""
    txt = (f"{feed.get('provider')} {feed.get('feed_name')}").lower()
    if any(k in txt for k in EXCLUDE_KEYWORDS): 
        return False
    if any(k in txt for k in INCLUDE_KEYWORDS): 
        return True
    return True


def extract_mobility_database():
    """
    Extrait les fichiers GTFS depuis Mobility Database
    Télécharge et dézippe dans MOBILITY_RAW_DIR
    """
    print("\n🌐 === EXTRACTION MOBILITY DATABASE ===")
    
    token = get_token()
    if not token: 
        return False
    
    os.makedirs(MOBILITY_RAW_DIR, exist_ok=True)
    extracted_count = 0

    for country in TARGET_COUNTRIES:
        print(f"\n📍 Pays : {country}")
        
        try:
            r = requests.get(
                f"{API_URL}/gtfs_feeds", 
                headers={"Authorization": f"Bearer {token}"}, 
                params={"country_code": country, "limit": 100}
            )
            feeds = r.json()
        except Exception as e:
            print(f"   ❌ Erreur requête API : {e}")
            continue
        
        for feed in feeds:
            if not is_interesting(feed): 
                continue
            
            dataset = feed.get("latest_dataset")
            if not dataset: 
                continue
            
            url = dataset.get("hosted_url")
            if not url: 
                continue
            
            provider = sanitize(feed.get("provider", "Unknown"))
            feed_id = feed.get("id")
            
            # Dossier de destination pour ce provider
            provider_dir = f"{MOBILITY_RAW_DIR}/{country}_{provider}_{feed_id}"
            zip_path = f"{provider_dir}.zip"
            
            # Téléchargement
            if not os.path.exists(provider_dir):
                print(f"   ⬇️  {provider}...", end=" ", flush=True)
                try:
                    with open(zip_path, 'wb') as f:
                        f.write(requests.get(url).content)
                    
                    # Extraction
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        z.extractall(provider_dir)
                    
                    # Suppression du ZIP (garder seulement les CSV)
                    os.remove(zip_path)
                    
                    print("✅")
                    extracted_count += 1
                    
                except Exception as e:
                    print(f"❌ {e}")
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
            else:
                print(f"   ⏭️  {provider} (déjà extrait)")
    
    print(f"\n✅ Mobility Database : {extracted_count} providers extraits")
    return True


# ===========================
# PARTIE 2 : BACK ON TRACK
# ===========================

def extract_backontrack():
    """
    Extrait les CSV depuis Google Sheets Back on Track
    Télécharge dans BACKONTRACK_RAW_DIR et convertit avec PySpark
    """
    print("\n📊 === EXTRACTION BACK ON TRACK ===")
    
    os.makedirs(BACKONTRACK_RAW_DIR, exist_ok=True)
    extracted_count = 0

    for onglet in BACKONTRACK_ONGLETS:
        url = f"https://docs.google.com/spreadsheets/d/{BACKONTRACK_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={onglet}"
        temp_file = f"{BACKONTRACK_RAW_DIR}/temp_{onglet}.csv"
        output_file = f"{BACKONTRACK_RAW_DIR}/back_on_track_{onglet}.csv"
        
        try:
            print(f"   ⬇️  {onglet}...", end=" ", flush=True)
            
            # Télécharger le fichier CSV temporaire
            response = requests.get(url)
            response.raise_for_status()
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            # Lire avec PySpark
            df = spark.read.option("header", "true") \
                          .option("inferSchema", "true") \
                          .csv(temp_file)
            
            # Appliquer le mapping des colonnes selon le type d'onglet
            if onglet == "stops":
                df = apply_column_mapping(df, "stops")
            elif onglet == "routes":
                df = apply_column_mapping(df, "routes")
            elif onglet == "trip_stop":
                df = apply_column_mapping(df, "stop_times")
            
            row_count = df.count()
            
            # Écrire le fichier final avec PySpark (mode coalesce pour un seul fichier)
            df.coalesce(1).write.mode("overwrite") \
                          .option("header", "true") \
                          .csv(f"{BACKONTRACK_RAW_DIR}/spark_{onglet}")
            
            # Renommer le fichier Spark output vers le nom final
            spark_output_dir = f"{BACKONTRACK_RAW_DIR}/spark_{onglet}"
            for f_name in os.listdir(spark_output_dir):
                if f_name.endswith('.csv'):
                    shutil.move(f"{spark_output_dir}/{f_name}", output_file)
                    break
            shutil.rmtree(spark_output_dir, ignore_errors=True)
            
            # Nettoyer le fichier temporaire
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            print(f"✅ ({row_count} lignes)")
            extracted_count += 1
            
        except Exception as e:
            print(f"❌ Erreur : {e}")
            # Nettoyer en cas d'erreur
            if os.path.exists(temp_file):
                os.remove(temp_file)
    
    print(f"\n✅ Back on Track : {extracted_count}/{len(BACKONTRACK_ONGLETS)} fichiers extraits")
    return True


# ===========================
# PARTIE 3 : OURAIRPORTS
# ===========================

def extract_airports(overwrite=False):
    """
    Extrait les données des aéroports depuis OurAirports
    Télécharge et traite avec PySpark dans AIRPORTS_RAW_DIR
    """
    print("\n✈️  === EXTRACTION OURAIRPORTS ===")
    
    output_dir = Path(AIRPORTS_RAW_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Utilise la date du jour dans le nom du fichier
    today = dt.date.today().strftime("%Y-%m-%d")
    temp_file = output_dir / f"airports_{today}_temp.csv"
    output_file = output_dir / f"airports_{today}.csv"
    
    # Si le fichier du jour existe déjà et qu'on ne veut pas le réécrire
    if output_file.exists() and not overwrite:
        print(f"   ⏭️  Fichier du jour déjà présent : {output_file}")
        return True
    
    try:
        print(f"   ⬇️  Téléchargement airports.csv...", end=" ", flush=True)
        
        # Télécharge le fichier depuis OurAirports
        req = Request(AIRPORTS_DATA_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=60) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP error {response.status}")
            content = response.read()
        
        # Écrit le fichier temporaire
        temp_file.write_bytes(content)
        print("✅")
        
        # Traiter avec PySpark
        print(f"   🔄 Traitement avec PySpark...", end=" ", flush=True)
        df = spark.read.option("header", "true") \
                      .option("inferSchema", "true") \
                      .csv(str(temp_file))
        
        # Appliquer le mapping des colonnes pour aéroports
        df = apply_column_mapping(df, "airports")
        
        row_count = df.count()
        
        # Écrire le fichier final avec PySpark
        spark_output_dir = str(output_dir / "spark_airports")
        df.coalesce(1).write.mode("overwrite") \
                      .option("header", "true") \
                      .csv(spark_output_dir)
        
        # Renommer le fichier Spark output vers le nom final
        for f_name in os.listdir(spark_output_dir):
            if f_name.endswith('.csv'):
                shutil.move(f"{spark_output_dir}/{f_name}", str(output_file))
                break
        shutil.rmtree(spark_output_dir, ignore_errors=True)
        
        # Nettoyer le fichier temporaire
        if temp_file.exists():
            temp_file.unlink()
        
        print(f"✅ ({row_count} aéroports)")
        
    except Exception as e:
        print(f"❌ Erreur : {e}")
        # Nettoyer en cas d'erreur
        if temp_file.exists():
            temp_file.unlink()
        return False
    
    print(f"\n✅ OurAirports : Fichier extrait → {output_file}")
    return True


# ===========================
# ORCHESTRATION
# ===========================

def run_extraction():
    """
    Lance l'extraction complète des 3 sources
    """
    print("=" * 60)
    print("🚀 EXTRACTION COMBINÉE - Début du processus (PySpark)")
    print("=" * 60)
    
    # Nettoyage préalable (optionnel)
    # shutil.rmtree(MOBILITY_RAW_DIR, ignore_errors=True)
    # shutil.rmtree(BACKONTRACK_RAW_DIR, ignore_errors=True)
    # shutil.rmtree(AIRPORTS_RAW_DIR, ignore_errors=True)
    
    success_mobility = extract_mobility_database()
    success_backontrack = extract_backontrack()
    success_airports = extract_airports()
    
    print("\n" + "=" * 60)
    if success_mobility and success_backontrack and success_airports:
        print("🎉 EXTRACTION TERMINÉE AVEC SUCCÈS")
        print(f"📂 Mobility Database : {MOBILITY_RAW_DIR}")
        print(f"📂 Back on Track     : {BACKONTRACK_RAW_DIR}")
        print(f"📂 OurAirports       : {AIRPORTS_RAW_DIR}")
    else:
        print("⚠️ EXTRACTION PARTIELLE (vérifier les erreurs ci-dessus)")
    print("=" * 60)
    
    # Arrêter la session Spark
    spark.stop()


if __name__ == "__main__":
    run_extraction()
