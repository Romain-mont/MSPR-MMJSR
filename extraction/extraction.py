"""
EXTRACTION COMBINÉE - Mobility Database + Back on Track + OurAirports
======================================================================
Ce fichier extrait UNIQUEMENT les données brutes des 3 sources :
- Mobility Database : API GTFS (fichiers ZIP)
- Back on Track : Google Sheets CSV
- OurAirports : CSV des aéroports mondiaux
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


# Détection d'exécution dans Docker
def running_in_docker():
    path = '/.dockerenv'
    return os.path.exists(path) or os.environ.get('RUNNING_IN_DOCKER') == '1'

# INITIALISATION SPARK compatible Docker
def get_spark():
    builder = SparkSession.builder.appName("DataExtraction")
    builder = builder.config("spark.driver.memory", "2g")
    builder = builder.master("local[*]")
    if running_in_docker():
        builder = builder.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    return builder.getOrCreate()

spark = get_spark()

# CONFIGURATION
load_dotenv()

# API Mobility Database
API_URL = "https://api.mobilitydatabase.org/v1"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

import ast
target_countries_env = os.getenv("TARGET_COUNTRIES", "['FR']")
try:
    TARGET_COUNTRIES = ast.literal_eval(target_countries_env)
except:
    TARGET_COUNTRIES = ['FR']


# Dossiers de sortie (données brutes) adaptables via env (Docker)
MOBILITY_RAW_DIR = os.environ.get("RAW_MOBILITY_DIR", "./data/raw/mobility_gtfs")
BACKONTRACK_RAW_DIR = os.environ.get("RAW_BACKONTRACK_DIR", "./data/raw/backontrack_csv")
AIRPORTS_RAW_DIR = os.environ.get("RAW_AIRPORTS_DIR", "./data/raw/airports")

# Filtres API (pour limiter les téléchargements)
EXCLUDE_KEYWORDS = ["bus", "shuttle", "tram", "metro", "urbain", "autocar", "car"]
INCLUDE_KEYWORDS = ["sncf", "db", "sbb", "cff", "renfe", "trenitalia", "national", "fernverkehr", "tgv", "intercity"]

# Back on Track Google Sheets ID
BACKONTRACK_SHEET_ID = "15zsK-lBuibUtZ1s2FxVHvAmSu-pEuE0NDT6CAMYL2TY"
BACKONTRACK_ONGLETS = ["agencies", "routes", "trips", "stops", "calendar", "trip_stop"]

# OurAirports URL
AIRPORTS_DATA_URL = "https://ourairports.com/data/airports.csv"


# MAPPING DES COLONNES

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

# PARTIE 1 : MOBILITY DATABASE

def get_token():
    """Authentification API Mobility Database"""
    if not REFRESH_TOKEN: 
        print("REFRESH_TOKEN manquant dans .env")
        return None
    try:
        r = requests.post(f"{API_URL}/tokens", json={"refresh_token": REFRESH_TOKEN})
        r.raise_for_status()
        return r.json()["access_token"]
    except Exception as e:
        print(f"Erreur authentification : {e}")
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
    print("EXTRACTION MOBILITY DATABASE")
    
    token = get_token()
    if not token: 
        return False
    
    os.makedirs(MOBILITY_RAW_DIR, exist_ok=True)
    extracted_count = 0

    for country in TARGET_COUNTRIES:
        print(f"Pays : {country}")
        
        try:
            r = requests.get(
                f"{API_URL}/gtfs_feeds", 
                headers={"Authorization": f"Bearer {token}"}, 
                params={"country_code": country, "limit": 100}
            )
            feeds = r.json()
        except Exception as e:
            print(f"Erreur requête API : {e}")
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
                print(f"{provider}...", end=" ", flush=True)
                try:
                    with open(zip_path, 'wb') as f:
                        f.write(requests.get(url).content)
                    
                    # Extraction
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        z.extractall(provider_dir)
                    
                    # Suppression du ZIP (garder seulement les CSV)
                    os.remove(zip_path)

                    print("téléchargement et extraction finis")
                    extracted_count += 1
                    
                except Exception as e:
                    print(f"Erreur : {e}")
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
            else:
                print(f"{provider} (déjà extrait)")
    
    print(f"Mobility Database : {extracted_count} providers extraits")
    return True


# PARTIE 2 : BACK ON TRACK

def extract_backontrack():
    """
    Extrait les CSV depuis Google Sheets Back on Track
    Télécharge dans BACKONTRACK_RAW_DIR et convertit avec PySpark
    """
    print("EXTRACTION BACK ON TRACK")
    
    os.makedirs(BACKONTRACK_RAW_DIR, exist_ok=True)
    extracted_count = 0

    for onglet in BACKONTRACK_ONGLETS:
        url = f"https://docs.google.com/spreadsheets/d/{BACKONTRACK_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={onglet}"
        temp_file = f"{BACKONTRACK_RAW_DIR}/temp_{onglet}.csv"
        output_file = f"{BACKONTRACK_RAW_DIR}/back_on_track_{onglet}.csv"
        
        try:
            print(f"{onglet}...", end=" ", flush=True)
            
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
            
            # Écrire le fichier final via Pandas (évite HADOOP_HOME sur Windows)
            df.toPandas().to_csv(output_file, index=False)
            
            # Nettoyer le fichier temporaire
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            print(f"({row_count} lignes)")
            extracted_count += 1
            
        except Exception as e:
            print(f"Erreur : {e}")
            # Nettoyer en cas d'erreur
            if os.path.exists(temp_file):
                os.remove(temp_file)
    
    print(f"Back on Track : {extracted_count}/{len(BACKONTRACK_ONGLETS)} fichiers extraits")
    return True


# PARTIE 3 : OURAIRPORTS

def extract_airports(overwrite=False):
    """
    Extrait les données des aéroports depuis OurAirports
    Télécharge et traite avec PySpark dans AIRPORTS_RAW_DIR
    """
    print("EXTRACTION OURAIRPORTS")
    
    output_dir = Path(AIRPORTS_RAW_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Utilise la date du jour dans le nom du fichier
    today = dt.date.today().strftime("%Y-%m-%d")
    temp_file = output_dir / f"airports_{today}_temp.csv"
    output_file = output_dir / f"airports_{today}.csv"
    
    # Si le fichier du jour existe déjà et qu'on ne veut pas le réécrire
    if output_file.exists() and not overwrite:
        print(f"Fichier du jour déjà présent : {output_file}")
        return True
    
    try:
        print(f"Téléchargement airports.csv...", end=" ", flush=True)
        
        # Télécharge le fichier depuis OurAirports
        req = Request(AIRPORTS_DATA_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=60) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP error {response.status}")
            content = response.read()
        
        # Écrit le fichier temporaire
        temp_file.write_bytes(content)
        
        # Traiter avec PySpark
        print(f"Traitement avec PySpark...", end=" ", flush=True)
        df = spark.read.option("header", "true") \
                      .option("inferSchema", "true") \
                      .csv(str(temp_file))
        
        # Appliquer le mapping des colonnes pour aéroports
        df = apply_column_mapping(df, "airports")
        
        row_count = df.count()
        
        # Écrire le fichier final via Pandas (évite HADOOP_HOME sur Windows)
        df.toPandas().to_csv(str(output_file), index=False)
        
        # Nettoyer le fichier temporaire
        if temp_file.exists():
            temp_file.unlink()
        
        print(f"({row_count} aéroports)")
        
    except Exception as e:
        print(f"Erreur : {e}")
        # Nettoyer en cas d'erreur
        if temp_file.exists():
            temp_file.unlink()
        return False
    
    print(f"OurAirports : Fichier extrait → {output_file}")
    return True


# PARTIE 4 : SNCF FRÉQUENTATION DES GARES

SNCF_FREQUENTATION_URL = (
    "https://data.sncf.com/api/explore/v2.1/catalog/datasets/"
    "frequentation-gares/exports/csv"
    "?lang=fr&timezone=Europe%2FParis&use_labels=true&delimiter=%3B"
)
SNCF_FREQ_DIR = os.environ.get("RAW_SNCF_FREQ_DIR", "./data/raw/sncf_frequentation")


def extract_sncf_frequentation():
    """
    Extrait la fréquentation annuelle des gares SNCF (2015-2024)
    Source : data.sncf.com - dataset frequentation-gares
    ~3000 gares, voyageurs annuels, séparateur point-virgule
    """
    print("EXTRACTION SNCF FRÉQUENTATION DES GARES")
    os.makedirs(SNCF_FREQ_DIR, exist_ok=True)

    today = dt.date.today().strftime("%Y-%m-%d")
    output_file = f"{SNCF_FREQ_DIR}/sncf_frequentation_{today}.csv"

    if os.path.exists(output_file):
        print(f"Fichier du jour déjà présent : {output_file}")
        return True

    try:
        print("Téléchargement fréquentation SNCF...", end=" ", flush=True)
        r = requests.get(SNCF_FREQUENTATION_URL, timeout=120)
        r.raise_for_status()

        content = r.content.decode("utf-8", errors="replace")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(content)

        n_lines = max(0, content.count("\n") - 1)
        print(f"({n_lines} gares)")
        return True

    except Exception as e:
        print(f"Erreur : {e}")
        if os.path.exists(output_file):
            os.remove(output_file)
        return False


# PARTIE 5 : POPULATION DES VILLES EUROPÉENNES - EUROSTAT

# Codes Urban Audit Eurostat pour les principales villes couvertes par le projet
EUROSTAT_CITY_CODES = [
    # France
    "FR001C", "FR002C", "FR003C", "FR004C", "FR005C",
    "FR006C", "FR007C", "FR008C", "FR009C", "FR010C",
    # Allemagne
    "DE001C", "DE002C", "DE003C", "DE004C", "DE005C", "DE006C",
    # Espagne
    "ES001C", "ES002C", "ES003C",
    # Italie
    "IT001C", "IT002C", "IT003C",
    # Belgique
    "BE001C",
    # Pays-Bas
    "NL001C", "NL002C",
    # Suisse
    "CH001C", "CH002C",
    # Autriche
    "AT001C",
    # Royaume-Uni
    "UK001C",
]

EUROSTAT_URB_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/"
    "statistics/1.0/data/urb_cpop1"
)
EUROSTAT_POP_DIR = os.environ.get("RAW_EUROSTAT_POP_DIR", "./data/raw/population")


def _parse_eurostat_sdmx(data):
    """
    Parse une réponse SDMX-JSON Eurostat en liste de dicts.
    Le format flat-index de l'API Eurostat est calculé à partir
    de l'ordre des dimensions (data["id"]) et de leurs tailles (data["size"]).
    """
    id_order = data["id"]
    sizes    = data["size"]
    values   = data["value"]

    cats = {}
    for dim in id_order:
        cat = data["dimension"][dim]["category"]
        cats[dim] = {int(pos): code for code, pos in cat["index"].items()}

    rows = []
    total = 1
    for s in sizes:
        total *= s

    for flat in range(total):
        val = values.get(str(flat))
        if val is None:
            continue
        row = {"value": val}
        remaining = flat
        for dim, size in zip(reversed(id_order), reversed(sizes)):
            pos = remaining % size
            remaining //= size
            row[dim] = cats[dim].get(pos, str(pos))
        rows.append(row)

    return rows


def extract_population_eurostat():
    """
    Extrait la population totale des grandes villes européennes.
    Source : Eurostat urb_cpop1 (Urban Audit) - sans authentification.
    Requêtes par batch de 5 codes pour éviter l'erreur 413.
    """
    print("EXTRACTION EUROSTAT - POPULATION VILLES EUROPÉENNES")
    os.makedirs(EUROSTAT_POP_DIR, exist_ok=True)

    today = dt.date.today().strftime("%Y-%m-%d")
    output_file = f"{EUROSTAT_POP_DIR}/eurostat_city_population_{today}.csv"

    if os.path.exists(output_file):
        print(f"Fichier du jour déjà présent : {output_file}")
        return True

    try:
        all_rows = []
        print(f"Requête Eurostat urb_cpop1 ({len(EUROSTAT_CITY_CODES)} villes, 1 par requête)...", flush=True)

        for code in EUROSTAT_CITY_CODES:
            try:
                params = [
                    ("format", "JSON"),
                    ("lang",   "EN"),
                    ("sex",    "T"),
                    ("age",    "TOTAL"),
                    ("time",   "2021"),
                    ("geo",    code),
                ]
                r = requests.get(EUROSTAT_URB_URL, params=params, timeout=30)
                r.raise_for_status()
                rows = _parse_eurostat_sdmx(r.json())
                all_rows.extend(rows)
                print(f"  {code} : {len(rows)} entrées")
            except Exception as e:
                print(f"  {code} : ignoré ({e})")

        if not all_rows:
            print("Aucune donnée retournée par Eurostat")
            return False

        import csv
        fieldnames = list(all_rows[0].keys())
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)

        n_cities = len({row.get("geo") for row in all_rows})
        print(f"({len(all_rows)} entrées - {n_cities} villes)")
        return True

    except Exception as e:
        print(f"Erreur : {e}")
        if os.path.exists(output_file):
            os.remove(output_file)
        return False


# PARTIE 6 : POPULATION DES COMMUNES FRANÇAISES - API GEO (INSEE)

# API Geo (api.gouv.fr) - données INSEE, sans authentification requise
GEO_API_COMMUNES_URL = "https://geo.api.gouv.fr/communes"
GEO_API_COMMUNES_PARAMS = {
    "fields": "nom,code,codeDepartement,codeRegion,population",
    "format": "json",
    "boost":  "population",
}
INSEE_POP_DIR = os.environ.get("RAW_INSEE_POP_DIR", "./data/raw/population")


def extract_population_communes_france():
    """
    Extrait la population de toutes les communes françaises.
    Source : API Geo (api.gouv.fr) - données INSEE, sans clé API.
    ~34 000 communes avec population légale (dernier recensement).
    """
    print("EXTRACTION POPULATION COMMUNES FRANCE - API GEO/INSEE")
    os.makedirs(INSEE_POP_DIR, exist_ok=True)

    today = dt.date.today().strftime("%Y-%m-%d")
    output_file = f"{INSEE_POP_DIR}/communes_population_france_{today}.csv"

    if os.path.exists(output_file):
        print(f"Fichier du jour déjà présent : {output_file}")
        return True

    try:
        print("Téléchargement communes françaises...", end=" ", flush=True)
        r = requests.get(GEO_API_COMMUNES_URL, params=GEO_API_COMMUNES_PARAMS, timeout=120)
        r.raise_for_status()

        data = r.json()
        if not data:
            print("Aucune donnée retournée")
            return False

        import csv as csv_mod
        fieldnames = list(data[0].keys())
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"({len(data)} communes)")
        return True

    except Exception as e:
        print(f"Erreur : {e}")
        if os.path.exists(output_file):
            os.remove(output_file)
        return False


# PARTIE 7 : POPULATION VILLES EUROPÉENNES - GEONAMES

GEONAMES_URL = "https://download.geonames.org/export/dump/cities15000.zip"
GEONAMES_DIR = os.environ.get("RAW_GEONAMES_DIR", "./data/raw/population")


def extract_population_geonames():
    """
    Extrait la population des villes européennes depuis GeoNames.
    Source : cities15000.zip — toutes villes >15k hab, monde entier, gratuit.
    Colonnes utiles : name, country_code, population, latitude, longitude.
    """
    print("EXTRACTION GEONAMES - POPULATION VILLES EUROPÉENNES")
    os.makedirs(GEONAMES_DIR, exist_ok=True)

    today = dt.date.today().strftime("%Y-%m-%d")
    output_file = f"{GEONAMES_DIR}/geonames_cities_{today}.csv"

    if os.path.exists(output_file):
        print(f"Fichier du jour déjà présent : {output_file}")
        return True

    try:
        import zipfile, io, csv as csv_mod

        print("Téléchargement GeoNames cities15000...", end=" ", flush=True)
        r = requests.get(GEONAMES_URL, timeout=120)
        r.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with z.open("cities15000.txt") as f:
                content = f.read().decode("utf-8")

        # Colonnes GeoNames (tab-séparé, pas de header)
        col_indices = {
            "geonameid": 0, "name": 1, "asciiname": 2,
            "country_code": 8, "population": 14,
            "latitude": 4, "longitude": 5,
        }
        european_countries = {
            "FR","DE","CH","AT","BE","NL","ES","IT","PT","GB","IE",
            "SE","NO","DK","FI","PL","CZ","SK","HU","RO","BG","HR",
            "SI","RS","BA","ME","GR","LU","LV","LT","EE","TR",
        }

        rows = []
        for line in content.splitlines():
            parts = line.split("\t")
            if len(parts) < 15:
                continue
            cc = parts[col_indices["country_code"]]
            if cc not in european_countries:
                continue
            rows.append({
                "name":         parts[col_indices["name"]],
                "asciiname":    parts[col_indices["asciiname"]],
                "country_code": cc,
                "population":   parts[col_indices["population"]],
                "latitude":     parts[col_indices["latitude"]],
                "longitude":    parts[col_indices["longitude"]],
            })

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

        print(f"({len(rows)} villes européennes)")
        return True

    except Exception as e:
        print(f"Erreur : {e}")
        if os.path.exists(output_file):
            os.remove(output_file)
        return False


# ORCHESTRATION

def run_extraction():
    """
    Lance l'extraction complète des 6 sources.
    """
    print("EXTRACTION COMBINÉE - Début du processus (PySpark)")

    success_mobility    = extract_mobility_database()
    success_backontrack = extract_backontrack()
    success_airports    = extract_airports()
    success_sncf_freq   = extract_sncf_frequentation()
    success_eurostat    = extract_population_eurostat()
    success_communes    = extract_population_communes_france()
    success_geonames    = extract_population_geonames()

    results = [
        ("Mobility Database",        success_mobility,    MOBILITY_RAW_DIR),
        ("Back on Track",            success_backontrack, BACKONTRACK_RAW_DIR),
        ("OurAirports",              success_airports,    AIRPORTS_RAW_DIR),
        ("SNCF Fréquentation gares", success_sncf_freq,   SNCF_FREQ_DIR),
        ("Eurostat Population",      success_eurostat,    EUROSTAT_POP_DIR),
        ("Communes France (INSEE)",  success_communes,    INSEE_POP_DIR),
        ("GeoNames Villes EU",       success_geonames,    GEONAMES_DIR),
    ]

    all_ok = all(ok for _, ok, _ in results)
    status = "TERMINÉE AVEC SUCCÈS" if all_ok else "PARTIELLE (vérifier les erreurs ci-dessus)"
    print(f"\nEXTRACTION {status}")
    for name, ok, path in results:
        mark = "OK" if ok else "ECHEC"
        print(f"  [{mark}] {name:<30} -> {path}")

    spark.stop()


if __name__ == "__main__":
    run_extraction()
