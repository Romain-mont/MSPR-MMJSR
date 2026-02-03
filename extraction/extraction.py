"""
EXTRACTION COMBINÉE - Mobility Database + Back on Track
========================================================
Ce fichier extrait UNIQUEMENT les données brutes des 2 sources :
- Mobility Database : API GTFS (fichiers ZIP)
- Back on Track : Google Sheets CSV

Aucune transformation n'est effectuée ici.
Les données sont stockées dans des dossiers distincts pour traitement ultérieur.
"""

import requests
import os
import re
import zipfile
import shutil
import pandas as pd
from dotenv import load_dotenv

# === CONFIGURATION ===
load_dotenv()

# API Mobility Database
API_URL = "https://api.mobilitydatabase.org/v1"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
TARGET_COUNTRIES = ["FR", "CH", "DE"]

# Dossiers de sortie (données brutes)
MOBILITY_RAW_DIR = "./data/raw/mobility_gtfs"
BACKONTRACK_RAW_DIR = "./data/raw/backontrack_csv"

# Filtres API (pour limiter les téléchargements)
EXCLUDE_KEYWORDS = ["bus", "shuttle", "tram", "metro", "urbain", "autocar", "car"]
INCLUDE_KEYWORDS = ["sncf", "db", "sbb", "cff", "renfe", "trenitalia", "national", "fernverkehr", "tgv", "intercity"]

# Back on Track Google Sheets ID
BACKONTRACK_SHEET_ID = "15zsK-lBuibUtZ1s2FxVHvAmSu-pEuE0NDT6CAMYL2TY"
BACKONTRACK_ONGLETS = ["agencies", "routes", "trips", "stops", "calendar", "trip_stop"]


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
    Télécharge dans BACKONTRACK_RAW_DIR
    """
    print("\n📊 === EXTRACTION BACK ON TRACK ===")
    
    os.makedirs(BACKONTRACK_RAW_DIR, exist_ok=True)
    extracted_count = 0

    for onglet in BACKONTRACK_ONGLETS:
        url = f"https://docs.google.com/spreadsheets/d/{BACKONTRACK_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={onglet}"
        output_file = f"{BACKONTRACK_RAW_DIR}/back_on_track_{onglet}.csv"
        
        try:
            print(f"   ⬇️  {onglet}...", end=" ", flush=True)
            df = pd.read_csv(url)
            df.to_csv(output_file, index=False)
            print(f"✅ ({len(df)} lignes)")
            extracted_count += 1
            
        except Exception as e:
            print(f"❌ Erreur : {e}")
    
    print(f"\n✅ Back on Track : {extracted_count}/{len(BACKONTRACK_ONGLETS)} fichiers extraits")
    return True


# ===========================
# ORCHESTRATION
# ===========================

def run_extraction():
    """
    Lance l'extraction complète des 2 sources
    """
    print("=" * 60)
    print("🚀 EXTRACTION COMBINÉE - Début du processus")
    print("=" * 60)
    
    # Nettoyage préalable (optionnel)
    # shutil.rmtree(MOBILITY_RAW_DIR, ignore_errors=True)
    # shutil.rmtree(BACKONTRACK_RAW_DIR, ignore_errors=True)
    
    success_mobility = extract_mobility_database()
    success_backontrack = extract_backontrack()
    
    print("\n" + "=" * 60)
    if success_mobility and success_backontrack:
        print("🎉 EXTRACTION TERMINÉE AVEC SUCCÈS")
        print(f"📂 Mobility Database : {MOBILITY_RAW_DIR}")
        print(f"📂 Back on Track     : {BACKONTRACK_RAW_DIR}")
    else:
        print("⚠️ EXTRACTION PARTIELLE (vérifier les erreurs ci-dessus)")
    print("=" * 60)


if __name__ == "__main__":
    run_extraction()
