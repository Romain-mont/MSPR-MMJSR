import requests
import os
import re
import pandas as pd
import zipfile
import math
from dotenv import load_dotenv

# === CONFIGURATION ===
load_dotenv()
API_URL = "https://api.mobilitydatabase.org/v1"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

if not REFRESH_TOKEN:
    raise ValueError("❌ REFRESH_TOKEN non trouvé dans le fichier .env")

TARGET_COUNTRIES = ["FR", "CH", "DE"]
ONLY_RAIL_AND_MAJOR = True 
EXCLUDE_KEYWORDS = ["bus", "shuttle", "tram", "trolley", "urbain", "municipal"]
INCLUDE_KEYWORDS = ["sncf", "db", "sbb", "cff", "rail", "train", "ferro", "national", "regional", "nmbs", "renfe"]

# Dossier de stockage temporaire
DATA_DIR = "./data/raw_feeds"
os.makedirs(DATA_DIR, exist_ok=True)

# === PARTIE 1 : FONCTIONS DE TRANSFORMATION (MOTEUR ETL) ===

def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def gtfs_time_to_hours(time_str):
    try:
        if not isinstance(time_str, str): return 0.0
        h, m, s = map(int, time_str.split(':'))
        return h + m/60 + s/3600
    except:
        return 0.0

def analyze_gtfs_content(zip_path):
    """
    Ouvre le ZIP, cherche des trains longue distance et retourne un résumé.
    Ne plante pas si le fichier est vide ou bizarre.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Vérification basique de la présence des fichiers
            files = z.namelist()
            required = ['routes.txt', 'trips.txt', 'stops.txt', 'stop_times.txt']
            if not all(f in files for f in required):
                return "⚠️ Fichiers manquants (pas un GTFS valide)"

            # Lecture
            routes = pd.read_csv(z.open('routes.txt'), dtype=str)
            
            # 1. Filtre rapide sur les routes
            routes['route_type'] = pd.to_numeric(routes['route_type'], errors='coerce')
            rail_routes = routes[
                (routes['route_type'] == 2) | 
                ((routes['route_type'] >= 100) & (routes['route_type'] <= 117))
            ]
            
            if len(rail_routes) == 0:
                return "⚪ Aucun train détecté (Routes vides)"

            # Si on a des trains, on charge le reste (plus lourd)
            trips = pd.read_csv(z.open('trips.txt'), dtype=str)
            rail_trips = trips[trips['route_id'].isin(rail_routes['route_id'])]
            
            if len(rail_trips) == 0:
                return "⚪ Lignes de train trouvées, mais aucun voyage planifié."

            # Chargement partiel des arrêts et horaires
            stops = pd.read_csv(z.open('stops.txt'), dtype={'stop_id': str})
            stop_times = pd.read_csv(z.open('stop_times.txt'), dtype={'trip_id': str, 'stop_id': str})
            
            # Filtre stop_times
            rail_stop_times = stop_times[stop_times['trip_id'].isin(rail_trips['trip_id'])]
            
            if len(rail_stop_times) == 0:
                return "⚪ Pas d'horaires trouvés."

            # Calcul des distances (Version simplifiée pour l'analyse rapide)
            # On prend juste le premier voyage pour voir à quoi ça ressemble
            first_trip = rail_trips.iloc[0]['trip_id']
            sample_times = rail_stop_times[rail_stop_times['trip_id'] == first_trip].sort_values('stop_sequence')
            
            if len(sample_times) < 2:
                return "⚠️ Données corrompues sur les trajets."
            
            # Récupérer coordonnées départ/arrivée du sample
            start_stop = sample_times.iloc[0]['stop_id']
            end_stop = sample_times.iloc[-1]['stop_id']
            
            s_dep = stops[stops['stop_id'] == start_stop].iloc[0]
            s_arr = stops[stops['stop_id'] == end_stop].iloc[0]
            
            dist = calculate_distance(float(s_dep['stop_lat']), float(s_dep['stop_lon']),
                                      float(s_arr['stop_lat']), float(s_arr['stop_lon']))
            
            return f"✅ CONTIENT DU TRAIN ! ({len(rail_routes)} lignes, {len(rail_trips)} voyages). Ex trajet: {dist:.1f} km"

    except Exception as e:
        return f"❌ Erreur lecture GTFS: {str(e)[:50]}"

# === PARTIE 2 : FONCTIONS D'EXTRACTION (MOTEUR API) ===

def get_token():
    r = requests.post(f"{API_URL}/tokens", json={"refresh_token": REFRESH_TOKEN})
    r.raise_for_status()
    return r.json()["access_token"]

def sanitize_filename(name):
    clean = re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_").strip()
    return clean[:50]

def is_interesting_feed(feed):
    if not ONLY_RAIL_AND_MAJOR: return True
    provider = (feed.get("provider") or "").lower()
    name = (feed.get("feed_name") or "").lower()
    full_text = f"{provider} {name}"
    if any(kw in full_text for kw in INCLUDE_KEYWORDS): return True
    if any(kw in full_text for kw in EXCLUDE_KEYWORDS): return False
    return True

def download_and_analyze(country_code, token):
    headers = {"Authorization": f"Bearer {token}"}
    print(f"\n🌍 SCAN DU PAYS : {country_code}")
    
    try:
        r = requests.get(f"{API_URL}/gtfs_feeds", headers=headers, params={"country_code": country_code, "limit": 100})
        feeds = r.json()
    except Exception as e:
        print(f"❌ Erreur API : {e}")
        return

    print(f"   -> {len(feeds)} sources trouvées.")
    
    for feed in feeds:
        if not is_interesting_feed(feed): continue

        dataset = feed.get("latest_dataset")
        if not dataset: continue
        url = dataset.get("hosted_url")
        if not url: continue

        provider = sanitize_filename(feed.get("provider", "Unknown"))
        feed_id = feed.get("id")
        filename = f"{country_code}_{provider}_{feed_id}.zip"
        filepath = os.path.join(DATA_DIR, filename)

        print(f"   ⬇️ Téléchargement : {provider} ...", end=" ", flush=True)
        
        try:
            # 1. Téléchargement
            r_file = requests.get(url)
            with open(filepath, 'wb') as f:
                f.write(r_file.content)
            print("OK.")
            
            # 2. Analyse immédiate (Pipeline)
            print(f"      ⚙️ Analyse : ", end="")
            result = analyze_gtfs_content(filepath)
            print(result)
            
            # Optionnel : Si le fichier est inutile (pas de trains), on peut le supprimer pour gagner de la place
            if "Aucun train" in result or "Fichiers manquants" in result:
                # os.remove(filepath) # Décommenter pour supprimer automatiquement
                pass

        except Exception as e:
            print(f"Erreur technique : {e}")

# === EXECUTION ===
if __name__ == "__main__":
    print("=== DÉMARRAGE DU PIPELINE ETL INTELLIGENT ===")
    token = get_token()
    
    for country in TARGET_COUNTRIES:
        download_and_analyze(country, token)