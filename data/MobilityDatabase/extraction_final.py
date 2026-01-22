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

# Pays cibles
TARGET_COUNTRIES = ["FR", "CH"]
DATA_DIR = "./data/raw"
OUTPUT_FILE = "data/Europe_Rail_Database.csv"

# Filtres API
EXCLUDE_KEYWORDS = ["bus", "shuttle", "tram", "metro", "urbain"]
INCLUDE_KEYWORDS = ["sncf", "db", "sbb", "cff", "renfe", "trenitalia", "national", "fernverkehr", "tgv", "intercity"]

# === 1. FONCTIONS MÉTIERS (Ta logique de calcul) ===

def calculate_distance(lat1, lon1, lat2, lon2):
    """Formule de Haversine"""
    try:
        R = 6371
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c
    except:
        return 0.0

def gtfs_time_to_hours(time_str):
    """Convertit 'HH:MM:SS' en float (gère 25:30)"""
    try:
        if not isinstance(time_str, str): return 0.0
        parts = list(map(int, time_str.split(':')))
        return parts[0] + parts[1]/60 + parts[2]/3600
    except:
        return 0.0

def determine_train_type(departure_h, duration_h):
    """Règle Nuit vs Jour"""
    dep_mod = departure_h % 24
    if (dep_mod >= 22 or dep_mod <= 5) and duration_h > 4:
        return "Nuit"
    return "Jour"

def calculate_co2(distance_km, train_type):
    """4g/km (Jour) vs 14g/km (Nuit)"""
    factor = 14.0 if train_type == "Nuit" else 4.0
    return round(distance_km * factor / 1000, 2)

# === 2. FONCTION DE TRANSFORMATION (Ton script adapté) ===

def process_single_gtfs(zip_path, provider_name, country_code):
    """
    Lit un ZIP, extrait les trains > 100km, calcule le CO2.
    Retourne un DataFrame propre.
    """
    print(f"   ⚙️ Traitement de {os.path.basename(zip_path)}...")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Vérif présence fichiers
            required = ['routes.txt', 'trips.txt', 'stops.txt', 'stop_times.txt']
            if not all(f in z.namelist() for f in required):
                return None

            # Chargement
            routes = pd.read_csv(z.open('routes.txt'), dtype=str)
            trips = pd.read_csv(z.open('trips.txt'), dtype=str)
            stops = pd.read_csv(z.open('stops.txt'), dtype={'stop_id': str})
            stop_times = pd.read_csv(z.open('stop_times.txt'), dtype={'trip_id': str, 'stop_id': str})

            # Conversion types
            stops['stop_lat'] = pd.to_numeric(stops['stop_lat'], errors='coerce')
            stops['stop_lon'] = pd.to_numeric(stops['stop_lon'], errors='coerce')
            routes['route_type'] = pd.to_numeric(routes['route_type'], errors='coerce')
            stop_times['stop_sequence'] = pd.to_numeric(stop_times['stop_sequence'], errors='coerce')

            # --- FILTRAGE TRAIN ---
            rail_routes = routes[
                (routes['route_type'] == 2) | 
                ((routes['route_type'] >= 100) & (routes['route_type'] <= 117))
            ]
            
            if rail_routes.empty: return None

            rail_trips = trips[trips['route_id'].isin(rail_routes['route_id'])]
            if rail_trips.empty: return None

            # Optimisation: ne garder que les stop_times des trains
            rail_stop_times = stop_times[stop_times['trip_id'].isin(rail_trips['trip_id'])]
            
            # --- AGGRÉGATION (Départ -> Arrivée) ---
            rail_stop_times = rail_stop_times.sort_values(['trip_id', 'stop_sequence'])
            trip_bounds = rail_stop_times.groupby('trip_id').agg({
                'stop_id': ['first', 'last'],
                'departure_time': 'first',
                'arrival_time': 'last'
            }).reset_index()
            trip_bounds.columns = ['trip_id', 'start_stop_id', 'end_stop_id', 'departure_time', 'arrival_time']

            # --- ENRICHISSEMENT (Coordonnées) ---
            # Jointure Départ
            merged = trip_bounds.merge(stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], 
                                     left_on='start_stop_id', right_on='stop_id')
            merged = merged.rename(columns={'stop_name': 'origin', 'stop_lat': 'lat1', 'stop_lon': 'lon1'})
            
            # Jointure Arrivée
            merged = merged.merge(stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], 
                                left_on='end_stop_id', right_on='stop_id', suffixes=('', '_arr'))
            merged = merged.rename(columns={'stop_name': 'destination', 'stop_lat': 'lat2', 'stop_lon': 'lon2'})

            # --- CALCULS MÉTIERS ---
            merged['distance_km'] = merged.apply(lambda x: calculate_distance(x['lat1'], x['lon1'], x['lat2'], x['lon2']), axis=1)
            
            # Filtre Longue Distance (> 100km)
            merged = merged[merged['distance_km'] > 100].copy()
            
            if merged.empty: return None

            merged['dep_dec'] = merged['departure_time'].apply(gtfs_time_to_hours)
            merged['arr_dec'] = merged['arrival_time'].apply(gtfs_time_to_hours)
            merged['duration_h'] = (merged['arr_dec'] - merged['dep_dec']).round(2)
            
            merged['train_type'] = merged.apply(lambda x: determine_train_type(x['dep_dec'], x['duration_h']), axis=1)
            merged['co2_kg'] = merged.apply(lambda x: calculate_co2(x['distance_km'], x['train_type']), axis=1)

            # Ajout métadonnées source
            merged['provider'] = provider_name
            merged['country'] = country_code

            # Sélection finale
            return merged[[
                'provider', 'country', 'trip_id', 'origin', 'destination', 
                'departure_time', 'arrival_time', 'duration_h', 
                'distance_km', 'train_type', 'co2_kg'
            ]]

    except Exception as e:
        print(f"      ❌ Erreur GTFS: {e}")
        return None

# === 3. MOTEUR API (Téléchargement) ===

def get_token():
    if not REFRESH_TOKEN: return None
    try:
        r = requests.post(f"{API_URL}/tokens", json={"refresh_token": REFRESH_TOKEN})
        r.raise_for_status()
        return r.json()["access_token"]
    except: return None

def sanitize(name):
    return re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_").strip()[:40]

def is_interesting(feed):
    txt = (f"{feed.get('provider')} {feed.get('feed_name')}").lower()
    if any(k in txt for k in INCLUDE_KEYWORDS): return True
    if any(k in txt for k in EXCLUDE_KEYWORDS): return False
    return True # Par défaut

# === 4. EXÉCUTION PRINCIPALE ===

if __name__ == "__main__":
    print("=== 🚄 LANCEMENT PIPELINE EURO-RAIL ===")
    token = get_token()
    if not token: exit("❌ Erreur Token")
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    all_dataframes = []

    for country in TARGET_COUNTRIES:
        print(f"\n🌍 PAYS : {country}")
        try:
            r = requests.get(f"{API_URL}/gtfs_feeds", headers={"Authorization": f"Bearer {token}"}, 
                           params={"country_code": country, "limit": 100})
            feeds = r.json()
        except: continue
        
        for feed in feeds:
            if not is_interesting(feed): continue
            
            dataset = feed.get("latest_dataset")
            if not dataset: continue
            url = dataset.get("hosted_url")
            if not url: continue
            
            provider = sanitize(feed.get("provider", "Unknown"))
            zip_path = f"{DATA_DIR}/{country}_{provider}_{feed.get('id')}.zip"
            
            # 1. Téléchargement (Si pas déjà là)
            if not os.path.exists(zip_path):
                print(f"   ⬇️ Téléchargement {provider}...", end=" ")
                try:
                    with open(zip_path, 'wb') as f:
                        f.write(requests.get(url).content)
                    print("OK")
                except:
                    print("Erreur")
                    continue
            
            # 2. Transformation (Appel de ta logique)
            df_feed = process_single_gtfs(zip_path, provider, country)
            
            if df_feed is not None:
                print(f"      ✅ {len(df_feed)} voyages longue distance ajoutés.")
                all_dataframes.append(df_feed)
            else:
                print("      ⚪ Aucun trajet pertinent.")

    # === 5. CONSOLIDATION (LOAD) ===
    print("\n📦 CONSOLIDATION DES DONNÉES...")
    if all_dataframes:
        final_db = pd.concat(all_dataframes, ignore_index=True)
        final_db.to_csv(OUTPUT_FILE, index=False)
        print(f"🎉 SUCCESS ! Base de données générée : {OUTPUT_FILE}")
        print(f"📊 Total : {len(final_db)} lignes.")
        print(final_db.head())
    else:
        print("⚠️ Aucune donnée n'a été retenue.")