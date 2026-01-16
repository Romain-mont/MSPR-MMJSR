import pandas as pd
import zipfile
import os
import glob
import math

# === CONFIGURATION ===
INPUT_FOLDER = "./data/raw_feeds" 
OUTPUT_FILE = "dataset_intermediaire_europe.csv"

# === 1. OUTILS D'ANALYSE ===

def get_zip_inventory(zip_path):
    """Liste les fichiers présents dans le ZIP et repère les 'exotiques'"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            files = z.namelist()
            
        standards = ['agency.txt', 'stops.txt', 'routes.txt', 'trips.txt', 'stop_times.txt', 'calendar.txt', 'calendar_dates.txt', 'feed_info.txt']
        extras = [f for f in files if f not in standards and f.endswith('.txt')]
        
        return files, extras
    except:
        return [], []

def calculate_distance(lat1, lon1, lat2, lon2):
    """Haversine Formula"""
    try:
        R = 6371
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    except:
        return 0.0

def gtfs_time_to_hours(time_str):
    try:
        if not isinstance(time_str, str): return 0.0
        h, m, s = map(int, time_str.split(':'))
        return h + m/60 + s/3600
    except:
        return 0.0

# === 2. MOTEUR D'EXTRACTION ===

def process_single_zip(zip_path):
    filename = os.path.basename(zip_path)
    
    # 1. Inspection du contenu
    all_files, extra_files = get_zip_inventory(zip_path)
    if not all_files:
        return None
    
    # On affiche s'il y a des fichiers surprises
    if extra_files:
        print(f"   🎁 Fichiers bonus dans {filename} : {extra_files}")

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Lecture Routes
            routes = pd.read_csv(z.open('routes.txt'), dtype=str)
            routes['route_type'] = pd.to_numeric(routes['route_type'], errors='coerce')
            
            # Filtre Train
            rail_routes = routes[
                (routes['route_type'] == 2) | 
                ((routes['route_type'] >= 100) & (routes['route_type'] <= 117))
            ]
            
            if len(rail_routes) == 0:
                return None

            # Lecture Trips
            trips = pd.read_csv(z.open('trips.txt'), dtype=str)
            rail_trips = trips[trips['route_id'].isin(rail_routes['route_id'])]
            
            if len(rail_trips) == 0:
                return None
                
            # Lecture Stops & Times (Lourds)
            stops = pd.read_csv(z.open('stops.txt'), dtype={'stop_id': str})
            stops['stop_lat'] = pd.to_numeric(stops['stop_lat'], errors='coerce')
            stops['stop_lon'] = pd.to_numeric(stops['stop_lon'], errors='coerce')
            
            stop_times = pd.read_csv(z.open('stop_times.txt'), dtype={'trip_id': str, 'stop_id': str})
            rail_stop_times = stop_times[stop_times['trip_id'].isin(rail_trips['trip_id'])]
            
            # Agrégation (Départ / Arrivée)
            rail_stop_times = rail_stop_times.sort_values(['trip_id', 'stop_sequence'])
            agg = rail_stop_times.groupby('trip_id').agg({
                'stop_id': ['first', 'last'],
                'departure_time': 'first',
                'arrival_time': 'last'
            }).reset_index()
            agg.columns = ['trip_id', 'start_stop_id', 'end_stop_id', 'departure_time', 'arrival_time']
            
            # Enrichissement (Noms & GPS)
            merged = agg.merge(stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], 
                               left_on='start_stop_id', right_on='stop_id') \
                        .rename(columns={'stop_name': 'origin_name', 'stop_lat': 'lat1', 'stop_lon': 'lon1'})
                        
            merged = merged.merge(stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], 
                                  left_on='end_stop_id', right_on='stop_id', suffixes=('_dep', '_arr')) \
                           .rename(columns={'stop_name': 'destination_name', 'stop_lat': 'lat2', 'stop_lon': 'lon2'})
            
            # Calcul Distance
            merged['distance_km'] = merged.apply(lambda r: calculate_distance(r['lat1'], r['lon1'], r['lat2'], r['lon2']), axis=1)
            
            # Filtre Longue Distance (> 100km)
            final_df = merged[merged['distance_km'] > 100].copy()
            
            if len(final_df) > 0:
                # Ajout Métadonnées
                final_df['source_file'] = filename
                
                # Calculs Métiers rapides
                final_df['duration_h'] = (final_df['arrival_time'].apply(gtfs_time_to_hours) - final_df['departure_time'].apply(gtfs_time_to_hours)).round(2)
                final_df['train_type'] = "Jour" 
                final_df['total_emission_kgco2e'] = (final_df['distance_km'] * 4 / 1000).round(2) # Hypothèse élec jour
                
                # Nettoyage colonnes
                keep_cols = ['trip_id', 'origin_name', 'destination_name', 'departure_time', 'arrival_time', 'distance_km', 'duration_h', 'train_type', 'total_emission_kgco2e', 'source_file']
                return final_df[keep_cols]
            
            return None

    except Exception as e:
        print(f"   ❌ Erreur lecture {filename}: {e}")
        return None

# === 3. ORCHESTRATION ===

if __name__ == "__main__":
    zip_files = glob.glob(os.path.join(INPUT_FOLDER, "*.zip"))
    print(f"📂 {len(zip_files)} fichiers trouvés dans {INPUT_FOLDER}. Début de la compilation...")
    
    all_data = []
    
    for zf in zip_files:
        print(f"   ⚙️ Traitement de {os.path.basename(zf)}...", end="\r")
        df = process_single_zip(zf)
        if df is not None:
            all_data.append(df)
            print(f"   ✅ {len(df)} trains ajoutés ({os.path.basename(zf)})")
            
    if all_data:
        full_dataset = pd.concat(all_data, ignore_index=True)
        print("\n" + "="*50)
        print(f"🎉 COMPILATION TERMINÉE !")
        print(f"📊 Total trains compilés : {len(full_dataset)}")
        print(f"💾 Sauvegarde dans : {OUTPUT_FILE}")
        full_dataset.to_csv(OUTPUT_FILE, index=False)
        print("="*50)
        print("Aperçu :")
        print(full_dataset.head())
    else:
        print("\n❌ Aucun train longue distance trouvé dans l'ensemble des fichiers.")