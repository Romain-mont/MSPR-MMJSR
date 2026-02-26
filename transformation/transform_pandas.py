"""
Transformation ETL avec Pandas (sans Spark)
Version stable pour gros volumes de données sur Windows
"""

import os
import pandas as pd
import numpy as np
from functools import reduce
import warnings
warnings.filterwarnings('ignore')

# === CHEMINS ===
RAW_MOBILITY_DIR = './data/raw/mobility_gtfs'
RAW_BACKONTRACK_DIR = './data/raw/backontrack_csv'
RAW_AIRPORTS_DIR = './data/raw/airports'
STAGING_DIR = './data/staging'

# === PAYS EUROPÉENS ===
EU_COUNTRIES = ['FR', 'DE', 'CH', 'BE', 'NL', 'AT', 'IT', 'ES', 'PT', 'PL', 
                'CZ', 'SK', 'HU', 'SI', 'HR', 'DK', 'SE', 'NO', 'FI', 'IE', 
                'GB', 'LU', 'RO', 'BG', 'GR', 'EE', 'LV', 'LT']

# === FONCTIONS UTILITAIRES ===

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcule la distance Haversine entre deux points (vectorisé)."""
    R = 6371.0  # Rayon de la Terre en km
    
    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    lon1_rad = np.radians(lon1)
    lon2_rad = np.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return R * c


def calculate_co2(distance_km, vehicule_type):
    """
    Calcule le CO2 en kg par passager (vectorisé).
    
    Facteurs (kg CO2 / 100 km / passager):
    - TGV/Train: 0.29
    - Avion court (<500km): 22.5
    - Avion moyen (500-1000km): 18.45
    - Avion long (>1000km): 15.2
    """
    co2 = np.where(
        vehicule_type.str.contains('Avion', case=False, na=False),
        np.where(
            distance_km < 500, distance_km * 22.5 / 100,
            np.where(
                distance_km < 1000, distance_km * 18.45 / 100,
                distance_km * 15.2 / 100
            )
        ),
        distance_km * 0.29 / 100  # Train
    )
    return co2


# === LECTURE DES DONNÉES ===

def read_mobility_provider(provider_dir, max_routes=500):
    """Lit un provider GTFS et retourne un DataFrame standardisé."""
    provider_name = os.path.basename(provider_dir)
    
    try:
        # Fichiers requis
        required = ['stops.txt', 'stop_times.txt', 'trips.txt', 'routes.txt']
        for f in required:
            if not os.path.exists(os.path.join(provider_dir, f)):
                return None
        
        # Lecture
        stops = pd.read_csv(f"{provider_dir}/stops.txt", low_memory=False)
        stop_times = pd.read_csv(f"{provider_dir}/stop_times.txt", low_memory=False)
        trips = pd.read_csv(f"{provider_dir}/trips.txt", low_memory=False)
        routes = pd.read_csv(f"{provider_dir}/routes.txt", low_memory=False)
        
        # Filtrer les routes train (type 2 ou 100-117)
        routes['route_type'] = pd.to_numeric(routes['route_type'], errors='coerce')
        rail_routes = routes[
            (routes['route_type'] == 2) | 
            ((routes['route_type'] >= 100) & (routes['route_type'] <= 117))
        ]
        
        if len(rail_routes) == 0:
            return None
        
        # Jointure trips + routes
        rail_trips = trips.merge(
            rail_routes[['route_id', 'route_type']], 
            on='route_id', 
            how='inner'
        )
        
        if len(rail_trips) == 0:
            return None
        
        # Trouver origine et destination par trip
        stop_times['stop_sequence'] = pd.to_numeric(stop_times['stop_sequence'], errors='coerce')
        
        # Origine (premier arrêt)
        origins = stop_times.sort_values('stop_sequence').groupby('trip_id').first().reset_index()
        origins = origins[['trip_id', 'stop_id', 'departure_time']].rename(
            columns={'stop_id': 'origin_stop_id', 'departure_time': 'departure_time'}
        )
        
        # Destination (dernier arrêt)
        dests = stop_times.sort_values('stop_sequence').groupby('trip_id').last().reset_index()
        dests = dests[['trip_id', 'stop_id', 'arrival_time']].rename(
            columns={'stop_id': 'dest_stop_id', 'arrival_time': 'arrival_time'}
        )
        
        # Combiner
        df = rail_trips.merge(origins, on='trip_id', how='inner')
        df = df.merge(dests, on='trip_id', how='inner')
        
        # Ajouter coordonnées
        stops['stop_lat'] = pd.to_numeric(stops['stop_lat'], errors='coerce')
        stops['stop_lon'] = pd.to_numeric(stops['stop_lon'], errors='coerce')
        
        df = df.merge(
            stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].rename(
                columns={'stop_id': 'origin_stop_id', 'stop_name': 'origin', 
                         'stop_lat': 'station_lat', 'stop_lon': 'station_long'}
            ),
            on='origin_stop_id', how='left'
        )
        
        df = df.merge(
            stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].rename(
                columns={'stop_id': 'dest_stop_id', 'stop_name': 'destination',
                         'stop_lat': 'station_lat_dest', 'stop_lon': 'station_long_dest'}
            ),
            on='dest_stop_id', how='left'
        )
        
        # Type de véhicule
        df['vehicule_type'] = np.where(df['route_type'] == 102, 'Train Nuit', 'Train Jour')
        
        # Sélection colonnes
        result = df[[
            'origin', 'destination', 'vehicule_type',
            'station_lat', 'station_long', 'station_lat_dest', 'station_long_dest',
            'departure_time', 'arrival_time'
        ]].copy()
        
        result['source'] = 'mobility_db'
        result['provider'] = provider_name
        
        # Limiter
        if len(result) > max_routes:
            result = result.head(max_routes)
        
        print(f"✅ ({len(result)} trajets)")
        return result
        
    except Exception as e:
        print(f"❌ ({e})")
        return None


def read_all_mobility(countries=None, max_routes_per_country=2000):
    """Lit tous les providers Mobility Database."""
    if countries is None:
        countries = EU_COUNTRIES
    
    print(f"\n🌐 Lecture Mobility Database ({len(countries)} pays)...")
    
    if not os.path.exists(RAW_MOBILITY_DIR):
        print(f"   ❌ Dossier introuvable : {RAW_MOBILITY_DIR}")
        return None
    
    all_dfs = []
    total_routes = 0
    
    for country in countries:
        country_routes = 0
        print(f"\n   🗺️  {country}:")
        
        for provider_name in sorted(os.listdir(RAW_MOBILITY_DIR)):
            if not provider_name.startswith(f"{country}_"):
                continue
            
            if country_routes >= max_routes_per_country:
                print(f"   ⏭️  Limite {country} atteinte")
                break
            
            provider_path = os.path.join(RAW_MOBILITY_DIR, provider_name)
            if os.path.isdir(provider_path):
                print(f"      📂 {provider_name[:40]}...", end=" ", flush=True)
                remaining = max_routes_per_country - country_routes
                df = read_mobility_provider(provider_path, max_routes=remaining)
                if df is not None:
                    all_dfs.append(df)
                    country_routes += len(df)
        
        if country_routes > 0:
            print(f"   ✅ {country}: {country_routes} routes")
            total_routes += country_routes
    
    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)
        print(f"\n   📊 Total Mobility: {len(result)} trajets")
        return result
    
    return None


def read_backontrack():
    """Lit les données Back on Track."""
    print("\n📊 Lecture Back on Track...")
    
    trips_path = f"{RAW_BACKONTRACK_DIR}/back_on_track_trips.csv"
    stops_path = f"{RAW_BACKONTRACK_DIR}/back_on_track_stops.csv"
    
    if not os.path.exists(trips_path):
        print("   ❌ Fichiers manquants")
        return None
    
    try:
        trips = pd.read_csv(trips_path, low_memory=False)
        stops = pd.read_csv(stops_path, low_memory=False)
        
        # Colonnes origin/destination
        if 'trip_origin' not in trips.columns or 'trip_headsign' not in trips.columns:
            print("   ❌ Colonnes manquantes")
            return None
        
        df = trips[['trip_origin', 'trip_headsign', 'distance', 'emissions_co2e']].copy()
        df = df.rename(columns={
            'trip_origin': 'origin',
            'trip_headsign': 'destination'
        })
        
        # Ajouter coordonnées des stops
        stops['stop_lat'] = pd.to_numeric(stops.get('station_lat'), errors='coerce')
        stops['stop_lon'] = pd.to_numeric(stops.get('station_long'), errors='coerce')
        
        stops_coords = stops[['station_name', 'stop_lat', 'stop_lon']].drop_duplicates()
        
        df = df.merge(
            stops_coords.rename(columns={'station_name': 'origin', 'stop_lat': 'station_lat', 'stop_lon': 'station_long'}),
            on='origin', how='left'
        )
        df = df.merge(
            stops_coords.rename(columns={'station_name': 'destination', 'stop_lat': 'station_lat_dest', 'stop_lon': 'station_long_dest'}),
            on='destination', how='left'
        )
        
        df['vehicule_type'] = 'Train Jour'
        df['departure_time'] = None
        df['arrival_time'] = None
        df['source'] = 'back_on_track'
        df['provider'] = 'BackOnTrack'
        
        # Sélection colonnes
        result = df[[
            'origin', 'destination', 'vehicule_type',
            'station_lat', 'station_long', 'station_lat_dest', 'station_long_dest',
            'departure_time', 'arrival_time', 'source', 'provider'
        ]].drop_duplicates()
        
        print(f"   ✅ {len(result)} trajets")
        return result
        
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return None


def read_airports():
    """Lit les aéroports européens."""
    print("\n✈️  Lecture Aéroports...")
    
    # Trouver le fichier le plus récent
    if not os.path.exists(RAW_AIRPORTS_DIR):
        return None
    
    csv_files = [f for f in os.listdir(RAW_AIRPORTS_DIR) if f.endswith('.csv')]
    if not csv_files:
        return None
    
    latest = sorted(csv_files)[-1]
    df = pd.read_csv(f"{RAW_AIRPORTS_DIR}/{latest}", low_memory=False)
    
    # Filtrer Europe + gros/moyens aéroports
    eu_codes = EU_COUNTRIES + ['UK']
    df = df[df['country_code'].isin(eu_codes)]
    df = df[df['category'].isin(['large_airport', 'medium_airport'])]
    
    result = df[['airport_name', 'aero_lat', 'aero_long', 'category', 'iata_code', 'country_code', 'ident']].drop_duplicates()
    
    print(f"   ✅ {len(result)} aéroports européens")
    return result


def find_nearby_airports(stations_df, airports_df, max_distance_km=50):
    """Trouve l'aéroport le plus proche de chaque gare (<50km)."""
    print("\n🔗 Intermodalité (gares → aéroports)...")
    
    if stations_df is None or airports_df is None:
        return None
    
    # Gares uniques
    stations = stations_df[['origin', 'station_lat', 'station_long']].drop_duplicates()
    stations = stations.rename(columns={'origin': 'station_name', 'station_lat': 'lat', 'station_long': 'long'})
    stations = stations.dropna(subset=['lat', 'long'])
    
    print(f"   📊 {len(stations)} gares uniques")
    
    results = []
    airports_np = airports_df[['airport_name', 'aero_lat', 'aero_long', 'iata_code', 'category']].values
    
    for _, station in stations.iterrows():
        if pd.isna(station['lat']) or pd.isna(station['long']):
            continue
        
        # Distance vers tous les aéroports
        distances = haversine_distance(
            station['lat'], station['long'],
            airports_df['aero_lat'].values, airports_df['aero_long'].values
        )
        
        min_idx = np.argmin(distances)
        if distances[min_idx] < max_distance_km:
            results.append({
                'station_name': station['station_name'],
                'lat': station['lat'],
                'long': station['long'],
                'airport_name': airports_np[min_idx][0],
                'aero_lat': airports_np[min_idx][1],
                'aero_long': airports_np[min_idx][2],
                'iata_code': airports_np[min_idx][3],
                'category': airports_np[min_idx][4],
                'distance_to_airport_km': distances[min_idx]
            })
    
    if results:
        df = pd.DataFrame(results)
        print(f"   ✅ {len(df)} gares avec aéroport proche (<{max_distance_km}km)")
        return df
    
    return None


def generate_plane_routes(train_df, intermodal_df):
    """Génère les trajets avion équivalents aux trajets train."""
    print("\n✈️  Génération trajets avion...")
    
    if intermodal_df is None or len(intermodal_df) == 0:
        print("   ⚠️ Pas de données intermodales")
        return None
    
    # Paires O-D uniques
    pairs = train_df[['origin', 'destination']].drop_duplicates()
    print(f"   📊 {len(pairs)} paires train uniques")
    
    # Joindre aéroports origine
    pairs = pairs.merge(
        intermodal_df[['station_name', 'airport_name', 'aero_lat', 'aero_long', 'iata_code']].rename(
            columns={'station_name': 'origin', 'airport_name': 'airport_origin',
                     'aero_lat': 'airport_origin_lat', 'aero_long': 'airport_origin_long',
                     'iata_code': 'iata_origin'}
        ),
        on='origin', how='inner'
    )
    
    # Joindre aéroports destination
    pairs = pairs.merge(
        intermodal_df[['station_name', 'airport_name', 'aero_lat', 'aero_long', 'iata_code']].rename(
            columns={'station_name': 'destination', 'airport_name': 'airport_dest',
                     'aero_lat': 'airport_dest_lat', 'aero_long': 'airport_dest_long',
                     'iata_code': 'iata_dest'}
        ),
        on='destination', how='inner'
    )
    
    print(f"   ✈️  {len(pairs)} paires avec aéroports aux deux extrémités")
    
    if len(pairs) == 0:
        return None
    
    # Distance entre aéroports
    pairs['distance_km'] = haversine_distance(
        pairs['airport_origin_lat'], pairs['airport_origin_long'],
        pairs['airport_dest_lat'], pairs['airport_dest_long']
    )
    
    # Filtrer >100km
    pairs = pairs[pairs['distance_km'] > 100]
    
    # Formater
    planes = pd.DataFrame({
        'origin': pairs['airport_origin'],
        'destination': pairs['airport_dest'],
        'vehicule_type': 'Avion',
        'station_lat': pairs['airport_origin_lat'],
        'station_long': pairs['airport_origin_long'],
        'station_lat_dest': pairs['airport_dest_lat'],
        'station_long_dest': pairs['airport_dest_long'],
        'departure_time': None,
        'arrival_time': None,
        'source': 'airports',
        'provider': 'OurAirports',
        'distance_km': pairs['distance_km']
    })
    
    # CO2
    planes['co2_kg'] = calculate_co2(planes['distance_km'], planes['vehicule_type'])
    
    print(f"   ✅ {len(planes)} trajets avion générés")
    return planes


def run_transform():
    """Exécute la transformation complète."""
    print("\n" + "=" * 60)
    print("🚀 TRANSFORMATION PANDAS (Europe)")
    print("=" * 60)
    
    os.makedirs(STAGING_DIR, exist_ok=True)
    
    # === ÉTAPE 1: LECTURE ===
    df_mobility = read_all_mobility(max_routes_per_country=3000)
    df_backontrack = read_backontrack()
    df_airports = read_airports()
    
    # === ÉTAPE 2: UNION TRAIN ===
    print("\n🔗 Union des sources train...")
    train_dfs = [df for df in [df_mobility, df_backontrack] if df is not None]
    
    if not train_dfs:
        print("❌ Aucune donnée train")
        return
    
    df_train = pd.concat(train_dfs, ignore_index=True)
    print(f"   📊 Total train : {len(df_train)} trajets")
    
    # Dédoublonner
    df_train = df_train.drop_duplicates(subset=['origin', 'destination'])
    print(f"   📊 Après dédup : {len(df_train)} trajets uniques")
    
    # === ÉTAPE 3: DISTANCE & CO2 ===
    print("\n📏 Calcul distances et CO2...")
    
    df_train['distance_km'] = haversine_distance(
        df_train['station_lat'], df_train['station_long'],
        df_train['station_lat_dest'], df_train['station_long_dest']
    )
    
    df_train['co2_kg'] = calculate_co2(df_train['distance_km'], df_train['vehicule_type'])
    
    valid = df_train['distance_km'].notna().sum()
    print(f"   ✅ {valid}/{len(df_train)} distances calculées")
    
    # === ÉTAPE 4: INTERMODALITÉ ===
    intermodal = find_nearby_airports(df_train, df_airports)
    
    # === ÉTAPE 5: GÉNÉRATION AVION ===
    df_planes = generate_plane_routes(df_train, intermodal)
    
    # === ÉTAPE 6: UNION FINALE ===
    if df_planes is not None and len(df_planes) > 0:
        # Aligner colonnes
        common_cols = [c for c in df_train.columns if c in df_planes.columns]
        df_final = pd.concat([df_train[common_cols], df_planes[common_cols]], ignore_index=True)
        print(f"\n   ✅ Total train + avion : {len(df_final)} trajets")
    else:
        df_final = df_train
    
    # === ÉTAPE 7: EXPORT ===
    print("\n💾 Export staging...")
    
    df_final.to_csv(f"{STAGING_DIR}/final_routes.csv", index=False)
    print(f"   📂 {STAGING_DIR}/final_routes.csv ({len(df_final)} lignes)")
    
    if df_airports is not None:
        df_airports.to_csv(f"{STAGING_DIR}/staging_airports.csv", index=False)
        print(f"   📂 {STAGING_DIR}/staging_airports.csv ({len(df_airports)} lignes)")
    
    if intermodal is not None:
        intermodal.to_csv(f"{STAGING_DIR}/staging_intermodal.csv", index=False)
        print(f"   📂 {STAGING_DIR}/staging_intermodal.csv ({len(intermodal)} lignes)")
    
    # === STATS FINALES ===
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ")
    print("=" * 60)
    
    print("\n📈 Par source:")
    print(df_final['source'].value_counts().to_string())
    
    print("\n📈 Par type de véhicule:")
    print(df_final['vehicule_type'].value_counts().to_string())
    
    print("\n📈 Statistiques CO2:")
    stats = df_final.groupby('vehicule_type').agg({
        'distance_km': ['mean', 'max'],
        'co2_kg': ['mean', 'sum']
    }).round(2)
    print(stats.to_string())
    
    print("\n✅ Transformation terminée !")
    return df_final


if __name__ == "__main__":
    run_transform()
