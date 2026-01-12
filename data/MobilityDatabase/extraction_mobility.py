import pandas as pd
import zipfile
import math
import os

# === CONFIGURATION ===
# Chemin vers ton fichier téléchargé
FILE_PATH = "./data/tdg-67595-202601120151.zip"

# === 1. FONCTION UTILITAIRE : CALCUL DE DISTANCE (Haversine) ===
def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calcule la distance en km entre deux points GPS (Formule de Haversine).
    """
    R = 6371  # Rayon de la Terre en km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c

# === 2. CHARGEMENT DES DONNÉES ===
def load_gtfs_data(zip_path):
    print(f"📂 Lecture du fichier : {zip_path}")
    
    with zipfile.ZipFile(zip_path, 'r') as z:
        # On lit les fichiers essentiels directement depuis le ZIP
        # dtype={'route_short_name': str} évite les erreurs si le nom est "01" vs 1
        routes = pd.read_csv(z.open('routes.txt'), dtype=str)
        trips = pd.read_csv(z.open('trips.txt'), dtype=str)
        stops = pd.read_csv(z.open('stops.txt'), dtype={'stop_id': str})
        stop_times = pd.read_csv(z.open('stop_times.txt'), dtype={'trip_id': str, 'stop_id': str})
        
        # Conversion des types numériques nécessaires
        stops['stop_lat'] = pd.to_numeric(stops['stop_lat'])
        stops['stop_lon'] = pd.to_numeric(stops['stop_lon'])
        stop_times['stop_sequence'] = pd.to_numeric(stop_times['stop_sequence'])
        
    return routes, trips, stops, stop_times

# === 3. COEUR DE LA TRANSFORMATION ===
def process_gtfs(routes, trips, stops, stop_times):
    print("🔄 Début du traitement...")

    # A. FILTRAGE DES LIGNES (On ne garde que le TRAIN)
    # Codes GTFS standards : 2 = Rail (Longue distance), 100-117 = Ferroviaire varié
    # On convertit route_type en numérique pour filtrer
    routes['route_type'] = pd.to_numeric(routes['route_type'])
    
    # On garde Rail (2) et tout ce qui est ferroviaire (100 à 117)
    # On exclut Métro (1), Tram (0), Bus (3), Ferry (4)
    rail_routes = routes[
        (routes['route_type'] == 2) | 
        ((routes['route_type'] >= 100) & (routes['route_type'] <= 117))
    ]
    print(f"   -> Lignes ferroviaires conservées : {len(rail_routes)}")

    # B. RÉCUPÉRATION DES VOYAGES (TRIPS) LIÉS AUX LIGNES
    rail_trips = trips[trips['route_id'].isin(rail_routes['route_id'])]
    
    # C. RÉCUPÉRATION DES HORAIRES (STOP TIMES)
    # Attention : stop_times est souvent très lourd, on filtre tout de suite
    rail_stop_times = stop_times[stop_times['trip_id'].isin(rail_trips['trip_id'])]
    
    # D. AGGRÉGATION : TROUVER DÉPART ET ARRIVÉE POUR CHAQUE VOYAGE
    # On trie par séquence pour être sûr
    rail_stop_times = rail_stop_times.sort_values(['trip_id', 'stop_sequence'])
    
    # On prend le premier et le dernier arrêt de chaque voyage
    aggregations = {
        'stop_id': ['first', 'last'],
        'departure_time': 'first', # Heure de départ du premier arrêt
        'arrival_time': 'last'     # Heure d'arrivée du dernier arrêt
    }
    
    trips_boundaries = rail_stop_times.groupby('trip_id').agg(aggregations).reset_index()
    
    # Aplatissement des colonnes (MultiIndex vers colonnes simples)
    trips_boundaries.columns = ['trip_id', 'start_stop_id', 'end_stop_id', 'departure_time', 'arrival_time']
    
    # E. ENRICHISSEMENT AVEC LES COORDONNÉES GPS (JOINTURES)
    # On joint pour avoir les infos de la gare de départ
    merged = trips_boundaries.merge(stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], 
                                    left_on='start_stop_id', right_on='stop_id')
    merged = merged.rename(columns={'stop_name': 'origin_name', 'stop_lat': 'lat1', 'stop_lon': 'lon1'})
    
    # On joint pour avoir les infos de la gare d'arrivée
    merged = merged.merge(stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], 
                          left_on='end_stop_id', right_on='stop_id', suffixes=('_dep', '_arr'))
    merged = merged.rename(columns={'stop_name': 'destination_name', 'stop_lat': 'lat2', 'stop_lon': 'lon2'})
    
    return merged

# === 4. EXÉCUTION ===
if __name__ == "__main__":
    try:
        # 1. Charger
        r, t, s, st = load_gtfs_data(FILE_PATH)
        
        # 2. Transformer
        df_trips = process_gtfs(r, t, s, st)
        
        # 3. Calculer la distance (Apply ligne par ligne)
        print("📏 Calcul des distances...")
        df_trips['distance_km'] = df_trips.apply(
            lambda row: calculate_distance(row['lat1'], row['lon1'], row['lat2'], row['lon2']), axis=1
        )
        
        # 4. Filtre > 100km (Demande ObRail pour "Longue Distance")
        long_distance_trains = df_trips[df_trips['distance_km'] > 100].copy()
        
        # 5. Nettoyage final pour affichage
        final_df = long_distance_trains[[
            'trip_id', 'origin_name', 'destination_name', 
            'departure_time', 'arrival_time', 'distance_km'
        ]]
        
        print("\n✅ TRANSFORMATION TERMINÉE")
        print(f"   -> Trajets totaux trouvés : {len(df_trips)}")
        print(f"   -> Trajets longue distance (>100km) : {len(final_df)}")
        
        # Afficher un exemple
        print("\n🔎 Aperçu des données :")
        print(final_df.head(10))
        
        # Optionnel : Sauvegarder en CSV pour vérifier
        # final_df.to_csv("resultat_test_gtfs.csv", index=False)

    except FileNotFoundError:
        print(f"❌ Erreur : Le fichier {FILE_PATH} est introuvable. Vérifie le chemin.")
    except Exception as e:
        print(f"❌ Erreur critique : {e}")