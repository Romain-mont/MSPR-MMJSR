"""
Enrichissement des données de fréquentation via matching GPS.
Lit donnee/staging_fact_route_analysis.csv, trouve pour chaque gare
la gare SNCF la plus proche (rayon 200m), récupère son trafic annuel.
"""
import os
import glob
import math
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

RADIUS_M        = 200
DATA_FILE       = os.path.join(os.path.dirname(__file__), '..', 'donnee', 'staging_fact_route_analysis.csv')
SNCF_FREQ_DIR   = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'sncf_frequentation')
SNCF_GARES_DIR  = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'sncf_gares_reference')


def haversine_m(lat1, lon1, lat2, lon2):
    """Distance en mètres entre deux points GPS."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_sncf_gares_with_traffic():
    """
    Charge le référentiel gares SNCF (GPS + UIC) et le joint
    avec la fréquentation sur le code UIC.
    Retourne un DataFrame : nom, lat, lon, uic, annual_traffic
    """
    # Référentiel gares
    gares_files = sorted(glob.glob(os.path.join(SNCF_GARES_DIR, 'sncf_gares_reference_*.csv')))
    if not gares_files:
        print("❌ Référentiel gares SNCF introuvable — lancer extract_sncf_gares_reference()")
        return None

    df_gares = pd.read_csv(gares_files[-1], sep=';', encoding='utf-8-sig')
    print(f"Référentiel gares : {len(df_gares)} gares | colonnes : {list(df_gares.columns)}")

    # Parse position_geographique "lat, lon"
    def parse_pos(pos):
        try:
            parts = str(pos).split(',')
            return float(parts[0].strip()), float(parts[1].strip())
        except Exception:
            return None, None

    df_gares[['lat','lon']] = df_gares['position_geographique'].apply(
        lambda p: pd.Series(parse_pos(p))
    )
    df_gares = df_gares.dropna(subset=['lat','lon'])

    # Normaliser codes_uic : garder le premier si plusieurs séparés par "|"
    df_gares['uic'] = df_gares['codes_uic'].astype(str).str.split('|').str[0].str.strip()

    # Fréquentation
    freq_files = sorted(glob.glob(os.path.join(SNCF_FREQ_DIR, 'sncf_frequentation_*.csv')))
    if not freq_files:
        print("⚠️  Fréquentation SNCF introuvable — trafic sera NULL")
        df_gares['annual_traffic'] = None
        return df_gares[['nom','lat','lon','uic','annual_traffic']]

    df_freq = pd.read_csv(freq_files[-1], sep=';', encoding='utf-8-sig')
    traffic_col = 'Total Voyageurs 2024' if 'Total Voyageurs 2024' in df_freq.columns else 'Total Voyageurs 2023'
    df_freq['uic'] = df_freq['Code UIC'].astype(str).str.strip()
    df_freq['annual_traffic'] = pd.to_numeric(df_freq[traffic_col], errors='coerce')
    df_freq = df_freq[['uic','annual_traffic']].dropna()

    df_merged = df_gares.merge(df_freq, on='uic', how='left')
    matched = df_merged['annual_traffic'].notna().sum()
    print(f"Joint fréquentation sur UIC : {matched}/{len(df_merged)} gares avec trafic")

    return df_merged[['nom','lat','lon','uic','annual_traffic']]


def find_nearest_station(target_lat, target_lon, df_gares, radius_m=RADIUS_M):
    """
    Retourne le trafic de la gare SNCF la plus proche dans le rayon donné.
    Pré-filtre par bbox pour éviter de calculer toutes les distances.
    """
    if pd.isna(target_lat) or pd.isna(target_lon):
        return None

    # Bbox ~200m en degrés (~0.002°)
    margin = 0.003
    candidates = df_gares[
        (df_gares['lat'] >= target_lat - margin) &
        (df_gares['lat'] <= target_lat + margin) &
        (df_gares['lon'] >= target_lon - margin) &
        (df_gares['lon'] <= target_lon + margin)
    ].copy()

    if candidates.empty:
        return None

    candidates['dist_m'] = candidates.apply(
        lambda r: haversine_m(target_lat, target_lon, r['lat'], r['lon']),
        axis=1
    )
    candidates = candidates[candidates['dist_m'] <= radius_m]

    if candidates.empty:
        return None

    best = candidates.loc[candidates['dist_m'].idxmin()]
    return best['annual_traffic'] if pd.notna(best['annual_traffic']) else None


def main():
    print("=== Enrichissement fréquentation SNCF via matching GPS ===")
    print(f"Rayon de matching : {RADIUS_M}m\n")

    # Charger les données SNCF
    df_gares = load_sncf_gares_with_traffic()
    if df_gares is None:
        return

    # Charger le fichier ML
    df = pd.read_csv(DATA_FILE)
    print(f"\nFichier ML : {len(df)} corridors")
    print(f"Avant enrichissement :")
    print(f"  origin_station_traffic non-null : {df['origin_station_traffic'].notna().sum()}/{len(df)}")
    print(f"  dest_station_traffic non-null   : {df['dest_station_traffic'].notna().sum()}/{len(df)}")

    # Matching GPS pour les stations sans trafic
    print("\nMatching GPS en cours...")
    updated_origin = 0
    updated_dest   = 0

    for idx, row in df.iterrows():
        # Origin
        if pd.isna(row['origin_station_traffic']):
            traffic = find_nearest_station(row['station_lat'], row['station_long'], df_gares)
            if traffic is not None:
                df.at[idx, 'origin_station_traffic'] = traffic
                updated_origin += 1

        # Destination
        if pd.isna(row['dest_station_traffic']):
            traffic = find_nearest_station(row['station_lat_dest'], row['station_long_dest'], df_gares)
            if traffic is not None:
                df.at[idx, 'dest_station_traffic'] = traffic
                updated_dest += 1

    print(f"\nRésultat matching GPS :")
    print(f"  Nouvelles origines matchées  : +{updated_origin}")
    print(f"  Nouvelles destinations matchées : +{updated_dest}")
    print(f"\nAprès enrichissement :")
    print(f"  origin_station_traffic non-null : {df['origin_station_traffic'].notna().sum()}/{len(df)} ({df['origin_station_traffic'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  dest_station_traffic non-null   : {df['dest_station_traffic'].notna().sum()}/{len(df)} ({df['dest_station_traffic'].notna().sum()/len(df)*100:.1f}%)")

    # Sauvegarde
    df.to_csv(DATA_FILE, index=False)
    print(f"\n✅ Fichier mis à jour : {DATA_FILE}")


if __name__ == "__main__":
    main()
