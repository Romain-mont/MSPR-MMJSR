"""
Enrichissement : calcul de duration_h par corridor ferroviaire.

Méthode : vitesse commerciale moyenne par type de train (distance / vitesse).
Sources : RFF / SNCF rapports vitesses commerciales, données UIC 2022.

Les vitesses tiennent compte des arrêts intermédiaires et de l'accélération/décélération.
"""
import os
import pandas as pd

STAGING_PATH = os.path.join(os.path.dirname(__file__), '..', 'donnee',
                            'staging_fact_route_analysis.csv')

# Vitesse commerciale moyenne en km/h par type de train
# (inclut arrêts, ralentissements, portions de voie classique)
AVG_SPEED_KMH = {
    'TGV':                        220,
    'TGV Nuit':                   100,
    'ICE':                        190,
    'ICE Nuit':                    85,
    'AVE':                        200,
    'AVE Nuit':                    85,
    'EuroCity':                   130,
    'EuroCity Nuit':               80,
    'InterCity':                  120,
    'InterCity Nuit':              80,
    'EuroNight':                   75,
    'Nightjet':                    75,
    'Train Longue Distance':      110,
    'Train Longue Distance Nuit':  70,
    'Train Nuit':                  70,
}

DEFAULT_SPEED = 100  # fallback si type inconnu

df = pd.read_csv(STAGING_PATH)
print(f"Chargé : {len(df)} corridors")

df['avg_speed_kmh'] = df['vehicule_type'].map(AVG_SPEED_KMH).fillna(DEFAULT_SPEED)
df['duration_h'] = (df['distance_km'] / df['avg_speed_kmh']).round(2)

# Vérification
print("\nDuration_h moyen par type de train :")
check = df.groupby('vehicule_type').agg(
    dist_moy=('distance_km', 'mean'),
    speed=('avg_speed_kmh', 'first'),
    duration_moy_h=('duration_h', 'mean'),
).round(2)
check['duration_moy_min'] = (check['duration_moy_h'] * 60).round(0).astype(int)
print(check[['dist_moy', 'speed', 'duration_moy_h', 'duration_moy_min']].to_string())

df.drop(columns=['avg_speed_kmh'], inplace=True)
df.to_csv(STAGING_PATH, index=False)
print(f"\n✅ duration_h ajouté dans {STAGING_PATH}")
print(f"   min={df['duration_h'].min():.2f}h  max={df['duration_h'].max():.2f}h  "
      f"médiane={df['duration_h'].median():.2f}h")
