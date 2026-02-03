import pandas as pd
from datetime import datetime, timedelta
import os

def calculate_duration(dep, arr):
    """Calcule la durée en heures entre deux horaires HH:MM avec gestion du passage minuit"""
    try:
        dep_time = datetime.strptime(str(dep), "%H:%M")
        arr_time = datetime.strptime(str(arr), "%H:%M")
        
        # Si arrivée < départ → passage minuit (+24h)
        if arr_time < dep_time:
            arr_time += timedelta(days=1)
        
        duration = (arr_time - dep_time).total_seconds() / 3600
        return round(duration, 2)
    except:
        return None

def process_backontrack_local():
    print("🏗️ Début de l'assemblage du puzzle Back-on-Track...")

    # 1. CHARGEMENT
    try:
        agencies = pd.read_csv("./data/BOT/back_on_track_agencies.csv")
        routes = pd.read_csv("./data/BOT/back_on_track_routes.csv")
        trips = pd.read_csv("./data/BOT/back_on_track_trips.csv")
        stops = pd.read_csv("./data/BOT/back_on_track_stops.csv")
        trip_stops = pd.read_csv("./data/BOT/back_on_track_trip_stop.csv")
        
        # Nettoyage des colonnes Unnamed
        trips = trips.loc[:, ~trips.columns.str.contains('^Unnamed')]
        trip_stops = trip_stops.loc[:, ~trip_stops.columns.str.contains('^Unnamed')]
        
        print(f"✓ {len(agencies)} agences, {len(routes)} routes, {len(trips)} trips")
        print(f"✓ {len(stops)} gares, {len(trip_stops)} arrêts")
        
    except FileNotFoundError as e:
        print(f"❌ Fichier manquant : {e}")
        return

    # 2. MAPPING DES TYPES DE TRAIN (selon GTFS standard)
    route_type_mapping = {
        0: "Tramway",
        1: "Métro",
        2: "Train régional",
        3: "Bus",
        100: "Train longue distance",
        101: "Train grande vitesse",
        102: "Train de nuit",
    }
    
    # 3. FUSIONS PROGRESSIVES
    print("\n🔗 Fusion des données...")
    
    # ÉTAPE 1: Routes + Agences
    routes_full = routes.merge(agencies, on='agency_id', how='left')
    # ↳ Chaque route récupère le nom de sa compagnie

    # ÉTAPE 2: Trips + Routes enrichies  
    trips_full = trips.merge(routes_full, on='route_id', how='left')
    # ↳ Chaque voyage récupère infos route + agence

    # ÉTAPE 3: Trip_stops + Trips enrichis
    data = trip_stops.merge(trips_full, on='trip_id', how='left')
    # ↳ Chaque arrêt récupère infos voyage complet

    # ÉTAPE 4: + Informations des gares
    data = data.merge(stops, on='stop_id', how='left')
    # ↳ Chaque arrêt récupère nom/pays de la gare
    
    print(f"✓ Dataset fusionné : {len(data)} lignes")
    
    # 4. APLATISSEMENT
    print("\n📊 Création des trajets origine-destination...")
    
    data = data.sort_values(['trip_id', 'stop_sequence'])
    
    origins = data.groupby('trip_id').first().reset_index()
    destinations = data.groupby('trip_id').last().reset_index()

    # 5. EXTRACTION DES PAYS depuis le champ countries
    origins['origin_country'] = origins['countries'].apply(
        lambda x: str(x).split(',')[0].strip() if pd.notna(x) else 'XX'
    )
    
    destinations['destination_country'] = destinations['countries'].apply(
        lambda x: str(x).split(',')[-1].strip() if pd.notna(x) else 'XX'
    )
    
    # 6. ASSEMBLAGE FINAL
    df_final = pd.DataFrame({
        'trip_id': origins['trip_id'],
        'agency_name': origins['agency_name'],
        
        'route_name': origins['route_long_name'].fillna(
            origins['route_short_name']
        ).fillna(
            origins['trip_origin'] + " - " + origins['trip_headsign']
        ),
        
        'train_type': origins['route_type'].map(route_type_mapping).fillna("Train de nuit"),
        
        'service_type': origins.apply(
            lambda x: 'International' if ',' in str(x.get('countries', '')) else 'National',
            axis=1
        ),
        
        'origin_stop_name': origins['stop_name'],
        'origin_country': origins['origin_country'],
        
        'destination_stop_name': destinations['stop_name'],
        'destination_country': destinations['destination_country'],
        
        'departure_time': origins['departure_time'],
        'arrival_time': destinations['arrival_time'],
        
        'distance_km': origins.get('distance', None),
        'duration_h': origins.get('duration', None),
        'emission_gco2e_pkm': origins.get('co2_per_km', None),
        'total_emission_kgco2e': origins.get('emissions_co2e', None),
        
        'source_dataset': 'Back-on-Track'
    })
    
    # 7. NETTOYAGE : Supprimer trajets sans origine/destination
    df_final = df_final.dropna(subset=['origin_stop_name', 'destination_stop_name'])
    
    print(f"✅ {len(df_final)} trajets complets créés")
    
    # 8. GESTION DES VALEURS MANQUANTES
    print("\n🔧 Traitement des valeurs manquantes...")
    
    print("📊 Avant traitement :")
    missing_before = df_final.isnull().sum()
    print(missing_before[missing_before > 0])
    
    # A. CALCUL DES DURÉES MANQUANTES
    print("\n  → Calcul des durées manquantes depuis horaires...")
    df_final['duration_calculated'] = df_final.apply(
        lambda x: calculate_duration(x['departure_time'], x['arrival_time']), 
        axis=1
    )
    
    # Remplacer les durées manquantes par les valeurs calculées
    df_final['duration_h'] = df_final['duration_h'].fillna(df_final['duration_calculated'])
    
    # Supprimer la colonne temporaire
    df_final = df_final.drop('duration_calculated', axis=1)
    
    durations_filled = missing_before['duration_h'] - df_final['duration_h'].isnull().sum()
    print(f"    ✓ {durations_filled} durées calculées")
    
    # B. IMPUTATION DES ÉMISSIONS PAR TYPE DE TRAIN
    print("\n  → Imputation des émissions par type de train...")
    mean_emissions = df_final.groupby('train_type')['emission_gco2e_pkm'].mean()
    print(f"    Moyennes par type : {mean_emissions.to_dict()}")
    
    # ✅ FONCTION AMÉLIORÉE pour gestion explicite des trains de nuit
    def get_emission_value(row):
        if pd.notna(row['emission_gco2e_pkm']):
            return row['emission_gco2e_pkm']
        
        # Récupérer la moyenne du type, sinon valeur par défaut
        mean_val = mean_emissions.get(row['train_type'])
        
        if pd.notna(mean_val):
            return mean_val
        elif row['train_type'] == 'Train de nuit':
            return 14.0  # Valeur standard ADEME pour trains de nuit électriques
        else:
            return 17.0  # Moyenne régionale générique

    df_final['emission_gco2e_pkm'] = df_final.apply(get_emission_value, axis=1)
    
    emissions_filled = missing_before['emission_gco2e_pkm'] - df_final['emission_gco2e_pkm'].isnull().sum()
    print(f"    ✓ {emissions_filled} émissions imputées")
    
    # C. RECALCUL DES ÉMISSIONS TOTALES
    print("\n  → Recalcul des émissions totales...")
    df_final['total_emission_kgco2e'] = df_final.apply(
        lambda x: round((x['distance_km'] * x['emission_gco2e_pkm']) / 1000, 2)
                  if pd.notna(x['distance_km']) and pd.notna(x['emission_gco2e_pkm'])
                  else x['total_emission_kgco2e'],
        axis=1
    )
    
    # D. VALEURS PAR DÉFAUT POUR CHAMPS TEXTE
    print("\n  → Remplissage des champs texte manquants...")
    df_final['agency_name'] = df_final['agency_name'].fillna('Opérateur non renseigné')
    df_final['route_name'] = df_final['route_name'].fillna('Non renseigné')
    
    # E. STATISTIQUES APRÈS TRAITEMENT
    print("\n📊 Après traitement :")
    missing_after = df_final.isnull().sum()
    print(missing_after[missing_after > 0])
    
    if missing_after.sum() == 0:
        print("  🎉 Aucune valeur manquante restante sur les champs critiques !")
    
    # Calcul du taux de complétude
    completeness = ((len(df_final) - missing_after) / len(df_final) * 100).round(1)
    print("\n📈 Taux de complétude par champ :")
    print(completeness.sort_values(ascending=False))
    
    # 9. APERÇU FINAL
    print("\n🔍 Aperçu des données nettoyées :")
    print(df_final.head())
    
    # 10. VALIDATION FINALE
    print("\n✅ VALIDATION FINALE")
    print(f"- Trajets avec pays valides : {(df_final['origin_country'] != 'XX').sum()}/{len(df_final)}")
    print(f"- Trajets avec émissions : {df_final['emission_gco2e_pkm'].notna().sum()}/{len(df_final)}")
    print(f"- Trajets avec distance : {df_final['distance_km'].notna().sum()}/{len(df_final)}")
    
    stats = {
        'Total trajets': len(df_final),
        'Trajets internationaux': (df_final['service_type'] == 'International').sum(),
        'Trajets nationaux': (df_final['service_type'] == 'National').sum(),
        'Gares uniques': len(set(df_final['origin_stop_name']) | set(df_final['destination_stop_name'])),
        'Agences': df_final['agency_name'].nunique(),
        'Pays couverts': len(set(df_final['origin_country']) | set(df_final['destination_country']) - {'XX'})
    }
    
    print("\n📊 STATISTIQUES POUR LE RAPPORT :")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    
    # 11. SAUVEGARDE
    os.makedirs("./data/processed", exist_ok=True)
    df_final.to_csv("./data/processed/clean_backontrack_final.csv", index=False)
    print("\n💾 Fichier sauvegardé : ./data/processed/clean_backontrack_final.csv")
    
    # 12. GÉNÉRATION D'UN RAPPORT DE QUALITÉ
    quality_report = pd.DataFrame({
        'Champ': df_final.columns,
        'Valeurs_manquantes': missing_after.values,
        'Taux_completude_%': completeness.values
    })
    quality_report.to_csv("./data/processed/quality_report_backontrack.csv", index=False)
    print("📄 Rapport qualité sauvegardé : ./data/processed/quality_report_backontrack.csv")
    

if __name__ == "__main__":
    process_backontrack_local()
