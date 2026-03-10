"""
ANALYSE AUTOMATIQUE DES RÉSULTATS DU PIPELINE ETL
==================================================
Ce script génère un rapport détaillé à la fin de l'exécution du pipeline.
"""

import pandas as pd
import os
from datetime import datetime


def generate_analysis_report(csv_path, output_path):
    """    
    Args:
        csv_path: Chemin vers le fichier final_routes.csv
        output_path: Chemin où sauvegarder le rapport
    """
    
    # Vérifier que le fichier existe
    if not os.path.exists(csv_path):
        print(f"Fichier non trouvé: {csv_path}")
        return False
    
    # Charger les données
    print(f"Chargement des données depuis {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Créer le dossier de sortie si nécessaire
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Générer le rapport
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("RAPPORT D'ANALYSE AUTOMATIQUE - PIPELINE ETL ObRail Europe\n")
        f.write("=" * 80 + "\n")
        f.write(f"Date de génération: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Source: {csv_path}\n")
        f.write("=" * 80 + "\n\n")
        
        # 1. VUE D'ENSEMBLE
        f.write("1) VUE D'ENSEMBLE\n")
        f.write("-" * 80 + "\n")
        f.write(f"   Total routes extraites        : {len(df)}\n")
        f.write(f"   Routes valides (avec distance): {df['distance_km'].notna().sum()}\n")
        f.write(f"   Routes invalides              : {df['distance_km'].isna().sum()}\n")
        
        # Sources
        if 'source' in df.columns:
            f.write(f"\n   Sources de données:\n")
            for source, count in df['source'].value_counts().items():
                f.write(f"      • {source:20s}: {count:4d} routes\n")
        
        f.write("\n")
        
        # 2. RÉPARTITION PAR TYPE DE VÉHICULE
        f.write("2) RÉPARTITION PAR TYPE DE VÉHICULE\n")
        f.write("-" * 80 + "\n")
        type_counts = df['vehicule_type'].value_counts()
        for vtype, count in type_counts.items():
            pct = (count / len(df)) * 100
            f.write(f"   • {vtype:35s}: {count:4d} trajets ({pct:5.1f}%)\n")
        f.write("\n")
        
        # 3. STATISTIQUES PAR TYPE DE VÉHICULE
        f.write("3) STATISTIQUES PAR TYPE DE VÉHICULE\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Type':<35} {'Count':>6} {'Dist.Moy (km)':>15} {'CO2 Moy (kg)':>15}\n")
        f.write("-" * 80 + "\n")
        
        grouped = df.groupby('vehicule_type').agg({
            'distance_km': ['count', 'mean', 'min', 'max'],
            'co2_kg': ['mean', 'min', 'max']
        }).round(2)
        
        for vtype in grouped.index:
            count = int(grouped.loc[vtype, ('distance_km', 'count')])
            dist_mean = grouped.loc[vtype, ('distance_km', 'mean')]
            co2_mean = grouped.loc[vtype, ('co2_kg', 'mean')]
            f.write(f"{vtype:<35} {count:>6} {dist_mean:>15.2f} {co2_mean:>15.2f}\n")
        
        f.write("\n")
        
        # 4. TOP 10 ROUTES LES PLUS LONGUES
        f.write("4) TOP 10 ROUTES LES PLUS LONGUES\n")
        f.write("-" * 80 + "\n")
        top_routes = df.nlargest(10, 'distance_km')[['origin', 'destination', 'vehicule_type', 'distance_km', 'co2_kg']]
        for idx, (i, row) in enumerate(top_routes.iterrows(), 1):
            f.write(f"   {idx:2d}. {row['origin'][:25]:25s} → {row['destination'][:25]:25s}\n")
            f.write(f"       {row['vehicule_type']:25s}  {row['distance_km']:7.1f} km  {row['co2_kg']:6.2f} kg CO2\n")
        f.write("\n")
        
        # 5. COMPARAISON ENVIRONNEMENTALE TRAIN VS AVION
        f.write("5) COMPARAISON ENVIRONNEMENTALE TRAIN vs AVION\n")
        f.write("-" * 80 + "\n")
        
        trains = df[df['vehicule_type'].str.contains('Train|Night|Euro|City', na=False)]
        avions = df[df['vehicule_type'] == 'Avion']
        
        if len(trains) > 0:
            f.write(f"   TRAINS ({len(trains)} routes)\n")
            f.write(f"      Distance moyenne    : {trains['distance_km'].mean():7.1f} km\n")
            f.write(f"      CO2 moyen           : {trains['co2_kg'].mean():7.2f} kg\n")
            f.write(f"      CO2 par km          : {(trains['co2_kg'].mean() / trains['distance_km'].mean()):7.4f} kg/km\n")
            f.write(f"      Distance min/max    : {trains['distance_km'].min():.1f} / {trains['distance_km'].max():.1f} km\n")
            f.write("\n")
        
        if len(avions) > 0:
            f.write(f"   AVIONS ({len(avions)} routes)\n")
            f.write(f"      Distance moyenne    : {avions['distance_km'].mean():7.1f} km\n")
            f.write(f"      CO2 moyen           : {avions['co2_kg'].mean():7.2f} kg\n")
            f.write(f"      CO2 par km          : {(avions['co2_kg'].mean() / avions['distance_km'].mean()):7.4f} kg/km\n")
            f.write(f"      Distance min/max    : {avions['distance_km'].min():.1f} / {avions['distance_km'].max():.1f} km\n")
            f.write("\n")
        
        if len(trains) > 0 and len(avions) > 0:
            facteur = (avions['co2_kg'].mean() / avions['distance_km'].mean()) / \
                     (trains['co2_kg'].mean() / trains['distance_km'].mean())
            f.write(f"   CONCLUSION: L'avion émet {facteur:.1f}x plus de CO2 par km que le train\n")
            f.write(f"   Économie moyenne en prenant le train: {avions['co2_kg'].mean() - trains['co2_kg'].mean():.2f} kg CO2\n")
        
        f.write("\n")
        
        # 6. CLASSIFICATION DES TRAINS
        f.write("6) CLASSIFICATION DÉTAILLÉE DES TRAINS\n")
        f.write("-" * 80 + "\n")
        
        trains_only = df[df['vehicule_type'].str.contains('Train|Night|Euro|City', na=False)]
        if len(trains_only) > 0:
            train_types = trains_only['vehicule_type'].value_counts()
            for ttype, count in train_types.items():
                pct = (count / len(trains_only)) * 100
                f.write(f"   • {ttype:35s}: {count:4d} trajets ({pct:5.1f}% des trains)\n")
        
        f.write("\n")
        
        # 7. ANALYSE DES TRAINS DE NUIT
        f.write("7) ANALYSE DES TRAINS DE NUIT\n")
        f.write("-" * 80 + "\n")
        
        night_trains = df[df['vehicule_type'].str.contains('Nuit|Night', na=False)]
        if len(night_trains) > 0:
            f.write(f"   Total trains de nuit détectés : {len(night_trains)}\n")
            f.write(f"   Distance moyenne              : {night_trains['distance_km'].mean():.1f} km\n")
            f.write(f"   CO2 moyen                     : {night_trains['co2_kg'].mean():.2f} kg\n")
            f.write("\n")
            
            # Décomposition par type
            for ntype, count in night_trains['vehicule_type'].value_counts().items():
                f.write(f"      • {ntype:30s}: {count:3d} trajets\n")
        else:
            f.write("   Aucun train de nuit détecté dans les données.\n")
        
        f.write("\n")
        
        # 8. COUVERTURE GÉOGRAPHIQUE
        f.write("8) COUVERTURE GÉOGRAPHIQUE\n")
        f.write("-" * 80 + "\n")
        
        if 'origin' in df.columns and 'destination' in df.columns:
            unique_origins = df['origin'].nunique()
            unique_destinations = df['destination'].nunique()
            all_stations = pd.concat([df['origin'], df['destination']]).nunique()
            
            f.write(f"   Gares/Aéroports d'origine     : {unique_origins}\n")
            f.write(f"   Gares/Aéroports de destination: {unique_destinations}\n")
            f.write(f"   Total unique                  : {all_stations}\n")
            
            # Top 10 gares les plus connectées
            all_points = pd.concat([df['origin'], df['destination']])
            top_stations = all_points.value_counts().head(10)
            f.write(f"\n   Top 10 gares/aéroports les plus connectés:\n")
            for i, (station, count) in enumerate(top_stations.items(), 1):
                f.write(f"      {i:2d}. {station[:40]:40s}: {count:3d} connexions\n")
        
        f.write("\n")
        
        # 9. QUALITÉ DES DONNÉES
        f.write("9) QUALITÉ DES DONNÉES\n")
        f.write("-" * 80 + "\n")
        
        total_rows = len(df)
        f.write(f"   Total lignes                  : {total_rows}\n")
        f.write(f"   Données complètes (distance)  : {df['distance_km'].notna().sum()} ({(df['distance_km'].notna().sum()/total_rows*100):.1f}%)\n")
        f.write(f"   Données complètes (CO2)       : {df['co2_kg'].notna().sum()} ({(df['co2_kg'].notna().sum()/total_rows*100):.1f}%)\n")
        
        if 'departure_time' in df.columns:
            f.write(f"   Horaires de départ renseignés : {df['departure_time'].notna().sum()} ({(df['departure_time'].notna().sum()/total_rows*100):.1f}%)\n")
        if 'arrival_time' in df.columns:
            f.write(f"   Horaires d'arrivée renseignés : {df['arrival_time'].notna().sum()} ({(df['arrival_time'].notna().sum()/total_rows*100):.1f}%)\n")
        
        f.write("\n")
        
        # 10. FOOTER
        f.write("=" * 80 + "\n")
        f.write("10) FOOTER\n")
        f.write("=" * 80 + "\n")
        f.write(f"\nFichier de sortie: {output_path}\n")
        f.write(f"Généré par: analyse/analyse_resultat.py\n")
        f.write(f"Pipeline ETL: ObRail Europe - Comparatif Train vs Avion\n")
    
    print(f"Rapport d'analyse généré: {output_path}")
    return True


def run_analysis():
    """Point d'entrée principal pour l'analyse"""
    # Chemins configurables via environnement
    csv_path = os.environ.get("OUTPUT_DIR", "./data/staging") + "/" + \
               os.environ.get("OUTPUT_FINAL_FILE", "final_routes.csv")
    
    output_path = "./analyse/rapport_analyse.md"
    
    print("\n" + "=" * 60)
    print("📊 GÉNÉRATION DU RAPPORT D'ANALYSE")
    print("=" * 60)
    
    success = generate_analysis_report(csv_path, output_path)
    
    if success:
        print("\n🎉 Analyse terminée avec succès!")
        print(f"📄 Rapport disponible: {output_path}")
    else:
        print("\n❌ Erreur lors de la génération du rapport")
    
    return success


if __name__ == "__main__":
    run_analysis()
