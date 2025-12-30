import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def load_and_explore_data():
    """
    Charge et explore les données du fichier CSV Back-on-Track
    """
    # Chargement des données
    df = pd.read_csv('../../data/processed/clean_backontrack_final.csv')
    
    print("=== EXPLORATION DES DONNÉES BACK-ON-TRACK ===\n")
    
    # Vérification des colonnes disponibles
    print("Colonnes disponibles :")
    print(df.columns.tolist())
    
    # Informations générales
    print(f"\nNombre total de trains : {len(df)}")
    print(f"Nombre de colonnes : {len(df.columns)}")
    
    # Analyse des opérateurs (utilisation du bon nom de colonne)
    print(f"\nNombre d'opérateurs uniques : {df['agency_name'].nunique()}")
    print("\nTop 10 des opérateurs :")
    print(df['agency_name'].value_counts().head(10))
    
    # Analyse des pays
    print(f"\nNombres de pays de départ uniques : {df['origin_country'].nunique()}")
    print(f"Nombres de pays d'arrivée uniques : {df['destination_country'].nunique()}")
    
    # Analyse des durées
    if 'duration_h' in df.columns:
        df['duration_timedelta'] = pd.to_timedelta(df['duration_h'], errors='coerce')
        print(f"\nDurée moyenne des trajets : {df['duration_timedelta'].mean()}")
        print(f"Durée minimale : {df['duration_timedelta'].min()}")
        print(f"Durée maximale : {df['duration_timedelta'].max()}")
    
    return df

def analyze_international_vs_national(df):
    """
    Analyse la répartition entre trains internationaux et nationaux
    """
    print("\n=== ANALYSE INTERNATIONAL vs NATIONAL ===\n")
    
    scope_counts = df['service_type'].value_counts()
    print("Répartition par portée :")
    print(scope_counts)
    print(f"\nPourcentage international : {(scope_counts.get('International', 0) / len(df)) * 100:.1f}%")
    print(f"Pourcentage national : {(scope_counts.get('National', 0) / len(df)) * 100:.1f}%")
    
    return scope_counts

def analyze_by_country(df):
    """
    Analyse la distribution par pays
    """
    print("\n=== ANALYSE PAR PAYS ===\n")
    
    # Pays les plus représentés en départ
    print("Top 10 pays de départ :")
    departure_countries = df['origin_country'].value_counts().head(10)
    print(departure_countries)
    
    print("\nTop 10 pays d'arrivée :")
    arrival_countries = df['destination_country'].value_counts().head(10)
    print(arrival_countries)
    
    # Analyse des liaisons internationales
    international_trains = df[df['service_type'] == 'International']
    if len(international_trains) > 0:
        print(f"\nNombre de liaisons internationales : {len(international_trains)}")
        print("\nTop 10 liaisons internationales :")
        international_routes = international_trains.groupby(['origin_country', 'destination_country']).size().sort_values(ascending=False).head(10)
        print(international_routes)
    
    return departure_countries, arrival_countries

def analyze_operators(df):
    """
    Analyse les opérateurs ferroviaires
    """
    print("\n=== ANALYSE DES OPÉRATEURS ===\n")
    
    # Top opérateurs
    top_operators = df['agency_name'].value_counts().head(15)
    print("Top 15 opérateurs :")
    print(top_operators)
    
    # Opérateurs par pays
    operator_analysis = []
    for operator in top_operators.index[:10]:
        operator_trains = df[df['agency_name'] == operator]
        countries = list(set(operator_trains['origin_country'].tolist() + operator_trains['destination_country'].tolist()))
        operator_analysis.append({
            'Opérateur': operator,
            'Nb_trains': len(operator_trains),
            'Pays_desservis': countries,
            'Nb_pays': len(countries)
        })
    
    print("\nAnalyse détaillée des principaux opérateurs :")
    for op in operator_analysis:
        print(f"{op['Opérateur']} : {op['Nb_trains']} trains, {op['Nb_pays']} pays")
    
    return top_operators

def analyze_train_types(df):
    """
    Analyse les types de trains
    """
    print("\n=== ANALYSE DES TYPES DE TRAINS ===\n")
    
    train_types = df['train_type'].value_counts()
    print("Répartition par type de train :")
    print(train_types)
    
    # Analyse des émissions par type
    if 'emission_gco2e_pkm' in df.columns:
        print("\nÉmissions moyennes par type de train (gCO2e/pkm) :")
        emissions_by_type = df.groupby('train_type')['emission_gco2e_pkm'].mean().sort_values(ascending=False)
        print(emissions_by_type)
    
    return train_types

def create_visualizations(df):
    """
    Crée des visualisations des données
    """
    print("\n=== CRÉATION DES VISUALISATIONS ===\n")
    
    # Configuration
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # 1. Répartition International vs National
    scope_counts = df['service_type'].value_counts()
    axes[0, 0].pie(scope_counts.values, labels=scope_counts.index, autopct='%1.1f%%')
    axes[0, 0].set_title('Répartition International vs National')
    
    # 2. Top 10 opérateurs
    top_operators = df['agency_name'].value_counts().head(10)
    axes[0, 1].barh(range(len(top_operators)), top_operators.values)
    axes[0, 1].set_yticks(range(len(top_operators)))
    axes[0, 1].set_yticklabels([op[:25] + '...' if len(op) > 25 else op for op in top_operators.index])
    axes[0, 1].set_title('Top 10 Opérateurs')
    axes[0, 1].set_xlabel('Nombre de trains')
    
    # 3. Top pays de départ
    top_departure = df['origin_country'].value_counts().head(10)
    axes[1, 0].bar(range(len(top_departure)), top_departure.values)
    axes[1, 0].set_xticks(range(len(top_departure)))
    axes[1, 0].set_xticklabels(top_departure.index, rotation=45)
    axes[1, 0].set_title('Top 10 Pays de Départ')
    axes[1, 0].set_ylabel('Nombre de trains')
    
    # 4. Types de trains
    train_types = df['train_type'].value_counts()
    axes[1, 1].pie(train_types.values, labels=train_types.index, autopct='%1.1f%%')
    axes[1, 1].set_title('Répartition par Type de Train')
    
    plt.tight_layout()
    plt.savefig('back_on_track_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("Graphiques sauvegardés dans 'back_on_track_analysis.png'")

def generate_summary_report(df):
    """
    Génère un rapport de synthèse
    """
    print("\n=== RAPPORT DE SYNTHÈSE ===\n")
    
    total_trains = len(df)
    international_pct = (df['service_type'] == 'International').sum() / total_trains * 100
    national_pct = (df['service_type'] == 'National').sum() / total_trains * 100
    
    report = f"""
    RAPPORT D'ANALYSE - BACK-ON-TRACK
    =================================
    
    Données générales :
    - Nombre total de services ferroviaires : {total_trains}
    - Services internationaux : {international_pct:.1f}%
    - Services nationaux : {national_pct:.1f}%
    
    Géographie :
    - Nombre de pays desservis (départ) : {df['origin_country'].nunique()}
    - Nombre de pays desservis (arrivée) : {df['destination_country'].nunique()}
    - Principal pays de départ : {df['origin_country'].mode().iloc[0]}
    - Principal pays d'arrivée : {df['destination_country'].mode().iloc[0]}
    
    Opérateurs :
    - Nombre d'opérateurs : {df['agency_name'].nunique()}
    - Principal opérateur : {df['agency_name'].mode().iloc[0]}
    
    Types de trains :
    - Types disponibles : {df['train_type'].nunique()}
    - Type principal : {df['train_type'].mode().iloc[0]}
    
    Observations :
    - Le réseau Back-on-Track couvre une large zone géographique européenne
    - Les trains de nuit restent une solution viable pour les longues distances
    - La diversité des opérateurs montre une collaboration européenne
    """
    
    print(report)
    
    # Sauvegarde du rapport
    with open('back_on_track_report.txt', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("Rapport sauvegardé dans 'back_on_track_report.txt'")

def main():
    """
    Fonction principale d'analyse
    """
    print("ANALYSE DES DONNÉES BACK-ON-TRACK")
    print("=" * 50)
    
    try:
        # Chargement et exploration
        df = load_and_explore_data()
        
        # Analyses spécifiques
        analyze_international_vs_national(df)
        analyze_by_country(df)
        analyze_operators(df)
        analyze_train_types(df)
        
        # Visualisations
        create_visualizations(df)
        
        # Rapport final
        generate_summary_report(df)
        
        print("\n=== ANALYSE TERMINÉE ===")
        print("Fichiers générés :")
        print("- back_on_track_analysis.png (graphiques)")
        print("- back_on_track_report.txt (rapport)")
        
    except FileNotFoundError:
        print("ERREUR : Fichier 'clean_backontrack_final.csv' non trouvé.")
        print("Veuillez vérifier le chemin du fichier.")
    except Exception as e:
        print(f"ERREUR : {str(e)}")

if __name__ == "__main__":
    main()