"""
Dashboard de visualisation du datamart CO2.
Génère des graphiques pour analyser les émissions carbone des trajets.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from dotenv import load_dotenv
import os
import shutil
from pathlib import Path

# Configuration
load_dotenv()

# Configuration de l'API
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

def load_data():
    """Charge les données via l'API REST."""
    try:
        print(f"Connexion à l'API: {API_URL}/data")
        response = requests.get(f"{API_URL}/data", timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print("Aucune donnée retournée par l'API")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        print(f"{len(df)} lignes chargées depuis l'API")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Erreur de connexion à l'API: {e}")
        print(f"URL utilisée: {API_URL}/data")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erreur lors du chargement des données: {e}")
        return pd.DataFrame()

def plot_distance_distribution(df):
    """Distribution des distances."""
    fig, ax = plt.subplots()
    sns.histplot(df['distance_km'], bins=50, kde=True, color='steelblue', ax=ax)
    ax.set_xlabel('Distance (km)')
    ax.set_ylabel('Nombre de trajets')
    ax.set_title('Distribution des distances de trajets')
    ax.axvline(df['distance_km'].mean(), color='red', linestyle='--', label=f"Moyenne: {df['distance_km'].mean():.0f} km")
    ax.legend()
    plt.tight_layout()
    return fig

def plot_co2_distribution(df):
    """Distribution des émissions CO2."""
    fig, ax = plt.subplots()
    sns.histplot(df['co2_kg'], bins=50, kde=True, color='green', ax=ax)
    ax.set_xlabel('CO2 émis (kg)')
    ax.set_ylabel('Nombre de trajets')
    ax.set_title('Distribution des émissions CO2 par trajet')
    ax.axvline(df['co2_kg'].mean(), color='red', linestyle='--', label=f"Moyenne: {df['co2_kg'].mean():.2f} kg")
    ax.legend()
    plt.tight_layout()
    return fig

def plot_distance_vs_co2(df):
    """Relation distance vs CO2."""
    fig, ax = plt.subplots()
    sns.scatterplot(data=df, x='distance_km', y='co2_kg', hue='vehicule_type', alpha=0.6, ax=ax)
    ax.set_xlabel('Distance (km)')
    ax.set_ylabel('CO2 émis (kg)')
    ax.set_title('Émissions CO2 en fonction de la distance')
    plt.tight_layout()
    return fig

def plot_top_routes(df, n=15):
    """Top N routes les plus longues."""
    df['route'] = df['origine'] + ' → ' + df['destination']
    top = df.nlargest(n, 'distance_km')[['route', 'distance_km', 'co2_kg']]
    
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(top['route'], top['distance_km'], color='steelblue')
    ax.set_xlabel('Distance (km)')
    ax.set_title(f'Top {n} des trajets les plus longs')
    ax.invert_yaxis()
    
    # Ajouter les valeurs CO2
    for i, (d, co2) in enumerate(zip(top['distance_km'], top['co2_kg'])):
        ax.text(d + 10, i, f'{co2:.1f} kg CO2', va='center', fontsize=8)
    
    plt.tight_layout()
    return fig

def plot_co2_by_vehicle(df):
    """CO2 par type de véhicule."""
    stats = df.groupby('vehicule_type').agg({
        'co2_kg': ['mean', 'sum', 'count'],
        'distance_km': 'mean'
    }).round(2)
    stats.columns = ['CO2 moyen (kg)', 'CO2 total (kg)', 'Nb trajets', 'Distance moy (km)']
    stats = stats.reset_index()
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # CO2 moyen par véhicule
    sns.barplot(data=stats, x='vehicule_type', y='CO2 moyen (kg)', palette='Greens_d', ax=axes[0])
    axes[0].set_title('CO2 moyen par type de véhicule')
    axes[0].set_xlabel('')
    
    # Nombre de trajets par véhicule
    sns.barplot(data=stats, x='vehicule_type', y='Nb trajets', palette='Blues_d', ax=axes[1])
    axes[1].set_title('Nombre de trajets par type de véhicule')
    axes[1].set_xlabel('')
    
    plt.tight_layout()
    return fig

def plot_distance_categories(df):
    """Répartition courte/longue distance."""
    categories = df['is_long_distance'].map({True: 'Longue distance (>500km)', False: 'Courte distance (≤500km)'})
    counts = categories.value_counts()
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    # Pie chart
    colors = ['#ff9999', '#66b3ff']
    axes[0].pie(counts, labels=counts.index, autopct='%1.1f%%', colors=colors, startangle=90)
    axes[0].set_title('Répartition des trajets')
    
    # Box plot CO2 par catégorie
    df['categorie'] = categories
    sns.boxplot(data=df, x='categorie', y='co2_kg', palette=['#66b3ff', '#ff9999'], ax=axes[1])
    axes[1].set_title('Émissions CO2 par catégorie de distance')
    axes[1].set_xlabel('')
    axes[1].set_ylabel('CO2 (kg)')
    
    plt.tight_layout()
    return fig

def generate_summary(df):
    """Génère un résumé statistique."""
    print("RÉSUMÉ DU DATAMART CO2")
    print(f"Statistiques générales:")
    print(f"Nombre total de trajets : {len(df):,}")
    print(f"Distance totale : {df['distance_km'].sum():,.0f} km")
    print(f"CO2 total émis : {df['co2_kg'].sum():,.2f} kg")
    
    print(f"Distances:")
    print(f"Moyenne : {df['distance_km'].mean():.1f} km")
    print(f"Médiane : {df['distance_km'].median():.1f} km")
    print(f"Min : {df['distance_km'].min():.1f} km")
    print(f"Max : {df['distance_km'].max():.1f} km")
    
    print(f"Émissions CO2:")
    print(f"Moyenne : {df['co2_kg'].mean():.3f} kg/trajet")
    print(f"Médiane : {df['co2_kg'].median():.3f} kg/trajet")
    print(f"Min : {df['co2_kg'].min():.3f} kg")
    print(f"Max : {df['co2_kg'].max():.3f} kg")
    
    print(f"Types de véhicules:")
    for vtype in df['vehicule_type'].unique():
        subset = df[df['vehicule_type'] == vtype]
        print(f"{vtype}: {len(subset)} trajets, CO2 moy={subset['co2_kg'].mean():.3f} kg")

def main():
    """Fonction principale."""
    print("GÉNÉRATION DU DASHBOARD DE VISUALISATION")
    
    print("Chargement des données via API REST...")
    df = load_data()
    
    if df.empty:
        print("Aucune donnée disponible (API inaccessible ou vide)")
        return False
    
    generate_summary(df)
    
    print("Génération des graphiques...")
    
    # Créer le dossier output (nettoyage si existe)
    output_dir = Path("visualization/output")
    
    # Supprimer l'ancien dossier s'il existe
    if output_dir.exists():
        print(f"Suppression des anciennes images dans {output_dir}/")
        shutil.rmtree(output_dir)
    
    # Recréer le dossier vide
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Générer et sauvegarder les graphiques
    figs = {
        '1_distribution_distances': plot_distance_distribution(df),
        '2_distribution_co2': plot_co2_distribution(df),
        '3_distance_vs_co2': plot_distance_vs_co2(df),
        '4_top_routes': plot_top_routes(df),
        '5_co2_par_vehicule': plot_co2_by_vehicle(df),
        '6_categories_distance': plot_distance_categories(df),
    }
    
    for name, fig in figs.items():
        path = output_dir / f"{name}.png"
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f"{path}")
        plt.close(fig)
    
    print(f"{len(figs)} graphiques générés dans {output_dir}/")
    return True

if __name__ == "__main__":
    main()
