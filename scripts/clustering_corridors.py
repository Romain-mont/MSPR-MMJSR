"""
Clustering de corridors ferroviaires — non supervisé
Enjeu : identifier des groupes naturels de corridors sans utiliser le label is_substitutable
Algorithmes : K-means (k=2 à 6) + GMM (k=2 à 6) + DBSCAN
Évaluation  : Silhouette Score + Elbow Method + BIC (GMM)
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import joblib
import warnings
warnings.filterwarnings('ignore')

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, DBSCAN
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score, davies_bouldin_score
from sklearn.decomposition import PCA

RANDOM_STATE = 42
DOCS_DIR     = os.path.join(os.path.dirname(__file__), '..', 'docs')
MODELS_DIR   = os.path.join(os.path.dirname(__file__), '..', 'models')
os.makedirs(DOCS_DIR,   exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

# ── Chargement ──────────────────────────────────────────────────────────────
df = pd.read_csv(os.path.join(os.path.dirname(__file__), '..', 'donnee',
                              'staging_fact_route_analysis.csv'))
df = df.dropna(subset=['co2_train_kg', 'co2_saved_kg'])
df['co2_avion_kg']  = df['co2_avion_kg'].fillna(0)
df['ratio_origin']  = (df['origin_station_traffic'] / df['origin_city_population'].replace(0, np.nan)).fillna(0)
df['ratio_dest']    = (df['dest_station_traffic']   / df['dest_city_population'].replace(0, np.nan)).fillna(0)

# Features de clustering — on N'INCLUT PAS is_substitutable (validé après)
FEATURES = ['distance_km', 'co2_saved_kg', 'co2_train_kg',
            'ratio_origin', 'ratio_dest']

X = df[FEATURES].fillna(0)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print(f"Dataset : {len(X)} corridors | Features : {FEATURES}")

# ── K-means : Elbow + Silhouette ────────────────────────────────────────────
print("\n=== K-means — Recherche du k optimal ===")
k_range   = range(2, 7)
inertias  = []
silhouettes = []

for k in k_range:
    km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
    labels = km.fit_predict(X_scaled)
    inertias.append(km.inertia_)
    sil = silhouette_score(X_scaled, labels)
    silhouettes.append(sil)
    print(f"  k={k}  Inertie={km.inertia_:.0f}  Silhouette={sil:.3f}")

best_k = k_range.start + silhouettes.index(max(silhouettes))
print(f"\n→ Meilleur k selon Silhouette : k={best_k} (score={max(silhouettes):.3f})")

# ── K-means final ───────────────────────────────────────────────────────────
km_final = KMeans(n_clusters=best_k, random_state=RANDOM_STATE, n_init=10)
df['cluster_kmeans'] = km_final.fit_predict(X_scaled)
joblib.dump(km_final, os.path.join(MODELS_DIR, 'kmeans_corridors.joblib'))

# ── GMM : BIC + Silhouette ───────────────────────────────────────────────────
print("\n=== GMM (Gaussian Mixture Model) — Recherche du k optimal ===")
bics, gmm_silhouettes = [], []
for k in k_range:
    gmm = GaussianMixture(n_components=k, random_state=RANDOM_STATE, n_init=5)
    gmm_labels = gmm.fit_predict(X_scaled)
    bics.append(gmm.bic(X_scaled))
    sil = silhouette_score(X_scaled, gmm_labels)
    gmm_silhouettes.append(sil)
    print(f"  k={k}  BIC={gmm.bic(X_scaled):.0f}  Silhouette={sil:.3f}")

best_k_gmm = k_range.start + gmm_silhouettes.index(max(gmm_silhouettes))
print(f"\n→ Meilleur k GMM selon Silhouette : k={best_k_gmm} (score={max(gmm_silhouettes):.3f})")

gmm_final = GaussianMixture(n_components=best_k_gmm, random_state=RANDOM_STATE, n_init=5)
df['cluster_gmm'] = gmm_final.fit_predict(X_scaled)

# ── DBSCAN ──────────────────────────────────────────────────────────────────
print("\n=== DBSCAN ===")
db = DBSCAN(eps=0.8, min_samples=10)
df['cluster_dbscan'] = db.fit_predict(X_scaled)
n_clusters_db = len(set(df['cluster_dbscan'])) - (1 if -1 in df['cluster_dbscan'].values else 0)
n_noise       = (df['cluster_dbscan'] == -1).sum()
print(f"  Clusters trouvés : {n_clusters_db}")
print(f"  Points bruit (-1) : {n_noise} ({n_noise/len(df)*100:.1f}%)")
if n_clusters_db > 1:
    sil_db = silhouette_score(X_scaled[df['cluster_dbscan'] != -1],
                               df['cluster_dbscan'][df['cluster_dbscan'] != -1])
    print(f"  Silhouette (sans bruit) : {sil_db:.3f}")

# ── Comparaison des méthodes ─────────────────────────────────────────────────
print("\n=== Comparaison K-Means vs GMM vs DBSCAN ===")
sil_km  = silhouette_score(X_scaled, df['cluster_kmeans'])
sil_gmm = silhouette_score(X_scaled, df['cluster_gmm'])
print(f"  K-Means  k={best_k}     Silhouette={sil_km:.3f}  (retenu)")
print(f"  GMM      k={best_k_gmm}     Silhouette={sil_gmm:.3f}")
if n_clusters_db > 1:
    print(f"  DBSCAN   k={n_clusters_db}    Silhouette={sil_db:.3f}  ({n_noise} points bruit)")
else:
    print(f"  DBSCAN   → 1 cluster (dégénéré, non utilisable)")

# ── Profil des clusters K-means ─────────────────────────────────────────────
print(f"\n=== Profil des {best_k} clusters K-means ===")
profile = df.groupby('cluster_kmeans').agg(
    nb=('distance_km', 'count'),
    dist_moy=('distance_km', 'mean'),
    co2_saved_moy=('co2_saved_kg', 'mean'),
    co2_train_moy=('co2_train_kg', 'mean'),
    ratio_origin_moy=('ratio_origin', 'mean'),
    pct_substituable=('is_substitutable', 'mean'),
).round(2)
profile['pct_substituable'] = (profile['pct_substituable'] * 100).round(1)
print(profile.to_string())

# Nommage des clusters selon profil
def name_cluster(row):
    if row['pct_substituable'] > 85 and row['dist_moy'] < 250:
        return "Substitution évidente"
    elif row['pct_substituable'] > 50:
        return "Zone intermédiaire"
    else:
        return "Long haul / Non-substituable"

profile['profil'] = profile.apply(name_cluster, axis=1)
print("\nNoms des clusters :")
for idx, row in profile.iterrows():
    print(f"  Cluster {idx} : {row['profil']} ({row['nb']} corridors, "
          f"dist_moy={row['dist_moy']:.0f}km, {row['pct_substituable']}% substituables)")

# ── Visualisations ──────────────────────────────────────────────────────────
# PCA 2D pour visualisation
pca = PCA(n_components=2, random_state=RANDOM_STATE)
X_pca = pca.fit_transform(X_scaled)
print(f"\nPCA variance expliquée : {pca.explained_variance_ratio_.sum()*100:.1f}%")

colors = cm.Set1(np.linspace(0, 0.8, best_k))
colors_gmm = cm.Set2(np.linspace(0, 0.8, best_k_gmm))

fig, axes = plt.subplots(2, 3, figsize=(20, 12))

# 1. Elbow curve K-Means
axes[0,0].plot(list(k_range), inertias, 'bo-', linewidth=2, markersize=8)
axes[0,0].set_xlabel('Nombre de clusters k')
axes[0,0].set_ylabel('Inertie')
axes[0,0].set_title('K-Means — Elbow Method')
axes[0,0].axvline(best_k, color='red', linestyle='--', label=f'k optimal={best_k}')
axes[0,0].legend()

# 2. Silhouette K-Means vs GMM
x = list(k_range)
w = 0.35
axes[0,1].bar([v - w/2 for v in x], silhouettes,     width=w, label='K-Means',
              color=['red' if k == best_k else '#3498db' for k in k_range])
axes[0,1].bar([v + w/2 for v in x], gmm_silhouettes, width=w, label='GMM',
              color=['tomato' if k == best_k_gmm else '#e67e22' for k in k_range])
axes[0,1].set_xlabel('Nombre de clusters k')
axes[0,1].set_ylabel('Silhouette Score')
axes[0,1].set_title('Silhouette : K-Means vs GMM')
axes[0,1].legend()

# 3. BIC GMM
axes[0,2].plot(list(k_range), bics, 'gs-', linewidth=2, markersize=8)
axes[0,2].set_xlabel('Nombre de clusters k')
axes[0,2].set_ylabel('BIC (plus bas = mieux)')
axes[0,2].set_title('GMM — BIC Score')
axes[0,2].axvline(best_k_gmm, color='red', linestyle='--', label=f'k optimal={best_k_gmm}')
axes[0,2].legend()

# 4. PCA — K-means
for i, (c, color) in enumerate(zip(range(best_k), colors)):
    mask = df['cluster_kmeans'] == c
    label = profile.loc[c, 'profil'] if c in profile.index else f'Cluster {c}'
    axes[1,0].scatter(X_pca[mask, 0], X_pca[mask, 1],
                      c=[color], label=f"C{c}: {label}", alpha=0.5, s=15)
axes[1,0].set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)')
axes[1,0].set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)')
axes[1,0].set_title(f'K-Means k={best_k} (PCA 2D) — Silhouette={sil_km:.3f}')
axes[1,0].legend(fontsize=7)

# 5. PCA — GMM
for i, (c, color) in enumerate(zip(range(best_k_gmm), colors_gmm)):
    mask = df['cluster_gmm'] == c
    axes[1,1].scatter(X_pca[mask, 0], X_pca[mask, 1],
                      c=[color], label=f"C{c}", alpha=0.5, s=15)
axes[1,1].set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)')
axes[1,1].set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)')
axes[1,1].set_title(f'GMM k={best_k_gmm} (PCA 2D) — Silhouette={sil_gmm:.3f}')
axes[1,1].legend(fontsize=7)

# 6. Distance vs CO2 saved coloré par cluster K-Means
for i, (c, color) in enumerate(zip(range(best_k), colors)):
    mask = df['cluster_kmeans'] == c
    label = profile.loc[c, 'profil'] if c in profile.index else f'Cluster {c}'
    axes[1,2].scatter(df.loc[mask, 'distance_km'], df.loc[mask, 'co2_saved_kg'],
                      c=[color], label=f"C{c}", alpha=0.5, s=15)
axes[1,2].axvline(600, color='black', linestyle='--', linewidth=1.5, label='Seuil 600km')
axes[1,2].set_xlabel('Distance (km)')
axes[1,2].set_ylabel('CO2 économisé (kg)')
axes[1,2].set_title('Distance vs CO2 économisé (K-Means)')
axes[1,2].legend(fontsize=8)

plt.suptitle(f'Clustering de corridors — K-Means (k={best_k}) vs GMM (k={best_k_gmm}) vs DBSCAN',
             fontsize=13)
plt.tight_layout()
plt.savefig(os.path.join(DOCS_DIR, 'fig_clustering_corridors.png'), dpi=150, bbox_inches='tight')
plt.close()
print(f"\n✅ Graphiques sauvegardés : docs/fig_clustering_corridors.png")

# ── Comparaison K-means vs is_substitutable ─────────────────────────────────
print("\n=== Alignement clusters vs label is_substitutable ===")
print(pd.crosstab(df['cluster_kmeans'], df['is_substitutable'],
                  rownames=['Cluster'], colnames=['is_substitutable'],
                  margins=True).to_string())

# ── Sauvegarde résultats ────────────────────────────────────────────────────
df[['origin','destination','distance_km','co2_saved_kg','is_substitutable',
    'cluster_kmeans','cluster_dbscan']].to_csv(
    os.path.join(DOCS_DIR, 'corridors_clustered.csv'), index=False)
profile.to_csv(os.path.join(DOCS_DIR, 'profil_clusters.csv'))
print(f"✅ Résultats : docs/corridors_clustered.csv | docs/profil_clusters.csv")
print(f"✅ Modèle    : models/kmeans_corridors.joblib")
