# Analyse — Clustering de corridors ferroviaires
## Algorithmes : K-means + DBSCAN (non supervisé)

---

## Contexte

**Enjeu :** Identifier des groupes naturels de corridors sans utiliser le label `is_substitutable`.  
**Validation :** Comparer les clusters obtenus avec le label après coup.  
**Dataset :** 43 782 corridors avec vol existant (`co2_saved_kg` non NULL) — sur 46 106 corridors français total

**Features utilisées :**

| Feature | Justification |
|---|---|
| `distance_km` | Déterminant principal de la substituabilité |
| `co2_saved_kg` | Bénéfice environnemental du corridor |
| `co2_train_kg` | Profil énergétique du train |
| `ratio_origin` | Intensité d'usage ferroviaire côté départ |
| `ratio_dest` | Intensité d'usage ferroviaire côté arrivée |

> `is_substitutable` est volontairement **exclu** des features — il sera utilisé uniquement pour valider les clusters après coup.

---

## K-means — Recherche du k optimal

| k | Inertie | Silhouette Score |
|---|---|---|
| 2 | 152 943 | 0.636 |
| 3 | 118 648 | 0.645 |
| **4** | **86 118** | **0.652** ← optimal |
| 5 | 66 342 | 0.647 |
| 6 | 54 015 | 0.354 |

**k=4 sélectionné** par Silhouette Score maximal (0.652).

---

## DBSCAN

| Paramètre | Valeur |
|---|---|
| `eps` | 0.8 |
| `min_samples` | 10 |
| Clusters trouvés | **13** |
| Points bruit (-1) | **140 (0.3%)** |
| Silhouette (sans bruit) | **0.366** |

DBSCAN détecte 13 micro-clusters sur 43k corridors — signe que les données sont plus riches et hétérogènes qu'à 2 147 corridors. K-means reste préféré pour l'interprétation métier.

---

## Profil des 4 clusters K-means

| Cluster | Corridors | Distance moy | CO2 économisé moy | CO2 train moy | Ratio moy | % substituables |
|---|---|---|---|---|---|---|
| **0** | 4 852 | 604 km | 148 kg | 11.6 kg | 40.9 | **48%** |
| **1** | 38 775 | 167 km | 86 kg | 3.14 kg | 28.3 | **100%** |
| **2** | 84 | 163 km | 86 kg | 2.44 kg | 56.7 | **100%** |
| **3** | 71 | 150 km | 83 kg | 2.36 kg | 3 849 | **100%** |

---

## Interprétation des clusters

### Cluster 0 — "Zone grise autour du seuil 600km" (4 852 corridors, 11%)
Distance moyenne 604 km — pile autour du seuil légal de 600 km. Seulement 48% substituables : 2 514 non-substituables et 2 338 substituables coexistent dans ce groupe. CO2 train plus élevé (11.6 kg — trajets longs), ratio modéré (40.9). C'est **la zone de décision critique** pour le modèle de classification : c'est là où l'IA apporte le plus de valeur par rapport à une règle simple "< 600km".

### Cluster 1 — "Substitution évidente — grande majorité" (38 775 corridors, 89%)
L'essentiel du dataset : trajets courts (167 km en moyenne), 100% substituables, CO2 train très faible (3.14 kg), forte fréquentation SNCF. Ces corridors ne font aucun doute — le train est clairement la meilleure option. Ils représentent le cœur du réseau ferroviaire français.

### Cluster 2 — "Haute fréquentation ferroviaire" (84 corridors, < 1%)
Profil très proche du Cluster 1 (163 km, 100% substituables) mais avec un ratio trafic/population significativement plus élevé (56.7 vs 28.3). Ce sous-groupe représente des corridors entre agglomérations à très forte culture ferroviaire — là où le train est déjà dominant et la substitution déjà en cours.

### Cluster 3 — "Hubs ferroviaires majeurs" (71 corridors, < 1%)
Profil exceptionnel : ratio_origin moyen de **3 849** (vs 28-57 pour les autres clusters) — ce sont des gares à fréquentation extraordinaire comme Paris Gare du Nord, Paris Gare de Lyon, Lyon Part-Dieu. 100% substituables, distances courtes (150 km). Ces corridors sont des arêtes du réseau grande vitesse au départ des plus grands hubs français.

---

## Validation — Alignement clusters vs label `is_substitutable`

```
is_substitutable     0      1    Total
Cluster                             
0                 2514   2338    4852
1                    0  38775   38775
2                    0     84      84
3                    0     71      71
Total             2514  41268   43782
```

**Résultat remarquable :** Le clustering non-supervisé retrouve presque parfaitement le label sans le voir :
- Clusters 1, 2 et 3 → **100% correspondance** avec `is_substitutable=1`
- Cluster 0 → zone d'incertitude (48% substituables) — exactement les corridors autour du seuil 600 km

Les seuls désaccords (2 514 cas) sont tous dans le Cluster 0, précisément là où la règle des 600 km crée une frontière difficile à trancher sans le contexte complet.

---

## Comparaison K-means vs DBSCAN

| Critère | K-means (k=4) | DBSCAN |
|---|---|---|
| Clusters | 4 | 13 + bruit |
| Silhouette | 0.652 | 0.366 |
| Points non assignés | 0 | 140 (0.3%) |
| Interprétabilité | ✅ Profils clairs | ❌ 13 micro-clusters difficiles à nommer |
| Robustesse | ✅ Stable | ⚠️ Sensible à eps |

**K-means préféré** pour l'interprétation métier (4 profils clairs).  
**DBSCAN complémentaire** pour identifier les 140 corridors atypiques (outliers).

---

## Évolution par rapport à l'ancienne version (2 147 corridors)

| Métrique | Ancien (2 147) | Nouveau (43 782) | Évolution |
|---|---|---|---|
| k optimal | 3 | **4** | +1 cluster |
| Silhouette | 0.640 | **0.652** | +0.012 |
| Corridors "zone grise" | 248 (12%) | **4 852 (11%)** | proportion stable |
| Corridors évidents | 1 899 (88%) | **39 030 (89%)** | proportion stable |

La structure en clusters est **stable** malgré ×20 de données — validation de la robustesse du modèle.

---

## Conclusion pour la soutenance

> "Sans utiliser le label `is_substitutable`, le clustering K-means retrouve naturellement les mêmes groupes avec une cohérence de 100% sur les clusters extrêmes. Ceci valide deux choses : la cohérence de nos données sur 46 106 corridors français, et le fait que les features géographiques et démographiques suffisent à distinguer les corridors substituables des non-substituables. Le Cluster 0 (zone grise autour de 600 km, 4 852 corridors) représente précisément les cas sur lesquels la politique publique doit se concentrer. Le Cluster 3 révèle en outre les hubs ferroviaires majeurs comme Paris et Lyon — corridors à enjeu symbolique fort pour la transition."

---

## Fichiers générés

| Fichier | Contenu |
|---|---|
| `models/kmeans_corridors.joblib` | Modèle K-means entraîné (k=4, 43k corridors) |
| `docs/fig_clustering_corridors.png` | Elbow, Silhouette, PCA 2D, Distance vs CO2 |
| `docs/corridors_clustered.csv` | Dataset avec labels de clusters (43 782 lignes) |
| `docs/profil_clusters.csv` | Profil statistique par cluster |
