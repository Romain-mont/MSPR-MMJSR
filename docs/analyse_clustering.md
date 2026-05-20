# Analyse — Clustering de corridors ferroviaires
## Algorithmes : K-means + DBSCAN (non supervisé)

---

## Contexte

**Enjeu :** Identifier des groupes naturels de corridors sans utiliser le label `is_substitutable`.  
**Validation :** Comparer les clusters obtenus avec le label après coup.  
**Dataset :** 2147 corridors (ceux ayant un vol existant — `co2_saved_kg` non NULL)

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
| 2 | 6580 | 0.629 |
| **3** | **5091** | **0.640** ← optimal |
| 4 | 3774 | 0.639 |
| 5 | 3232 | 0.373 |
| 6 | 2737 | 0.374 |

**k=3 sélectionné** par Silhouette Score maximal (0.640).

---

## DBSCAN

| Paramètre | Valeur |
|---|---|
| `eps` | 0.8 |
| `min_samples` | 10 |
| Clusters trouvés | **2** |
| Points bruit (-1) | **78 (3.6%)** |
| Silhouette (sans bruit) | **0.677** |

DBSCAN converge naturellement vers 2 clusters + 78 points atypiques — cohérent avec K-means qui trouve 2 groupes homogènes + 1 groupe de transition.

---

## Profil des 3 clusters K-means

| Cluster | Corridors | Distance moy | CO2 économisé moy | CO2 train moy | Ratio moy | % substituables |
|---|---|---|---|---|---|---|
| **0** | 1821 | 156 km | 85 kg | 1.93 kg | 35.2 | **100%** |
| **1** | 248 | 652 km | 138 kg | 28.8 kg | 1.6 | **41%** |
| **2** | 78 | 164 km | 89 kg | 1.31 kg | 36.1 | **100%** |

---

## Interprétation des clusters

### Cluster 0 — "Courts trajets substituables" (1821 corridors, 85%)
Distance moyenne 156 km, 100% substituables. Ce sont les trajets typiques de substitution évidente : trains régionaux français courts, CO2 train très faible (1.93 kg), fort ratio trafic/population (35.2 voyages/habitant/an). Ces corridors ne font aucun doute — le train est clairement la meilleure option.

### Cluster 1 — "Zone grise autour du seuil 600km" (248 corridors, 12%)
Distance moyenne 652 km, seulement 41% substituables. Ce sont exactement les corridors qui se situent autour du seuil légal de 600 km. CO2 train plus élevé (28.8 kg — trajets longs en pays à mix énergétique varié), ratio faible (1.6 — gares moins fréquentées). C'est la zone de décision la plus intéressante pour le modèle de classification.

### Cluster 2 — "Courts trajets haute fréquentation" (78 corridors, 4%)
Profil similaire au Cluster 0 (164 km, 100% substituables) mais séparé par les ratios trafic/population légèrement plus élevés (36.1) et un CO2 train encore plus bas (1.31 kg). Ce sous-groupe représente des corridors entre villes à forte culture ferroviaire — là où le train est déjà très utilisé et la substitution déjà en cours.

### Points bruit DBSCAN (78 corridors)
Les 78 corridors classés bruit par DBSCAN correspondent exactement au Cluster 2 de K-means — ce sont des corridors atypiques par leurs caractéristiques, probablement des trains de nuit sur courtes distances ou des corridors avec des données démographiques inhabituelles.

---

## Validation — Alignement clusters vs label `is_substitutable`

```
is_substitutable    0     1   Total
Cluster 0           0  1821    1821
Cluster 1         146   102     248
Cluster 2           0    78      78
Total             146  2001    2147
```

**Résultat remarquable :** Le clustering non-supervisé retrouve presque parfaitement le label sans le voir :
- Clusters 0 et 2 → **100% correspondance** avec `is_substitutable=1`
- Cluster 1 → zone d'incertitude avec 146 non-substituables et 102 substituables

Les seuls désaccords sont dans le Cluster 1 — précisément la zone autour du seuil 600km où la décision est la plus difficile.

---

## Comparaison K-means vs DBSCAN

| Critère | K-means (k=3) | DBSCAN |
|---|---|---|
| Clusters | 3 | 2 + bruit |
| Silhouette | 0.640 | 0.677 |
| Points non assignés | 0 | 78 (3.6%) |
| Interprétabilité | ✅ Profils clairs | ✅ Identifie les outliers |
| Robustesse | ✅ Stable | ⚠️ Sensible à eps |

**K-means préféré** pour l'interprétation métier (profils clairs par cluster).  
**DBSCAN complémentaire** pour identifier les corridors atypiques (les 78 outliers).

---

## Conclusion pour la soutenance

> "Sans utiliser le label `is_substitutable`, le clustering K-means retrouve naturellement les mêmes groupes avec une cohérence de 100% sur les clusters extrêmes. Ceci valide deux choses : la cohérence de nos données, et le fait que les features géographiques et démographiques suffisent à distinguer les corridors substituables des non-substituables. Le Cluster 1 (zone grise autour de 600km) représente précisément les cas sur lesquels la politique publique doit se concentrer."

---

## Fichiers générés

| Fichier | Contenu |
|---|---|
| `models/kmeans_corridors.joblib` | Modèle K-means entraîné (k=3) |
| `docs/fig_clustering_corridors.png` | Elbow, Silhouette, PCA 2D, Distance vs CO2 |
| `docs/corridors_clustered.csv` | Dataset avec labels de clusters |
| `docs/profil_clusters.csv` | Profil statistique par cluster |
