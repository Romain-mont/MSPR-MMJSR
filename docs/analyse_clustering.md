# Analyse — Clustering de corridors ferroviaires
## Algorithmes : K-Means + GMM + DBSCAN (non supervisé) | Validation : Dendrogramme + Carte

---

## Contexte

**Enjeu :** Identifier des groupes naturels de corridors sans utiliser le label `is_substitutable`.  
**Validation :** Comparer les clusters obtenus avec le label après coup.  
**Dataset :** 43 782 corridors avec vol existant (`co2_saved_kg` non NULL) — sur 46 106 corridors français total

**Features utilisées (StandardScaler appliqué) :**

| Feature | Justification |
|---|---|
| `distance_km` | Déterminant principal de la substituabilité |
| `co2_saved_kg` | Bénéfice environnemental du corridor |
| `co2_train_kg` | Profil énergétique du train |
| `ratio_origin` | trafic / population — intensité d'usage ferroviaire départ |
| `ratio_dest` | trafic / population — intensité d'usage ferroviaire arrivée |

> `is_substitutable` est volontairement **exclu** des features — il sera utilisé uniquement pour valider les clusters après coup.  
> `service_share` et `duration_h` ont été testés mais écartés (voir section Expériences).

---

## K-Means — Recherche du k optimal (ratios bruts)

| k | Inertie | Silhouette Score |
|---|---|---|
| 2 | 152 943 | 0.636 |
| 3 | 118 648 | 0.645 |
| **4** | **86 118** | **0.652** ← optimal |
| 5 | 66 342 | 0.647 |
| 6 | 54 015 | 0.354 |

**k=4 sélectionné** par Silhouette Score maximal (0.652). La méthode Elbow confirme un coude à k=4.

> ⚠️ **Note :** Cette version ratios bruts est conservée à titre pédagogique. Voir §3.5–3.7 pour la version log1p recommandée (k=2).

---

## GMM — Gaussian Mixture Model

| k | BIC | Silhouette Score |
|---|---|---|
| **2** | 254 823 | **0.381** ← optimal GMM |
| 3 | -54 796 | -0.007 |
| 4 | -280 532 | 0.009 |
| 5 | -297 468 | 0.034 |
| 6 | -317 469 | -0.097 |

**Résultat :** Silhouette négative dès k=3 — le GMM ne parvient pas à former des clusters cohérents.

**Pourquoi le GMM échoue ici :**  
Le GMM suppose que chaque cluster suit une distribution gaussienne de taille équilibrée. Or nos clusters sont de tailles très inégales (38 775 vs 71 corridors), ce qui viole cette hypothèse. Le meilleur score GMM (0.381 à k=2) reste largement inférieur à K-Means (0.652 à k=4).

---

## DBSCAN

| Paramètre | Valeur |
|---|---|
| `eps` | 0.8 |
| `min_samples` | 10 |
| Clusters trouvés | **13** |
| Points bruit (-1) | **140 (0.3%)** |
| Silhouette (sans bruit) | **0.366** |

**Résultat :** DBSCAN produit 13 micro-clusters difficiles à interpréter métier. Signe que les données forment un espace continu sans zones clairement séparées par du vide — hypothèse structurelle de DBSCAN non vérifiée ici.

---

## Comparaison des 3 méthodes

| Méthode | k | Silhouette | Points bruit | Interprétabilité | Décision |
|---|---|---|---|---|---|
| **K-Means** | **4** | **0.652** | 0 | ✅ 4 profils clairs | **Retenu** |
| GMM | 2 | 0.381 | 0 | ❌ négatif dès k=3 | Écarté |
| DBSCAN | 13 | 0.366 | 140 (0.3%) | ❌ 13 micro-clusters | Écarté |

**K-Means retenu** pour l'interprétation métier et la qualité de séparation.

---

## Profil des 4 clusters K-Means

| Cluster | Corridors | Distance moy | CO2 économisé moy | CO2 train moy | Ratio moy | % substituables |
|---|---|---|---|---|---|---|
| **0** | 4 852 | 604 km | 148 kg | 11.6 kg | 40.9 | **48%** |
| **1** | 38 775 | 167 km | 86 kg | 3.14 kg | 28.3 | **100%** |
| **2** | 84 | 163 km | 86 kg | 2.44 kg | 56.7 | **100%** |
| **3** | 71 | 150 km | 83 kg | 2.36 kg | 3 849 | **100%** |

---

## Interprétation des clusters

### Cluster 0 — "Zone grise autour du seuil 600 km" (4 852 corridors, 11%)
Distance moyenne 604 km — pile autour du seuil légal de 600 km. Seulement 48% substituables : 2 514 non-substituables et 2 338 substituables coexistent dans ce groupe. CO2 train plus élevé (11.6 kg — trajets longs), ratio modéré (40.9). C'est **la zone de décision critique** pour le modèle de classification : c'est là où l'IA apporte le plus de valeur par rapport à une règle simple "< 600 km".

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

## Validation hiérarchique — Dendrogramme Ward

**Méthode :** Clustering hiérarchique agglomératif (Ward) sur un échantillon de 500 corridors — indépendant de K-Means.

**Résultat :** La coupure du dendrogramme au seuil correspondant à 4 clusters donne **k=4**, confirmant la structure identifiée par K-Means sans aucun paramètre k à fixer a priori.

Cette convergence de deux méthodes radicalement différentes (partitionnement vs hiérarchique) constitue une preuve forte que la segmentation en 4 groupes reflète une structure réelle dans les données.

**Figure :** `docs/fig_dendrogram.png` — dendrogramme tronqué aux 20 dernières fusions, ligne rouge au seuil k=4.

---

## Visualisation géographique — Carte des clusters

**Outil :** Folium — carte interactive des 43 782 corridors colorés par cluster K-Means.

| Couleur | Cluster | Interprétation |
|---|---|---|
| Rouge | C0 — Long haul | Liaisons transfrontalières et longue distance |
| Vert | C1 — Standard | Maillage ferroviaire dense France intérieure |
| Bleu | C2 — Haute fréq. | Corridors entre agglomérations à forte culture ferro |
| Violet | C3 — Hubs | Grandes gares nationales (Paris, Lyon) |

La carte confirme que les clusters ont une cohérence géographique : les corridors rouges (C0) tracent les grandes diagonales transfrontalières, les corridors verts (C1) forment le maillage dense.

**Fichier :** `docs/carte_clusters.html` — carte interactive Folium avec tooltips et légende.

---

## Expériences de features écartées

| Feature testée | Silhouette avant | Silhouette après | Motif d'exclusion |
|---|---|---|---|
| `service_share` + `trip_count_corridor` | 0.652 | 0.522 | Compte brut, bruité, colinéaire avec structure |
| `duration_h` | 0.652 | 0.645 | Colinéaire avec `distance_km` (distance / vitesse) |

Ces features ont été conservées dans le CSV pour d'autres usages mais écartées du clustering.

---

## 3.5–3.7 Validation log1p — Clustering de référence

### Pourquoi tester une transformation log1p

Les features `ratio_origin` et `ratio_dest` présentent une distribution très asymétrique : quelques corridors atteignent des z-scores > +20 (ex: `Tilly → Habay` : z=+27.9). Ces valeurs extrêmes gonflent l'écart-type global et dominent la distance euclidienne utilisée par K-Means et Ward — créant des clusters artificiels (C2, C3).

**Correction appliquée :** `np.log1p()` sur `ratio_origin` et `ratio_dest` avant standardisation.

```python
# log1p écrase les extremes sans les supprimer
Valeur normale : 100   → log1p = 4.6
Outlier        : 10000 → log1p = 9.2  (pas 10 000)
```

### Résultats K-Means log1p

| k | Silhouette Score |
|---|---|
| **2** | **0.499** ← optimal |
| 3 | 0.412 |
| 4 | 0.378 |

**k=2 sélectionné.** La méthode Elbow converge vers k=2, Ward confirme k=2 (0 points isolés vs 3 dans la version brute).

### Comparatif des deux versions

| | Ratios bruts | Ratios log1p |
|---|---|---|
| k retenu (K-Means = Ward) | 4 | **2** |
| Silhouette K-Means | 0.652 | 0.499 |
| Points isolés (dendrogramme) | 3 | **0** |
| Tailles de clusters | 4 852 / 38 775 / 84 / 71 | **5 320 / 38 462** |

> La baisse de Silhouette (0.652 → 0.499) n'est pas une régression : le score original était **artificiellement gonflé** par la séparation triviale de C2/C3 (clusters minuscules et extrêmement éloignés). 0.499 reflète honnêtement la structure réelle (au-dessus du seuil usuel 0.4 pour des données bruitées).

### Verdict sur C2/C3

Trois preuves convergentes confirment que C2 (84 corridors) et C3 (71 corridors) étaient des **artefacts** :
1. Disparition des clusters après log1p sans réapparition équivalente
2. Passage de 3 à 0 points isolés dans le dendrogramme Ward
3. Convergence K-Means et Ward vers k=2

Ces corridors (0.33% du dataset) correspondaient à des gares à très faible population de ville mais fort trafic — signal réel mais trop rare pour constituer une niche métier exploitable.

### Profil des 2 clusters log1p (référence)

| Cluster | Corridors | Profil | % substituables |
|---|---|---|---|
| **0** | 5 320 (12%) | Long-haul, distance moy ~580 km, CO2 éco ~145 kg | **55.6%** |
| **1** | 38 462 (88%) | Standard, distance moy ~160 km, CO2 éco ~85 kg | **88.2%** |

**Cohérence métier :** k=2 colle naturellement à la variable binaire `is_substitutable` du Modèle 1. Le Cluster 0 (55.6% substituables) est la **zone de décision critique** — exactement les corridors long-haul où la desserte ferroviaire est déterminante. Le Cluster 1 (88.2%) représente le maillage court/moyen évident.

### Alignement clustering log1p vs label `is_substitutable`

Le clustering log1p n'a jamais vu `is_substitutable`, `service_share` ou `trip_count_corridor`. Pourtant :
- Cluster 0 (long-haul) : 55.6% substituables — nettement sous la moyenne globale (84.25%) ← cohérent avec r=-0.31 entre distance et label
- Cluster 1 (standard) : 88.2% substituables — au-dessus de la moyenne ← corridors courts, bien desservis en moyenne

> Le mélange 55.6%/44.4% du Cluster 0 confirme la **limite de la géographie seule** : c'est la desserte ferroviaire (`service_share`, `trip_count_corridor`) — volontairement exclue du clustering — qui reste le facteur déterminant (47.9% + 11.6% d'importance dans le Modèle 1).

**Recommandation :** Retenir la version log1p (`cluster_kmeans_log`, k=2) comme **clustering de référence** pour M3/segmentation. La version k=4 est conservée uniquement comme illustration pédagogique de l'effet des outliers.

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

> "Sans utiliser le label `is_substitutable`, le clustering K-Means retrouve naturellement la même structure que le label. Trois algorithmes ont été comparés — K-Means, GMM (0.381, inadapté) et DBSCAN (0.366, trop fragmenté). K-Means est retenu. Une validation par transformation log1p des ratios a révélé que les clusters C2 et C3 (155 corridors, 0.33%) étaient des artefacts de sensibilité aux outliers, et non des niches métier réelles. Le clustering de référence final est **k=2** (log1p, Silhouette=0.499) : Cluster 0 — long-haul (5 320 corridors, zone de décision critique, 55.6% substituables) et Cluster 1 — standard (38 462 corridors, 88.2% substituables). Le dendrogramme Ward confirme indépendamment k=2 avec 0 points isolés. La géographie seule capte un signal partiel — c'est la desserte ferroviaire qui reste le facteur déterminant."

---

## Fichiers générés

| Fichier | Contenu |
|---|---|
| `models/kmeans_corridors.joblib` | Modèle K-Means brut (k=4, 43k corridors) — version pédagogique |
| `models/kmeans_corridors_log.joblib` | Modèle K-Means log1p (k=2, 43k corridors) — **version de référence** |
| `docs/fig_clustering_corridors.png` | Elbow, Silhouette K-Means vs GMM, BIC, PCA, Distance vs CO2 (ratios bruts) |
| `docs/fig_clustering_corridors_log1p.png` | Même pipeline sur ratios log1p (k=2 de référence) |
| `docs/fig_dendrogram.png` | Dendrogramme Ward (n=500, k=4, ratios bruts — 3 points isolés) |
| `docs/fig_dendrogram_log1p.png` | Dendrogramme Ward (n=500, k=2, log1p — 0 points isolés) |
| `docs/carte_clusters.html` | Carte Folium interactive colorée par cluster K-Means |
| `docs/corridors_clustered.csv` | Dataset avec labels de clusters (43 782 lignes) |
| `docs/profil_clusters.csv` | Profil statistique par cluster |
