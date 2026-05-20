# Documentation MSPR — ObRail Europe
## Projet : Substitution avion → train | Modèles ML + API

---

## État du projet

| Étape | État | Fichier |
|---|---|---|
| ETL (extraction, transform, load) | ✅ | `main.py` |
| Fichier ML enrichi | ✅ | `donnee/staging_fact_route_analysis.csv` |
| Enrichissement GPS fréquentation | ✅ | `scripts/enrich_station_traffic_gps.py` |
| EDA + Préparation ML | ✅ | `notebooks/01_EDA.ipynb` |
| Modèle 1 — Classification | ✅ | `scripts/train_model1_classification.py` |
| Modèle 2 — Régression | ✅ | `scripts/train_model2_regression.py` |
| Clustering corridors | ✅ | `scripts/clustering_corridors.py` |
| predict.py standalone | 🔲 | `scripts/predict.py` |
| API FastAPI /predict | 🔲 | `api/main.py` |
| Benchmark cloud IA | 🔲 | `docs/benchmark_ia_cloud.md` |
| Rapport technique | 🔲 | `docs/rapport_technique.md` |

---

## Données

**Fichier source ML :** `donnee/staging_fact_route_analysis.csv`  
**2687 corridors, 21 colonnes** — enrichi avec population + fréquentation SNCF + ratios + fréquence de service

| Colonne | Rôle |
|---|---|
| `distance_km` | Feature principale |
| `co2_train_kg` | Feature M1 + garde-fou M2 |
| `co2_avion_kg` | Feature M1 + garde-fou M2 |
| `co2_saved_kg` | **Cible Modèle 2** |
| `vehicule_type` | Feature catégorielle (encodée) |
| `origin/dest_station_traffic` | Feature (SNCF, ~54% couverture, matching GPS 200m) |
| `origin/dest_city_population` | Feature (INSEE/GeoNames, ~71% couverture) |
| `ratio_origin/dest` | trafic/population — intensité usage ferroviaire |
| `trip_count_corridor` | Trajets hebdomadaires sur ce corridor (GTFS) |
| `trip_count_origin` | Trajets hebdomadaires total depuis la gare départ (GTFS) |
| `service_share` | `trip_count_corridor / trip_count_origin` — part du service dédiée au corridor |
| `is_substitutable` | **Cible Modèle 1** (loi FR 2023 : ≤600km + vol existant) |
| `origin_city`, `destination_city` | Noms des villes |
| `station_lat/lon` × 2 | Coordonnées GPS des gares |

---

## EDA — Résultats clés

| Observation | Valeur |
|---|---|
| Déséquilibre classes | 85.1% substituables / 14.9% non-substituables |
| Feature la plus corrélée avec is_sub | `co2_avion_kg` (-0.782) |
| Gain CO2 moyen train vs avion | 91.3 kg/passager |
| Gain CO2 médian | 84.8 kg/passager |
| Facteur CO2 avion/train médiane | ×63 |
| Couverture fréquentation SNCF | 54% (matching GPS 200m) |
| Couverture population | 71% (INSEE + GeoNames) |
| Couverture service_share | 85.9% (GTFS via Spark) |
| service_share médian | 0.053 (5.3% du service gare départ) |

---

## Modèle 1 — Classification `is_substitutable`

**Script :** `scripts/train_model1_classification.py`  
**Modèle sauvegardé :** `models/model1_classification.joblib` (Random Forest)  
**Split :** 1782 train / 382 val / 383 test — **13 features**

### Résultats test set

| Modèle | F1 (classe 0) | F1 weighted | AUC-ROC | Accuracy |
|---|---|---|---|---|
| Baseline (Dummy) | 0.000 | 0.783 | 0.500 | 0.851 |
| Logistic Regression | 0.851 | 0.951 | 0.978 | 0.948 |
| **Random Forest** | **1.000** | **1.000** | **1.000** | **1.000** |
| XGBoost | 0.991 | 0.997 | 1.000 | 0.997 |
| MLP | 0.956 | 0.987 | 0.971 | 0.987 |

### Feature importance (Random Forest)

| Feature | Importance |
|---|---|
| `co2_avion_kg` | 52.5% |
| `distance_km` | 24.2% |
| `co2_train_kg` | 8.0% |
| `trip_count_origin` | 2.2% |
| `vehicule_type` | 2.1% |
| `service_share` | 2.0% |
| `trip_count_corridor` | 1.8% |
| Trafic + population + ratios | ~7.2% |

### Note critique

Le score parfait s'explique par la définition du label : `is_substitutable = 1 si distance ≤ 600km ET co2_avion ≠ 0`. Ces deux features sont dans le dataset → le modèle réapprend la règle. L'intérêt réel est la **généralisation à l'Europe** où la loi française ne s'applique pas : le modèle combine distance, CO2, démographie et fréquence de service pour prédire sur des corridors allemands, suisses, etc.

---

## Modèle 2 — Régression `co2_saved_kg`

**Script :** `scripts/train_model2_regression.py`  
**Modèle sauvegardé :** `models/model2_regression.joblib` (XGBoost)  
**Split :** 1633 train / 350 val / 350 test — **11 features** (sans co2_avion ni co2_train)

**Features utilisées : sans `co2_avion_kg` ni `co2_train_kg`** (évite le biais circulaire — le modèle apprend depuis la géographie, la démographie et la fréquence de service)

### Résultats test set

| Modèle | MAE | RMSE | R² |
|---|---|---|---|
| Baseline (Dummy) | 18.031 | 25.681 | -0.001 |
| Ridge | 9.915 | 13.528 | 0.722 |
| Random Forest | 5.139 | 8.143 | 0.899 |
| **XGBoost** | **4.918** | **7.823** | **0.907** |
| MLP | 8.323 | 12.258 | 0.772 |

### Feature importance (XGBoost)

| Feature | Importance |
|---|---|
| `distance_km` | **65.75%** |
| `dest_city_population` | 4.90% |
| `origin_city_population` | 4.80% |
| `vehicule_type` | 4.56% |
| `ratio_dest` | 4.49% |
| `trip_count_origin` + `service_share` + `trip_count_corridor` | ~6.7% |
| Trafic + ratios restants | ~8.8% |

### Garde-fou EcoPassenger

Le modèle ne voit pas les valeurs CO2 calculées. Après prédiction, comparaison avec EcoPassenger :

| Métrique | Valeur |
|---|---|
| Écart médian | **3.1 kg** |
| Prédictions dans ±10 kg | **90.3%** |
| Prédictions dans ±20 kg | **96.6%** |

`distance_km` à 65.75% est cohérent avec la physique. L'ajout des features de fréquence de service (trip_count, service_share) a permis de passer de 82% à **90.3%** de prédictions dans ±10 kg.

---

## Modèles sauvegardés

| Fichier | Contenu |
|---|---|
| `models/model1_classification.joblib` | Random Forest — is_substitutable (13 features) |
| `models/model2_regression.joblib` | XGBoost — co2_saved_kg (11 features) |
| `models/kmeans_corridors.joblib` | K-Means k=3 — clustering corridors |
| `models/scaler.joblib` | StandardScaler (13 features) |
| `models/label_encoder_vehicule.joblib` | LabelEncoder vehicule_type |

---

## Documents d'analyse

| Fichier | Contenu |
|---|---|
| `docs/analyse_model1.md` | Analyse complète Modèle 1 |
| `docs/analyse_model2.md` | Analyse complète Modèle 2 |
| `docs/tableau_comparatif_m1.csv` | Tableau comparatif M1 |
| `docs/tableau_comparatif_m2.csv` | Tableau comparatif M2 |
| `docs/fig_model1_roc_confusion.png` | Courbes ROC + confusion M1 |
| `docs/fig_model2_results.png` | Prédictions + résidus + features M2 |
| `docs/fig_clustering_corridors.png` | Elbow, Silhouette, PCA, clusters |
| `docs/analyse_clustering.md` | Analyse complète clustering |
| `docs/carte_corridors.html` | Carte interactive Folium |
