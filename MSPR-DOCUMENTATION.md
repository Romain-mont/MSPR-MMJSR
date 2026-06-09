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
| predict.py standalone | ✅ | `scripts/predict.py` |
| API FastAPI /predict | ✅ | `api/main.py` |
| Benchmark cloud IA | ✅ | `docs/benchmark_ia_cloud.md` |
| Rapport technique | ✅ | `MSPR-DOCUMENTATION.md` + `analyse/rapport_analyse.md` |

---

## Données

**Fichier source ML :** `donnee/staging_fact_route_analysis.csv`  
**46 106 corridors français, 21 colonnes** — filtrés par GPS France métropolitaine, noms normalisés, enrichis SNCF + INSEE + GTFS

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
| Corridors analysés | **46 106** (France métropolitaine, GPS filtré) |
| Déséquilibre classes | 89.5% substituables / 10.5% non-substituables |
| Feature la plus corrélée avec is_sub | `co2_avion_kg` (-0.782) |
| Gain CO2 moyen train vs avion | 92.8 kg/passager |
| Gain CO2 médian | 86.0 kg/passager |
| Types de trains | **15** (TGV, ICE, AVE, EuroNight, Nightjet…) |
| Couverture fréquentation SNCF | ~55% (matching GPS 200m) |
| Couverture population | ~55% (INSEE + GeoNames) |
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
**Modèle sauvegardé :** `models/model2_regression.joblib` (Random Forest)  
**Split :** 30 647 train / 6 567 val / 6 568 test — **11 features** (sans co2_avion ni co2_train)

**Features utilisées : sans `co2_avion_kg` ni `co2_train_kg`** (évite le biais circulaire — le modèle apprend depuis la géographie, la démographie et la fréquence de service)

### Résultats test set

| Modèle | MAE | RMSE | R² |
|---|---|---|---|
| Baseline (Dummy) | 19.317 | 27.546 | -0.000 |
| Ridge | 10.422 | 13.418 | 0.763 |
| **Random Forest** | **4.07** | **6.268** | **0.948** |
| XGBoost | 4.785 | 6.836 | 0.938 |
| MLP | 6.587 | 9.012 | 0.893 |

### Feature importance (Random Forest)

| Feature | Importance |
|---|---|
| `distance_km` | **91.50%** |
| `trip_count_origin` | 1.22% |
| `origin_city_population` | 1.10% |
| `ratio_origin` | 0.99% |
| `service_share` | 0.98% |
| `dest_city_population` | 0.92% |
| `origin_station_traffic` | 0.84% |
| `trip_count_corridor` | 0.80% |
| `dest_station_traffic` | 0.69% |

### Garde-fou EcoPassenger

Le modèle ne voit pas les valeurs CO2 calculées. Après prédiction, comparaison avec EcoPassenger :

| Métrique | Valeur |
|---|---|
| Écart médian | **2.6 kg** |
| Prédictions dans ±10 kg | **91.2%** |
| Prédictions dans ±20 kg | **98.2%** |

`distance_km` à 91.5% est cohérent avec la physique. Le passage à 46k corridors (×19 de données) améliore le R² de 0.907 → **0.948** et le garde-fou de 90.3% → **91.2%** dans ±10 kg.

---

## Modèles sauvegardés

| Fichier | Contenu |
|---|---|
| `models/model1_classification.joblib` | Random Forest — is_substitutable (13 features, 46k dataset) |
| `models/model2_regression.joblib` | Random Forest — co2_saved_kg (11 features, 46k dataset) |
| `models/kmeans_corridors.joblib` | K-Means k=4 — clustering corridors (Silhouette=0.652) |
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
