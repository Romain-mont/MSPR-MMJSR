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
| predict.py standalone | 🔲 | `scripts/predict.py` |
| API FastAPI /predict | 🔲 | `api/main.py` |
| Benchmark cloud IA | 🔲 | `docs/benchmark_ia_cloud.md` |
| Rapport technique | 🔲 | `docs/rapport_technique.md` |

---

## Données

**Fichier source ML :** `donnee/staging_fact_route_analysis.csv`  
**2486 corridors, 18 colonnes** — enrichi avec population + fréquentation SNCF + ratios

| Colonne | Rôle |
|---|---|
| `distance_km` | Feature principale |
| `co2_train_kg` | Feature + garde-fou M2 |
| `co2_avion_kg` | Feature M1 + garde-fou M2 |
| `co2_saved_kg` | **Cible Modèle 2** |
| `vehicule_type` | Feature catégorielle (encodée) |
| `origin/dest_station_traffic` | Feature (SNCF, ~53% couverture) |
| `origin/dest_city_population` | Feature (INSEE/GeoNames, ~71% couverture) |
| `ratio_origin/dest` | trafic/population — intensité usage ferroviaire |
| `is_substitutable` | **Cible Modèle 1** (loi FR 2023 : ≤600km + vol existant) |

---

## EDA — Résultats clés

| Observation | Valeur |
|---|---|
| Déséquilibre classes | 80.5% substituables / 19.5% non-substituables |
| Feature la plus corrélée avec is_sub | `co2_avion_kg` (-0.782) |
| Gain CO2 moyen train vs avion | 91.4 kg/passager |
| Facteur CO2 avion/train médiane | ×63 |
| Couverture fréquentation SNCF | 53% (matching GPS 200m) |
| Couverture population | 71% (INSEE + GeoNames) |

---

## Modèle 1 — Classification `is_substitutable`

**Script :** `scripts/train_model1_classification.py`  
**Modèle sauvegardé :** `models/model1_classification.joblib` (Random Forest)

### Résultats test set

| Modèle | F1 (classe 0) | F1 weighted | AUC-ROC | Accuracy |
|---|---|---|---|---|
| Baseline (Dummy) | 0.000 | 0.781 | 0.500 | 0.850 |
| Logistic Regression | 0.914 | 0.973 | 0.997 | 0.972 |
| **Random Forest** | **1.000** | **1.000** | **1.000** | **1.000** |
| XGBoost | 0.991 | 0.997 | 1.000 | 0.997 |
| MLP | 0.971 | 0.991 | 1.000 | 0.992 |

### Feature importance (Random Forest)

| Feature | Importance |
|---|---|
| `co2_avion_kg` | 54.6% |
| `distance_km` | 27.1% |
| `co2_train_kg` | 9.3% |
| `vehicule_type` | 2.5% |
| `ratio_dest` | 1.5% |
| `ratio_origin` | 1.2% |
| Population + trafic | ~2.5% |

### Note critique

Le score parfait s'explique par la définition du label : `is_substitutable = 1 si distance ≤ 600km ET co2_avion ≠ 0`. Ces deux features sont dans le dataset → le modèle réapprend la règle. L'intérêt réel est la **généralisation à l'Europe** où la loi française ne s'applique pas : le modèle combine distance, CO2 et démographie pour prédire sur des corridors allemands, suisses, etc.

---

## Modèle 2 — Régression `co2_saved_kg`

**Script :** `scripts/train_model2_regression.py`  
**Modèle sauvegardé :** `models/model2_regression.joblib` (Random Forest)

**Features utilisées : sans `co2_avion_kg` ni `co2_train_kg`** (évite le biais circulaire — le modèle apprend depuis la géographie et la démographie)

### Résultats test set

| Modèle | MAE | RMSE | R² |
|---|---|---|---|
| Baseline (Dummy) | 16.836 | 24.423 | -0.001 |
| Ridge | 9.753 | 13.088 | 0.713 |
| **Random Forest** | **5.743** | **9.836** | **0.838** |
| XGBoost | 5.763 | 9.697 | 0.842 |
| MLP | 8.027 | 12.156 | 0.752 |

### Feature importance (Random Forest)

| Feature | Importance |
|---|---|
| `distance_km` | **89.8%** |
| `origin_city_population` | 2.6% |
| `dest_city_population` | 2.5% |
| Trafic + ratios + type | ~5% |

### Garde-fou EcoPassenger

Le modèle ne voit pas les valeurs CO2 calculées. Après prédiction, comparaison avec EcoPassenger :

| Métrique | Valeur |
|---|---|
| Écart médian | **2.8 kg** |
| Prédictions dans ±10 kg | **82%** |
| Prédictions dans ±20 kg | **95%** |

`distance_km` à 89.8% est cohérent avec la physique — le CO2 économisé est quasi-linéaire avec la distance. Le modèle a appris une relation réelle, pas une règle artificielle.

---

## Modèles sauvegardés

| Fichier | Contenu |
|---|---|
| `models/model1_classification.joblib` | Random Forest — is_substitutable |
| `models/model2_regression.joblib` | Random Forest — co2_saved_kg |
| `models/scaler.joblib` | StandardScaler (10 features) |
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
| `docs/carte_corridors.html` | Carte interactive Folium |
