# TODO MSPR IA — ObRail Europe
## Enjeu choisi : Identification automatique des lignes candidates à la substitution avion → train
### Source données ML : `staging_fact_route_analysis.csv` (2486 faits, 2456 corridors)

---

## 0. ETL — Finitions (pipeline déjà fonctionnel)

- [ ] Filtrer les 135 NULL `co2_train_kg` dans `transform.py` (InterCity/Nightjet sans coords GPS)
- [x] Dédupliquer les corridors bidirectionnels A→B + B→A (garder dep_name < arr_name)
- [x] Vérifier `requirements.txt` à jour (pyspark, pandas, sqlalchemy, scikit-learn, xgboost, fastapi...)


---

## 1. Analyse Exploratoire (Besoin 1 sujet)

**Livrable : `notebooks/01_EDA.ipynb`**

- [ ] Charger `staging_fact_route_analysis.csv` + jointures dimensions (DB ou CSV)
- [ ] Distribution `is_substitutable` (80/20 → documenter le déséquilibre)
- [ ] Distribution `distance_km`, `co2_train_kg`, `co2_avion_kg`, `co2_saved_kg`
- [ ] Corrélations entre features (heatmap)
- [ ] Valeurs manquantes : `co2_train_kg` (135), `annual_station_traffic`, `city_population`
- [ ] Visualisation géographique des corridors (Folium ou Plotly — livrable carte HTML)
- [ ] **Tableau des variables retenues avec justification** (livrable explicite du sujet)

---

## 2. Préparation des données ML (Besoin 1 sujet)

**Livrable : `scripts/prepare_data.py`**

- [ ] Charger et fusionner `staging_fact_route_analysis.csv` avec les dimensions
- [ ] Filtrer les 135 NULL co2_train
- [ ] Encoder `service_type` (One-Hot ou Label Encoding)
- [ ] Normaliser/standardiser les features numériques (distance, CO2, population)
- [ ] Construire les ensembles **train / validation / test** (70/15/15, stratifié sur `is_substitutable`)
- [ ] Gérer le déséquilibre 80/20 : `class_weight='balanced'` ou SMOTE

**Features retenues :**

| Feature | Source |
|---|---|
| `distance_km` | dim_route |
| `co2_train_kg` | fact |
| `co2_avion_kg` | fact |
| `co2_saved_kg` | fact |
| `service_type` (encodé) | dim_vehicle_type |
| `annual_station_traffic` | dim_station_frequentation |
| `city_population` | dim_station_frequentation |
| `is_substitutable` | **cible Modèle 1** |
| `co2_saved_kg` | **cible Modèle 2** |

---

## 3. Modèle 1 — Classification : `is_substitutable` (Besoin 3+4 sujet)

**Enjeu :** Identifier automatiquement les corridors où le train peut remplacer l'avion
**Label :** `is_substitutable` = 1 si distance ≤ 600km ET vol existant (loi française 2023)

**Livrable : `scripts/train_model1_classification.py`**

- [ ] **Baseline** : DummyClassifier (référence)
- [ ] **Logistic Regression** (baseline interprétable)
- [ ] **RandomForest Classifier**
- [ ] **XGBoost Classifier**
- [ ] **MLP Classifier** (réseau neuronal simple)
- [ ] GridSearchCV / RandomizedSearchCV sur hyperparamètres
- [ ] Cross-validation 5 folds (stratifiée)
- [ ] **Tableau comparatif** : Precision / Recall / F1 / AUC-ROC (focus classe 0 = non-substituable)
- [ ] Courbes ROC + matrice de confusion
- [ ] Feature importance (SHAP ou `feature_importances_`)
- [ ] Sélectionner et sauvegarder le modèle final : `models/model1_classification.joblib`

---

## 4. Modèle 2 — Régression : `co2_saved_kg` (Besoin 3+4 sujet)

**Enjeu :** Quantifier le gain CO2 si le passager prend le train plutôt que l'avion
**Target :** `co2_saved_kg` = co2_avion - co2_train (valeur calculée, pas de biais circulaire)

**Livrable : `scripts/train_model2_regression.py`**

- [ ] Filtrer sur `co2_saved_kg IS NOT NULL` (2351 lignes utilisables)
- [ ] **Régression linéaire Ridge** (baseline interprétable)
- [ ] **RandomForest Regressor**
- [ ] **XGBoost / LightGBM Regressor**
- [ ] **MLP Regressor** (réseau neuronal simple)
- [ ] GridSearchCV / RandomizedSearchCV
- [ ] Cross-validation 5 folds
- [ ] **Tableau comparatif** : MAE / RMSE / R²
- [ ] Visualisation résidus + distribution des erreurs
- [ ] Sélectionner et sauvegarder : `models/model2_regression.joblib`

---

## 5. Sauvegarde + Reproductibilité (Besoin 7 sujet)

**Livrable : `scripts/predict.py`**

- [ ] Sauvegarder les deux modèles finaux en `.joblib`
- [ ] Créer `predict.py` standalone :
  ```python
  # Exemple d'usage
  result = predict_substitution(distance_km=450, co2_train=3.5, co2_avion=120, service_type="TGV")
  # → {"is_substitutable": 1, "co2_saved_kg": 116.5}
  ```
- [ ] Documenter la procédure de ré-entraînement (seed fixée, steps reproductibles)
- [ ] Fixer les graines aléatoires (`random_state=42` partout)

---

## 6. API REST (Besoin 5 sujet)

**Technologie : FastAPI (déjà en place)**

**Livrable : `api/main.py` — nouvelles routes**

- [ ] `POST /predict/substitution` → utilise `model1_classification.joblib`
  ```json
  Input  : {"distance_km": 450, "co2_train_kg": 3.5, "service_type": "TGV"}
  Output : {"is_substitutable": 1, "confidence": 0.92}
  ```
- [ ] `POST /predict/co2_saved` → utilise `model2_regression.joblib`
  ```json
  Input  : {"distance_km": 450, "service_type": "TGV", "nb_stops": 1}
  Output : {"co2_saved_kg": 116.5}
  ```
- [ ] Identifier métriques de monitoring : latence, distribution CO2 prédits, drift
- [ ] Documentation Swagger automatique accessible sur `/docs`

---

## 7. Benchmark Services IA Cloud (Besoin 6 sujet)

**Livrable : `docs/benchmark_ia_cloud.md`**

- [ ] Comparer **4 services** sur nos données ferroviaires :
  - AWS SageMaker AutoPilot
  - Azure Machine Learning Studio
  - Google Vertex AI / AutoML Tables
  - HuggingFace AutoTrain
- [ ] Critères : prix, performances potentielles, RGPD/données EU, explicabilité, intégration
- [ ] **Justifier le choix modèle custom** vs service cloud pour ce projet
- [ ] Tableau comparatif final

---

## 8. Rapport Technique + Soutenance (Besoin 8+9 sujet)

**Livrables : `docs/rapport_technique.md` + `docs/support_soutenance.pdf`**

- [ ] Rapport structuré :
  - Contexte ObRail + enjeu choisi (substitution avion→train)
  - Architecture ETL (sources, pipeline, schéma étoile)
  - Méthodologie ML (choix features, modèles, évaluation)
  - Résultats + limites (135 NULL, couverture géographique, label proxy)
  - Plan d'amélioration (NetworkX graphe, clustering gares, OpenRailwayMap)
- [ ] Section **veille technique** : algorithmes, RGPD/CNIL, Green Deal EU
- [ ] Support soutenance : 20 min oral + démo API live

---

## Structure du projet attendue

```
MSPR/
├── notebooks/
│   └── 01_EDA.ipynb
├── scripts/
│   ├── prepare_data.py
│   ├── train_model1_classification.py
│   ├── train_model2_regression.py
│   └── predict.py
├── models/
│   ├── model1_classification.joblib
│   └── model2_regression.joblib
├── api/
│   └── main.py
├── docs/
│   ├── benchmark_ia_cloud.md
│   └── rapport_technique.md
├── data/staging/
│   └── staging_fact_route_analysis.csv  ← fichier source ML
└── requirements.txt
```

---

## Rappel données disponibles

| Fichier | Lignes | Usage |
|---|---|---|
| `staging_fact_route_analysis.csv` | 2486 | **Source principale ML** |
| `staging_dim_route.csv` | 2456 | Features distance, is_long_distance |
| `staging_dim_vehicle_type.csv` | 5 | Feature service_type |
| `staging_dim_station_frequentation.csv` | 519 | Features population, trafic |
| `final_routes.csv` | ~15k | Données brutes (EDA complémentaire) |

Chemin Optima : `/Volumes/Optima/MSPR_DATA/staging/`
