# TODO MSPR IA — ObRail Europe
## Enjeu : Identification automatique des lignes candidates à la substitution avion → train
### Fichier ML source : `donnee/staging_fact_route_analysis.csv` (18 colonnes, enrichi)

---

## 0. ETL — Finitions

- [x] Dédupliquer les corridors bidirectionnels A→B + B→A
- [x] Vérifier `requirements.txt` à jour
- [x] Fichier ML enrichi avec population + fréquentation gares
- [x] Un seul fichier final produit par le transform

---

## 1. EDA + Préparation ML

**Livrable : `notebooks/01_EDA.ipynb`** ← couvre sections 1 ET 2

- [x] Chargement fichier enrichi 18 colonnes
- [x] Distribution `is_substitutable` (85/14 → déséquilibre documenté)
- [x] Distributions CO2, distance, fréquentation, population
- [x] CO2 économisé vs Distance par classe
- [x] Matrice de corrélations
- [x] Valeurs manquantes → stratégie documentée (NULL → 0)
- [x] Tableau des variables retenues (livrable obligatoire sujet)
- [ ] Visualisation géographique Folium (carte HTML) ← kernel à régler
- [x] Encodage `vehicule_type` → LabelEncoder sauvegardé
- [x] Split train/val/test 70/15/15 stratifié
- [x] StandardScaler sauvegardé → `models/scaler.joblib`
- [x] Datasets ML sauvegardés → `data/train_m1.csv`, `data/test_m1.csv`, etc.

---

## 2. Modèle 1 — Classification : `is_substitutable`

**Livrable : `scripts/train_model1_classification.py`**

- [ ] DummyClassifier (baseline référence)
- [ ] Logistic Regression
- [ ] RandomForest Classifier
- [ ] XGBoost Classifier
- [ ] MLP Classifier
- [ ] GridSearchCV / RandomizedSearchCV
- [ ] Cross-validation 5 folds stratifiée
- [ ] Tableau comparatif : Precision / Recall / F1 / AUC-ROC
- [ ] Courbes ROC + matrice de confusion
- [ ] Feature importance (SHAP ou `feature_importances_`)
- [ ] Sauvegarder : `models/model1_classification.joblib`

---

## 3. Modèle 2 — Régression : `co2_saved_kg`

**Livrable : `scripts/train_model2_regression.py`**

- [ ] Ridge (baseline interprétable)
- [ ] RandomForest Regressor
- [ ] XGBoost / LightGBM Regressor
- [ ] MLP Regressor
- [ ] GridSearchCV / RandomizedSearchCV
- [ ] Cross-validation 5 folds
- [ ] Tableau comparatif : MAE / RMSE / R²
- [ ] Visualisation résidus
- [ ] Sauvegarder : `models/model2_regression.joblib`

---

## 4. Sauvegarde + Reproductibilité

**Livrable : `scripts/predict.py`**

- [x] `models/scaler.joblib` + `models/label_encoder_vehicule.joblib`
- [ ] `models/model1_classification.joblib`
- [ ] `models/model2_regression.joblib`
- [ ] `scripts/predict.py` standalone
- [ ] `random_state=42` partout

---

## 5. API REST

**Technologie : FastAPI (déjà en place)**

- [ ] `POST /predict/substitution`
- [ ] `POST /predict/co2_saved`
- [ ] Métriques monitoring (latence, drift)
- [ ] Swagger `/docs`

---

## 6. Benchmark Services IA Cloud

**Livrable : `docs/benchmark_ia_cloud.md`**

- [ ] AWS SageMaker / Azure ML / Google Vertex AI / HuggingFace
- [ ] Tableau comparatif : prix, perfs, RGPD, explicabilité
- [ ] Justifier choix modèle custom

---

## 7. Rapport + Soutenance

- [ ] `docs/rapport_technique.md`
- [ ] Section veille technique
- [ ] Support soutenance (20 min + démo API)
