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

- [x] DummyClassifier (baseline référence)
- [x] Logistic Regression
- [x] RandomForest Classifier
- [x] XGBoost Classifier
- [x] MLP Classifier
- [x] GridSearchCV / RandomizedSearchCV
- [x] Cross-validation 5 folds stratifiée
- [x] Tableau comparatif : Precision / Recall / F1 / AUC-ROC
- [x] Courbes ROC + matrice de confusion
- [x] Feature importance (`feature_importances_`)
- [x] Sauvegarder : `models/model1_classification.joblib`
- [x] Analyse documentée : `docs/analyse_model1.md`

---

## 3. Modèle 2 — Régression : `co2_saved_kg`

**Livrable : `scripts/train_model2_regression.py`**

- [x] Ridge (baseline interprétable)
- [x] RandomForest Regressor
- [x] XGBoost Regressor
- [x] MLP Regressor
- [x] GridSearchCV / RandomizedSearchCV
- [x] Cross-validation 5 folds
- [x] Tableau comparatif : MAE / RMSE / R²
- [x] Visualisation résidus + feature importance
- [x] Garde-fou EcoPassenger (vérification cohérence)
- [x] Sauvegarder : `models/model2_regression.joblib`
- [x] Analyse documentée : `docs/analyse_model2.md`

---

## 3b. Clustering de corridors (bonus)

**Livrable : `scripts/clustering_corridors.py`**

- [x] K-means k=2 à 6 avec Elbow + Silhouette
- [x] DBSCAN (détection outliers)
- [x] Profil des 3 clusters
- [x] Validation vs label is_substitutable
- [x] Graphiques + CSV résultats
- [x] Analyse documentée : `docs/analyse_clustering.md`
- [x] Sauvegarder : `models/kmeans_corridors.joblib`

---

## 4. Sauvegarde + Reproductibilité

**Livrable : `scripts/predict.py`**

- [x] `models/scaler.joblib` + `models/label_encoder_vehicule.joblib`
- [x] `models/model1_classification.joblib`
- [x] `models/model2_regression.joblib`
- [x] `scripts/predict.py` standalone
- [x] `random_state=42` partout

---

## 5. API REST

**Technologie : FastAPI (déjà en place)**

- [x] `POST /predict/substitution`
- [x] `POST /predict/co2_saved`
- [x] Latence mesurée (latency_ms dans la réponse)
- [x] Swagger `/docs`

---

## 6. Benchmark Services IA Cloud

**Livrable : `docs/benchmark_ia_cloud.md`**

- [x] AWS SageMaker / Azure ML / Google Vertex AI / HuggingFace
- [x] Tableau comparatif : prix, perfs, RGPD, explicabilité
- [x] Justifier choix modèle custom

---

## 7. Rapport + Soutenance

- [ ] `docs/rapport_technique.md`
- [ ] Section veille technique
- [ ] Support soutenance (20 min + démo API)
