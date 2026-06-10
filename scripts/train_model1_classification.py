"""
Modèle 1 — Classification : is_substitutable (v2 — scoring sigmoid)
Enjeu : identifier les corridors où le train peut remplacer l'avion

Label v2 — pivot 600km (loi FR) comme point d'inflexion du scoring :
  - service_score = f(service_share, trip_count_corridor, ratio_origin, ratio_dest)
  - seuil_dynamique = sigmoid centré sur 600km (low=0.05 → high=0.90)
  - is_substitutable = 1 si service_score ≥ seuil_dynamique ET vol existant
  → Corridors < 600km mais sans service → non-substituables
  → Corridors > 600km mais très bien desservis → substituables

Feature ajoutée : dist_to_600 = distance_km - 600 (pivot légal explicite)
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib
import warnings
warnings.filterwarnings('ignore')

from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import StratifiedKFold, cross_validate, GridSearchCV, train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import (classification_report, confusion_matrix,
                             roc_auc_score, roc_curve, ConfusionMatrixDisplay)
import xgboost as xgb

RANDOM_STATE = 42
DATA_PATH  = os.path.join(os.path.dirname(__file__), '..', 'donnee', 'staging_fact_route_analysis.csv')
MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
DOCS_DIR   = os.path.join(os.path.dirname(__file__), '..', 'docs')
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(DOCS_DIR,   exist_ok=True)

# ── Chargement & feature engineering ────────────────────────────────────────
df = pd.read_csv(DATA_PATH)

df['ratio_origin'] = (df['origin_station_traffic'] / df['origin_city_population'].replace(0, np.nan)).fillna(0)
df['ratio_dest']   = (df['dest_station_traffic']   / df['dest_city_population'].replace(0, np.nan)).fillna(0)
df['dist_to_600']  = df['distance_km'] - 600   # pivot légal français explicite

le = LabelEncoder()
df['vehicule_type'] = le.fit_transform(df['vehicule_type'])
joblib.dump(le, os.path.join(MODELS_DIR, 'label_encoder_vehicule.joblib'))

FEATURES_M1 = [
    'distance_km', 'dist_to_600',
    'co2_train_kg', 'co2_avion_kg',
    'vehicule_type',
    'origin_station_traffic', 'origin_city_population',
    'dest_station_traffic', 'dest_city_population',
    'ratio_origin', 'ratio_dest',
    'trip_count_corridor', 'trip_count_origin', 'service_share',
]
TARGET = 'is_substitutable'

df_ml = df[FEATURES_M1 + [TARGET]].fillna(0)

# ── Split stratifié ──────────────────────────────────────────────────────────
X = df_ml[FEATURES_M1]
y = df_ml[TARGET]

X_tmp,  X_test, y_tmp,  y_test  = train_test_split(X, y, test_size=0.15, random_state=RANDOM_STATE, stratify=y)
X_train, X_val, y_train, y_val  = train_test_split(X_tmp, y_tmp, test_size=0.15/0.85, random_state=RANDOM_STATE, stratify=y_tmp)

# ── Scaling ──────────────────────────────────────────────────────────────────
scaler = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_val_s   = scaler.transform(X_val)
X_test_s  = scaler.transform(X_test)
joblib.dump(scaler, os.path.join(MODELS_DIR, 'scaler.joblib'))

print(f"Train : {len(X_train)} | Val : {len(X_val)} | Test : {len(X_test)}")
print(f"Distribution — Train : {y_train.mean()*100:.1f}% sub | Test : {y_test.mean()*100:.1f}% sub")
print(f"Features ({len(FEATURES_M1)}) : {FEATURES_M1}")

# ── Modèles candidats ────────────────────────────────────────────────────────
pos_weight = (y_train == 0).sum() / (y_train == 1).sum()

models = {
    "Baseline (Dummy)":   DummyClassifier(strategy="most_frequent", random_state=RANDOM_STATE),
    "Logistic Regression": LogisticRegression(class_weight="balanced", max_iter=1000, random_state=RANDOM_STATE),
    "Random Forest":       RandomForestClassifier(n_estimators=100, class_weight="balanced", random_state=RANDOM_STATE),
    "XGBoost":             xgb.XGBClassifier(scale_pos_weight=pos_weight, eval_metric="logloss",
                                              random_state=RANDOM_STATE, verbosity=0),
    "MLP":                 MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=500, random_state=RANDOM_STATE),
}

# ── Cross-validation 5 folds ─────────────────────────────────────────────────
print("\n=== Cross-validation 5 folds (train) ===")
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)
cv_results = {}

for name, model in models.items():
    scores = cross_validate(model, X_train_s, y_train, cv=cv,
                            scoring=["f1_weighted", "roc_auc"],
                            return_train_score=False)
    cv_results[name] = scores
    print(f"  {name:25s} F1={scores['test_f1_weighted'].mean():.3f}±{scores['test_f1_weighted'].std():.3f}"
          f"  AUC={scores['test_roc_auc'].mean():.3f}")

# ── GridSearch RF + XGBoost ──────────────────────────────────────────────────
print("\n=== GridSearch Random Forest ===")
gs_rf = GridSearchCV(
    RandomForestClassifier(class_weight="balanced", random_state=RANDOM_STATE),
    {"n_estimators": [100, 200], "max_depth": [None, 10, 20], "min_samples_split": [2, 5]},
    cv=cv, scoring="f1_weighted", n_jobs=-1
)
gs_rf.fit(X_train_s, y_train)
print(f"  Meilleurs params : {gs_rf.best_params_} | F1 CV : {gs_rf.best_score_:.3f}")

print("\n=== GridSearch XGBoost ===")
gs_xgb = GridSearchCV(
    xgb.XGBClassifier(scale_pos_weight=pos_weight, eval_metric="logloss",
                       random_state=RANDOM_STATE, verbosity=0),
    {"n_estimators": [100, 200], "max_depth": [3, 6], "learning_rate": [0.05, 0.1]},
    cv=cv, scoring="f1_weighted", n_jobs=-1
)
gs_xgb.fit(X_train_s, y_train)
print(f"  Meilleurs params : {gs_xgb.best_params_} | F1 CV : {gs_xgb.best_score_:.3f}")

models["Random Forest"] = gs_rf.best_estimator_
models["XGBoost"]       = gs_xgb.best_estimator_

# ── Évaluation finale ─────────────────────────────────────────────────────────
print("\n=== Évaluation finale sur le TEST set ===")
results      = []
fitted_models = {}

for name, model in models.items():
    model.fit(X_train_s, y_train)
    y_pred  = model.predict(X_test_s)
    y_proba = model.predict_proba(X_test_s)[:, 1] if hasattr(model, "predict_proba") else None
    report  = classification_report(y_test, y_pred, output_dict=True)
    auc     = roc_auc_score(y_test, y_proba) if y_proba is not None else None
    results.append({
        "Modèle":       name,
        "Precision (0)": round(report["0"]["precision"], 3),
        "Recall (0)":    round(report["0"]["recall"],    3),
        "F1 (0)":        round(report["0"]["f1-score"],  3),
        "F1 weighted":   round(report["weighted avg"]["f1-score"], 3),
        "AUC-ROC":       round(auc, 3) if auc else "-",
        "Accuracy":      round(report["accuracy"], 3),
    })
    fitted_models[name] = (model, y_proba)

df_results = pd.DataFrame(results).set_index("Modèle")
print(df_results.to_string())
df_results.to_csv(os.path.join(DOCS_DIR, "tableau_comparatif_m1.csv"))

# ── Sélection meilleur modèle ─────────────────────────────────────────────────
best_name = df_results["F1 (0)"].idxmax()
print(f"\n✅ Meilleur modèle : {best_name}")
best_model, best_proba = fitted_models[best_name]

# ── Graphiques ────────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for name, (model, proba) in fitted_models.items():
    if proba is not None and name != "Baseline (Dummy)":
        fpr, tpr, _ = roc_curve(y_test, proba)
        axes[0].plot(fpr, tpr, label=f"{name} (AUC={roc_auc_score(y_test, proba):.3f})")
axes[0].plot([0,1],[0,1],'k--', label='Random')
axes[0].set(xlabel="FPR", ylabel="TPR", title="Courbes ROC — Modèle 1 v2")
axes[0].legend(fontsize=8)

ConfusionMatrixDisplay(
    confusion_matrix(y_test, best_model.predict(X_test_s)),
    display_labels=["Non-sub (0)", "Substituable (1)"]
).plot(ax=axes[1], colorbar=False, cmap='Blues')
axes[1].set_title(f"Matrice confusion — {best_name}")
plt.tight_layout()
plt.savefig(os.path.join(DOCS_DIR, "fig_model1_roc_confusion.png"), dpi=150, bbox_inches='tight')

if hasattr(best_model, "feature_importances_"):
    fi = pd.Series(best_model.feature_importances_, index=FEATURES_M1).sort_values(ascending=False)
    print("\nFeature Importance :")
    print(fi.round(4).to_string())
    fig2, ax2 = plt.subplots(figsize=(8, 5))
    fi.sort_values().plot(kind='barh', ax=ax2, color='#3498db')
    ax2.set_title(f"Feature Importance — {best_name}")
    plt.tight_layout()
    plt.savefig(os.path.join(DOCS_DIR, "fig_model1_feature_importance.png"), dpi=150, bbox_inches='tight')

# ── Sauvegarde modèle ─────────────────────────────────────────────────────────
model_path = os.path.join(MODELS_DIR, "model1_classification.joblib")
joblib.dump(best_model, model_path)
print(f"\n✅ Modèle sauvegardé : {model_path}")
print(f"   Scaler sauvegardé : models/scaler.joblib (avec dist_to_600)")
print(f"   Tableau comparatif : docs/tableau_comparatif_m1.csv")
