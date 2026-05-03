"""
Modèle 1 — Classification : is_substitutable
Enjeu : identifier automatiquement les corridors où le train peut remplacer l'avion
Label : is_substitutable=1 si distance ≤ 600km ET vol existant (loi française 2023)
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
from sklearn.model_selection import StratifiedKFold, cross_validate, GridSearchCV
from sklearn.metrics import (classification_report, confusion_matrix,
                             roc_auc_score, roc_curve, ConfusionMatrixDisplay)
import xgboost as xgb

RANDOM_STATE = 42
DATA_DIR   = os.path.join(os.path.dirname(__file__), '..', 'data')
MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
DOCS_DIR   = os.path.join(os.path.dirname(__file__), '..', 'docs')
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(DOCS_DIR,   exist_ok=True)

# ── Chargement des données ──────────────────────────────────────────────────
train = pd.read_csv(os.path.join(DATA_DIR, 'train_m1.csv'))
val   = pd.read_csv(os.path.join(DATA_DIR, 'val_m1.csv'))
test  = pd.read_csv(os.path.join(DATA_DIR, 'test_m1.csv'))

FEATURES = [c for c in train.columns if c != 'is_substitutable']
TARGET   = 'is_substitutable'

X_train, y_train = train[FEATURES].fillna(0).values, train[TARGET].values
X_val,   y_val   = val[FEATURES].fillna(0).values,   val[TARGET].values
X_test,  y_test  = test[FEATURES].fillna(0).values,  test[TARGET].values

print(f"Train : {len(X_train)} | Val : {len(X_val)} | Test : {len(X_test)}")
print(f"Features : {FEATURES}")

# ── Modèles candidats ───────────────────────────────────────────────────────
models = {
    "Baseline (Dummy)": DummyClassifier(strategy="most_frequent", random_state=RANDOM_STATE),
    "Logistic Regression": LogisticRegression(class_weight="balanced", max_iter=1000, random_state=RANDOM_STATE),
    "Random Forest": RandomForestClassifier(n_estimators=100, class_weight="balanced", random_state=RANDOM_STATE),
    "XGBoost": xgb.XGBClassifier(scale_pos_weight=y_train.sum() / (len(y_train) - y_train.sum()),
                                   eval_metric="logloss", random_state=RANDOM_STATE, verbosity=0),
    "MLP": MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=500, random_state=RANDOM_STATE),
}

# ── Cross-validation 5 folds ────────────────────────────────────────────────
print("\n=== Cross-validation 5 folds (train) ===")
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)
cv_results = {}

for name, model in models.items():
    scores = cross_validate(model, X_train, y_train, cv=cv,
                            scoring=["f1_weighted", "roc_auc"],
                            return_train_score=False)
    cv_results[name] = {
        "F1 (CV)":      scores["test_f1_weighted"].mean(),
        "F1 std":       scores["test_f1_weighted"].std(),
        "AUC (CV)":     scores["test_roc_auc"].mean(),
    }
    print(f"  {name:25s} F1={scores['test_f1_weighted'].mean():.3f}±{scores['test_f1_weighted'].std():.3f}  AUC={scores['test_roc_auc'].mean():.3f}")

# ── Optimisation hyperparamètres (Random Forest + XGBoost) ─────────────────
print("\n=== GridSearch Random Forest ===")
param_grid_rf = {
    "n_estimators": [100, 200],
    "max_depth":    [None, 10, 20],
    "min_samples_split": [2, 5],
}
gs_rf = GridSearchCV(
    RandomForestClassifier(class_weight="balanced", random_state=RANDOM_STATE),
    param_grid_rf, cv=cv, scoring="f1_weighted", n_jobs=-1
)
gs_rf.fit(X_train, y_train)
print(f"  Meilleurs params RF : {gs_rf.best_params_}")
print(f"  Meilleur F1 CV      : {gs_rf.best_score_:.3f}")

print("\n=== GridSearch XGBoost ===")
param_grid_xgb = {
    "n_estimators":  [100, 200],
    "max_depth":     [3, 6],
    "learning_rate": [0.05, 0.1],
}
gs_xgb = GridSearchCV(
    xgb.XGBClassifier(scale_pos_weight=y_train.sum() / (len(y_train) - y_train.sum()),
                       eval_metric="logloss", random_state=RANDOM_STATE, verbosity=0),
    param_grid_xgb, cv=cv, scoring="f1_weighted", n_jobs=-1
)
gs_xgb.fit(X_train, y_train)
print(f"  Meilleurs params XGB : {gs_xgb.best_params_}")
print(f"  Meilleur F1 CV       : {gs_xgb.best_score_:.3f}")

# Remplacer les modèles par les versions optimisées
models["Random Forest"] = gs_rf.best_estimator_
models["XGBoost"]       = gs_xgb.best_estimator_

# ── Évaluation finale sur le test set ───────────────────────────────────────
print("\n=== Évaluation finale sur le TEST set ===")
results = []
fitted_models = {}

for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None

    report = classification_report(y_test, y_pred, output_dict=True)
    auc    = roc_auc_score(y_test, y_proba) if y_proba is not None else None

    results.append({
        "Modèle":           name,
        "Precision (0)":    round(report["0"]["precision"], 3),
        "Recall (0)":       round(report["0"]["recall"],    3),
        "F1 (0)":           round(report["0"]["f1-score"],  3),
        "F1 weighted":      round(report["weighted avg"]["f1-score"], 3),
        "AUC-ROC":          round(auc, 3) if auc else "-",
        "Accuracy":         round(report["accuracy"], 3),
    })
    fitted_models[name] = (model, y_proba)

df_results = pd.DataFrame(results).set_index("Modèle")
print(df_results.to_string())
df_results.to_csv(os.path.join(DOCS_DIR, "tableau_comparatif_m1.csv"))

# ── Sélection du meilleur modèle ────────────────────────────────────────────
# Critère : F1 de la classe 0 (non-substituable) — cas le plus critique
best_name = df_results["F1 (0)"].idxmax()
print(f"\n✅ Meilleur modèle : {best_name}")
best_model, best_proba = fitted_models[best_name]

# ── Courbes ROC ─────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

for name, (model, proba) in fitted_models.items():
    if proba is not None and name != "Baseline (Dummy)":
        fpr, tpr, _ = roc_curve(y_test, proba)
        auc = roc_auc_score(y_test, proba)
        axes[0].plot(fpr, tpr, label=f"{name} (AUC={auc:.3f})")

axes[0].plot([0,1],[0,1],'k--', label='Random')
axes[0].set_xlabel("False Positive Rate")
axes[0].set_ylabel("True Positive Rate")
axes[0].set_title("Courbes ROC — Modèle 1")
axes[0].legend(fontsize=8)

# Matrice de confusion du meilleur modèle
y_pred_best = best_model.predict(X_test)
ConfusionMatrixDisplay(
    confusion_matrix(y_test, y_pred_best),
    display_labels=["Non-sub (0)", "Substituable (1)"]
).plot(ax=axes[1], colorbar=False, cmap='Blues')
axes[1].set_title(f"Matrice de confusion — {best_name}")

plt.tight_layout()
plt.savefig(os.path.join(DOCS_DIR, "fig_model1_roc_confusion.png"), dpi=150, bbox_inches='tight')
plt.show()

# ── Feature importance ──────────────────────────────────────────────────────
if hasattr(best_model, "feature_importances_"):
    fi = pd.Series(best_model.feature_importances_, index=FEATURES).sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(8, 5))
    fi.plot(kind='barh', ax=ax, color='#3498db')
    ax.set_title(f"Feature Importance — {best_name}")
    ax.set_xlabel("Importance")
    plt.tight_layout()
    plt.savefig(os.path.join(DOCS_DIR, "fig_model1_feature_importance.png"), dpi=150, bbox_inches='tight')
    plt.show()
    print("\nFeature Importance :")
    print(fi.sort_values(ascending=False).round(4).to_string())

# ── Sauvegarde ───────────────────────────────────────────────────────────────
model_path = os.path.join(MODELS_DIR, "model1_classification.joblib")
joblib.dump(best_model, model_path)
print(f"\n✅ Modèle sauvegardé : {model_path}")
print(f"   Tableau comparatif : docs/tableau_comparatif_m1.csv")
print(f"   Graphiques         : docs/fig_model1_*.png")
