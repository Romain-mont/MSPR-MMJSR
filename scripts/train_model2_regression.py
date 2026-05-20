"""
Modèle 2 — Régression : co2_saved_kg
Enjeu : quantifier le gain CO2 si le passager prend le train au lieu de l'avion
Target : co2_saved_kg = co2_avion - co2_train (valeur calculée, pas de biais circulaire)
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib
import warnings
warnings.filterwarnings('ignore')

from sklearn.dummy import DummyRegressor
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import KFold, cross_validate, GridSearchCV, train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb

RANDOM_STATE = 42
DATA_DIR   = os.path.join(os.path.dirname(__file__), '..', 'data')
MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
DOCS_DIR   = os.path.join(os.path.dirname(__file__), '..', 'docs')
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(DOCS_DIR,   exist_ok=True)

# ── Chargement ──────────────────────────────────────────────────────────────
train = pd.read_csv(os.path.join(DATA_DIR, 'train_m2.csv'))
val   = pd.read_csv(os.path.join(DATA_DIR, 'val_m2.csv'))
test  = pd.read_csv(os.path.join(DATA_DIR, 'test_m2.csv'))

TARGET   = 'co2_saved_kg'
# co2_avion_kg et co2_train_kg exclus des features :
# le modèle apprend depuis la géographie/démographie, pas depuis les CO2 calculés
# (sinon co2_saved = co2_avion - co2_train → trivial)
FEATURES = [c for c in train.columns
            if c not in (TARGET, 'co2_avion_kg', 'co2_train_kg')]

# Garde-fous : valeurs EcoPassenger calculées depuis le fichier ORIGINAL (non normalisé)
staging_path = os.path.join(os.path.dirname(__file__), '..', 'donnee', 'staging_fact_route_analysis.csv')
df_orig = pd.read_csv(staging_path).dropna(subset=['co2_train_kg','co2_saved_kg'])
df_orig['co2_avion_kg'] = df_orig['co2_avion_kg'].fillna(0)
_, tmp_orig = train_test_split(df_orig, test_size=0.30, random_state=RANDOM_STATE)
_, test_orig = train_test_split(tmp_orig, test_size=0.50, random_state=RANDOM_STATE)
guard_test = test_orig[['co2_avion_kg','co2_train_kg']].reset_index(drop=True)

X_train, y_train = train[FEATURES].fillna(0).values, train[TARGET].values
X_val,   y_val   = val[FEATURES].fillna(0).values,   val[TARGET].values
X_test,  y_test  = test[FEATURES].fillna(0).values,  test[TARGET].values

print(f"Train : {len(X_train)} | Val : {len(X_val)} | Test : {len(X_test)}")
print(f"Features utilisées : {FEATURES}")
print(f"Target — moy: {y_train.mean():.1f} | méd: {np.median(y_train):.1f} | std: {y_train.std():.1f}")

# ── Modèles candidats ───────────────────────────────────────────────────────
models = {
    "Baseline (Dummy)":   DummyRegressor(strategy="mean"),
    "Ridge":              Ridge(alpha=1.0),
    "Random Forest":      RandomForestRegressor(n_estimators=100, random_state=RANDOM_STATE),
    "XGBoost":            xgb.XGBRegressor(n_estimators=100, random_state=RANDOM_STATE, verbosity=0),
    "MLP":                MLPRegressor(hidden_layer_sizes=(64, 32), max_iter=500, random_state=RANDOM_STATE),
}

# ── Cross-validation 5 folds ────────────────────────────────────────────────
print("\n=== Cross-validation 5 folds (train) ===")
cv = KFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)
cv_results = {}

for name, model in models.items():
    scores = cross_validate(model, X_train, y_train, cv=cv,
                            scoring=["neg_mean_absolute_error", "r2"],
                            return_train_score=False)
    mae_cv = -scores["test_neg_mean_absolute_error"].mean()
    r2_cv  =  scores["test_r2"].mean()
    cv_results[name] = {"MAE (CV)": mae_cv, "R² (CV)": r2_cv}
    print(f"  {name:25s} MAE={mae_cv:.3f}  R²={r2_cv:.3f}")

# ── Optimisation hyperparamètres ────────────────────────────────────────────
print("\n=== GridSearch Random Forest ===")
gs_rf = GridSearchCV(
    RandomForestRegressor(random_state=RANDOM_STATE),
    {"n_estimators": [100, 200], "max_depth": [None, 10], "min_samples_split": [2, 5]},
    cv=cv, scoring="neg_mean_absolute_error", n_jobs=-1
)
gs_rf.fit(X_train, y_train)
print(f"  Meilleurs params : {gs_rf.best_params_}")
print(f"  MAE CV           : {-gs_rf.best_score_:.3f}")

print("\n=== GridSearch XGBoost ===")
gs_xgb = GridSearchCV(
    xgb.XGBRegressor(random_state=RANDOM_STATE, verbosity=0),
    {"n_estimators": [100, 200], "max_depth": [3, 6], "learning_rate": [0.05, 0.1]},
    cv=cv, scoring="neg_mean_absolute_error", n_jobs=-1
)
gs_xgb.fit(X_train, y_train)
print(f"  Meilleurs params : {gs_xgb.best_params_}")
print(f"  MAE CV           : {-gs_xgb.best_score_:.3f}")

models["Random Forest"] = gs_rf.best_estimator_
models["XGBoost"]       = gs_xgb.best_estimator_

# ── Évaluation finale sur le TEST set ───────────────────────────────────────
print("\n=== Évaluation finale sur le TEST set ===")
results = []
fitted_models = {}

for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2   = r2_score(y_test, y_pred)

    results.append({"Modèle": name, "MAE": round(mae,3), "RMSE": round(rmse,3), "R²": round(r2,3)})
    fitted_models[name] = (model, y_pred)

df_results = pd.DataFrame(results).set_index("Modèle")
print(df_results.to_string())
df_results.to_csv(os.path.join(DOCS_DIR, "tableau_comparatif_m2.csv"))

# ── Sélection du meilleur modèle (MAE minimale) ─────────────────────────────
best_name = df_results["MAE"].idxmin()
print(f"\n✅ Meilleur modèle : {best_name}")
best_model, best_pred = fitted_models[best_name]

# ── Visualisations ──────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

# 1. Prédictions vs réelles
axes[0].scatter(y_test, best_pred, alpha=0.4, s=15, color='#3498db')
lims = [min(y_test.min(), best_pred.min()), max(y_test.max(), best_pred.max())]
axes[0].plot(lims, lims, 'r--', linewidth=1.5, label='Parfait')
axes[0].set_xlabel("CO2 économisé réel (kg)")
axes[0].set_ylabel("CO2 économisé prédit (kg)")
axes[0].set_title(f"Prédictions vs Réelles — {best_name}")
axes[0].legend()

# 2. Résidus
residuals = y_test - best_pred
axes[1].hist(residuals, bins=40, color='#e74c3c', alpha=0.8, edgecolor='white')
axes[1].axvline(0, color='black', linestyle='--', linewidth=1.5)
axes[1].set_xlabel("Résidu (kg)")
axes[1].set_ylabel("Fréquence")
axes[1].set_title(f"Distribution des résidus — {best_name}")

# 3. Feature importance
if hasattr(best_model, "feature_importances_"):
    fi = pd.Series(best_model.feature_importances_, index=FEATURES).sort_values(ascending=True)
    fi.plot(kind='barh', ax=axes[2], color='#27ae60')
    axes[2].set_title(f"Feature Importance — {best_name}")
    axes[2].set_xlabel("Importance")
    print("\nFeature Importance :")
    print(fi.sort_values(ascending=False).round(4).to_string())
else:
    axes[2].axis('off')

plt.tight_layout()
plt.savefig(os.path.join(DOCS_DIR, "fig_model2_results.png"), dpi=150, bbox_inches='tight')
plt.show()

# ── Sauvegarde ───────────────────────────────────────────────────────────────
model_path = os.path.join(MODELS_DIR, "model2_regression.joblib")
joblib.dump(best_model, model_path)
print(f"\n✅ Modèle sauvegardé : {model_path}")
print(f"   Tableau comparatif : docs/tableau_comparatif_m2.csv")
print(f"   Graphiques         : docs/fig_model2_results.png")

# ── Garde-fou : comparaison prédiction vs valeurs EcoPassenger calculées ────
print("\n=== GARDE-FOU — Vérification vs valeurs EcoPassenger ===")
print("(co2_avion - co2_train ne sont PAS dans les features — ceci vérifie la cohérence)")

guard = guard_test.copy()
guard['co2_saved_calcule']  = guard['co2_avion_kg'] - guard['co2_train_kg']
guard['co2_saved_predit']   = best_pred
guard['ecart_kg']           = (guard['co2_saved_predit'] - guard['co2_saved_calcule']).abs()

print(f"\n  CO2 économisé calculé  — moy: {guard['co2_saved_calcule'].mean():.1f} kg | méd: {guard['co2_saved_calcule'].median():.1f} kg")
print(f"  CO2 économisé prédit   — moy: {guard['co2_saved_predit'].mean():.1f} kg  | méd: {guard['co2_saved_predit'].median():.1f} kg")
print(f"  Écart absolu moyen     : {guard['ecart_kg'].mean():.1f} kg")
print(f"  Écart absolu médian    : {guard['ecart_kg'].median():.1f} kg")
print(f"  % de prédictions dans ±10kg : {(guard['ecart_kg'] <= 10).mean()*100:.1f}%")
print(f"  % de prédictions dans ±20kg : {(guard['ecart_kg'] <= 20).mean()*100:.1f}%")
