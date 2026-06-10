# Analyse — Modèle 2 : Régression `co2_saved_kg`
## Enjeu : Quantifier le gain CO2 si le passager prend le train au lieu de l'avion
## Dataset 43 782 corridors | 11 features | Random Forest MAE=4.07 kg R²=0.948

---

## Contexte

**Target :** `co2_saved_kg` = co2_avion - co2_train (valeur calculée EcoPassenger)  
**Données :** 43 782 corridors avec vol existant (co2_avion non NULL) — sur 46 106 total  
**Split :** 70% train (30 647) / 15% validation (6 567) / 15% test (6 568) — non stratifié (seed=42)  
**Target — moy: 92.8 kg | méd: 86.0 kg | std: 27.2 kg**  
**Normalisation :** StandardScaler (fit sur train uniquement)

**Features utilisées (11 features, sans co2_avion et co2_train) :**

| Feature | Description |
|---|---|
| `distance_km` | Distance du corridor (feature dominante) |
| `vehicule_type` | Type de train (15 types encodés) |
| `origin/dest_station_traffic` | Fréquentation SNCF |
| `origin/dest_city_population` | Population des villes (INSEE/GeoNames) |
| `ratio_origin/dest` | trafic/population — intensité usage ferroviaire |
| `trip_count_corridor` | Trajets hebdomadaires sur ce corridor (GTFS) |
| `trip_count_origin` | Trajets hebdomadaires total gare départ (GTFS) |
| `service_share` | `trip_count_corridor / trip_count_origin` |

> `co2_avion_kg` et `co2_train_kg` sont volontairement exclus pour éviter le biais circulaire trivial.

---

## Cross-validation 5 folds (sur le train)

| Modèle | MAE (CV) | R² (CV) |
|---|---|---|
| Baseline (Dummy) | 19.171 | -0.000 |
| Ridge | 10.391 | 0.758 |
| **Random Forest** | **4.328** | **0.944** |
| XGBoost | 4.709 | 0.941 |
| MLP | 6.616 | 0.890 |

---

## Optimisation des hyperparamètres (GridSearchCV)

**Random Forest :** `n_estimators=200`, `max_depth=None`, `min_samples_split=2` → MAE CV : **4.314**  
**XGBoost :** `n_estimators=200`, `max_depth=6`, `learning_rate=0.1` → MAE CV : **4.836**

---

## Résultats finaux sur le TEST set (6 568 corridors)

| Modèle | MAE | RMSE | R² |
|---|---|---|---|
| Baseline (Dummy) | 19.317 | 27.546 | -0.000 |
| Ridge | 10.422 | 13.418 | 0.763 |
| **Random Forest** | **4.07** | **6.268** | **0.948** |
| XGBoost | 4.785 | 6.836 | 0.938 |
| MLP | 6.587 | 9.012 | 0.893 |

**✅ Modèle sélectionné : Random Forest** (MAE=4.07 kg, R²=0.948)

---

## Feature Importance (Random Forest)

| Feature | Importance | Interprétation |
|---|---|---|
| `distance_km` | **91.50%** | La distance détermine l'essentiel du CO2 économisé |
| `trip_count_origin` | 1.22% | Volume de service gare départ (GTFS) |
| `origin_city_population` | 1.10% | Population ville départ |
| `ratio_origin` | 0.99% | Intensité usage ferroviaire départ |
| `service_share` | 0.98% | Part du service hebdomadaire sur ce corridor |
| `dest_city_population` | 0.92% | Population ville arrivée |
| `origin_station_traffic` | 0.84% | Fréquentation SNCF gare départ |
| `trip_count_corridor` | 0.80% | Trajets hebdomadaires sur le corridor |
| `dest_station_traffic` | 0.69% | Fréquentation SNCF gare arrivée |
| `ratio_dest` | 0.60% | Intensité usage ferroviaire arrivée |
| `vehicule_type` | 0.35% | Type de train |

---

## Garde-fou — Validation vs valeurs EcoPassenger

| Métrique | Valeur |
|---|---|
| CO2 calculé (EcoPassenger) — médiane | 85.8 kg |
| CO2 prédit (modèle) — médiane | 86.3 kg |
| Écart absolu moyen | **4.1 kg** |
| Écart absolu médian | **2.6 kg** |
| Prédictions dans ±10 kg | **91.2%** |
| Prédictions dans ±20 kg | **98.2%** |

---

## Comparaison avec l'ancienne version (2 687 corridors)

| Métrique | Ancien (2 687) | Nouveau (46 106) | Évolution |
|---|---|---|---|
| Meilleur modèle | XGBoost | **Random Forest** | changement |
| MAE | 4.918 kg | **4.07 kg** | **-17%** |
| R² | 0.907 | **0.948** | **+4.5 pts** |
| Garde-fou ±10kg | 90.3% | **91.2%** | **+0.9 pts** |
| Train split | 1 633 | **30 647** | **×19** |

Le passage à 46k corridors améliore significativement toutes les métriques.

---

## Lien avec le clustering M3

La domination de `distance_km` à 91.5% est cohérente avec la structure des clusters K-Means :
- **Cluster 0** (604 km moy) → CO2 économisé ~148 kg — zone de forte valeur de substitution
- **Clusters 1/2/3** (150-167 km moy) → CO2 économisé ~83-86 kg — majorité du dataset

Le Modèle 2 sert à quantifier précisément le gain dans chaque cluster — complément métier du clustering.

---

## Fichiers générés

| Fichier | Contenu |
|---|---|
| `models/model2_regression.joblib` | Random Forest (11 features, 46k dataset) |
| `data/train_m2.csv` | Split train (30 647 lignes) |
| `data/val_m2.csv` | Split validation (6 567 lignes) |
| `data/test_m2.csv` | Split test (6 568 lignes) |
| `docs/tableau_comparatif_m2.csv` | Tableau comparatif des 5 modèles |
| `docs/fig_model2_results.png` | Prédictions + résidus + feature importance |
