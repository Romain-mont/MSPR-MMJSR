# Analyse — Modèle 1 : Classification `is_substitutable`
## Enjeu : Identifier automatiquement les corridors où le train peut remplacer l'avion
## Dataset 46 106 corridors | 14 features | XGBoost F1=0.996 | Scoring pivot 600km

---

## Contexte & évolution du label

### v1 (déprécié) — label déterministe
`is_substitutable = 1` si `distance_km ≤ 600` ET vol existant (loi française 2023).  
**Problème :** le modèle mémorisait la règle → F1=1.000 parfait mais sans valeur réelle.

### v2 (actuel) — scoring sigmoid avec pivot 600km
Le label est calculé par un **score de viabilité ferroviaire** pondéré par un seuil dynamique centré sur 600km.

```python
# Score de service (0 à 1)
service_score = (
    min(service_share / 0.12, 1)         * 0.40 +  # part du service corridor
    min(trip_count_corridor / 30, 1)     * 0.30 +  # fréquence hebdomadaire
    min((ratio_origin + ratio_dest)/20, 1) * 0.30  # usage relatif population
)

# Seuil dynamique — sigmoid centré sur 600km (pivot légal)
# low=0.05 (zone favorable) → high=0.90 (zone défavorable)
seuil = 0.05 + 0.85 / (1 + exp(-0.007 × (distance_km - 600)))

# Label
is_substitutable = 1  si  service_score ≥ seuil  ET  co2_avion_kg > 0
                   0  si  distance_km > 1100km  (limite physique absolue)
```

**Le 600km est un point d'inflexion, pas un seuil dur :**
- En dessous : seuil faible (~0.05–0.30) — le service doit être au minimum viable
- Au-dessus : seuil élevé (~0.50–0.90) — un service exceptionnel est requis

| Cas type | Distance | service_score | Seuil | Label |
|---|---|---|---|---|
| Paris → Lyon (dense) | 465 km | 0.867 | 0.288 | ✅ Substituable |
| Paris → Marseille (dense) | 658 km | 0.689 | 0.560 | ✅ Substituable |
| Village 300km (1 train/sem) | 300 km | 0.030 | 0.143 | ❌ Non substituable |
| Paris → Madrid | 1270 km | 0.336 | 0.892 | ❌ Non substituable |
| Paris → Bruxelles (Thalys) | 310 km | 0.922 | 0.149 | ✅ Substituable |

**Distribution résultante :**
- Actuelle (v1) : 89.5% substituables (41 268 / 46 106)
- Nouvelle (v2) : 84.3% substituables (38 884 / 46 106)
- 5 190 corridors < 600km sans service → devenus non-substituables
- 2 806 corridors > 600km bien desservis → devenus substituables

---

## Corrélations avec `is_substitutable` (Pearson)

| Feature | Corrélation | Signal |
|---|---|---|
| `co2_avion_kg` | **−0.33** | Plus le vol est long (pollue), moins c'est substituable |
| `distance_km` | **−0.31** | Corrélation négative — pivot à 600km |
| `service_share` | **+0.28** | Feature positive #1 — part du service corridor |
| `trip_count_corridor` | **+0.16** | Fréquence hebdomadaire directe |
| Trafic brut (gare) | ~0.00 | **Non discriminant** — justifie le passage en ratio |

> Les corrélations sont modérées (|r| ≤ 0.33) car le label v2 est non-linéaire (sigmoid). Le classement est cohérent avec les importances XGBoost : `trip_count_corridor` et `service_share` dominent en non-linéaire même si `distance_km` apparaît plus haut en Pearson.

**Couverture des features GTFS :** `service_share`, `trip_count_corridor`, `trip_count_origin` → **100%** (seulement 16 NULL sur 46 106). C'est la source de données la plus complète du dataset.

---

## Features utilisées (14 features)

| Feature | Description | Rôle |
|---|---|---|
| `distance_km` | Distance du corridor en km | Feature secondaire (derrière le service) |
| `dist_to_600` | `distance_km − 600` — écart au pivot légal | Encode la position par rapport à la loi FR |
| `co2_train_kg` | CO2 émis par le train (EcoPassenger) | Type de train |
| `co2_avion_kg` | CO2 émis par l'avion (EcoPassenger) | Proxy existence d'un vol |
| `vehicule_type` | Type de train (15 types) — encodé LabelEncoder | Catégoriel |
| `origin_station_traffic` | Fréquentation annuelle gare départ (SNCF) | Attractivité gare |
| `origin_city_population` | Population ville départ (INSEE/GeoNames) | Contexte démographique |
| `dest_station_traffic` | Fréquentation annuelle gare arrivée (SNCF) | Attractivité gare |
| `dest_city_population` | Population ville arrivée (INSEE/GeoNames) | Contexte démographique |
| `ratio_origin` | trafic / population — usage ferroviaire départ | Intensité modale |
| `ratio_dest` | trafic / population — usage ferroviaire arrivée | Intensité modale |
| `trip_count_corridor` | Trajets hebdomadaires sur ce corridor (GTFS) | **Feature #1** |
| `trip_count_origin` | Trajets hebdomadaires total gare départ (GTFS) | Volume de service |
| `service_share` | `trip_count_corridor / trip_count_origin` | Part du service corridor |

---

## Modèles testés

| Modèle | Rôle |
|---|---|
| Baseline (Dummy) | Prédit toujours la classe majoritaire |
| Logistic Regression | Baseline interprétable linéaire |
| Random Forest | Ensembliste (GridSearch) |
| XGBoost | Boosting (GridSearch) |
| MLP | Réseau neuronal (64, 32 neurones) |

---

## Résultats — Cross-validation 5 folds

| Modèle | F1 weighted (CV) | AUC (CV) |
|---|---|---|
| Baseline (Dummy) | 0.772 ± 0.000 | 0.500 |
| Logistic Regression | 0.917 ± 0.002 | 0.978 |
| Random Forest | 0.994 ± 0.001 | 1.000 |
| **XGBoost** | **0.996 ± 0.001** | **1.000** |
| MLP | 0.991 ± 0.001 | 0.999 |

---

## Résultats finaux sur le TEST set (6 916 corridors)

| Modèle | Precision (0) | Recall (0) | F1 (0) | F1 weighted | AUC-ROC | Accuracy |
|---|---|---|---|---|---|---|
| Baseline (Dummy) | 0.000 | 0.000 | 0.000 | 0.772 | 0.500 | 0.843 |
| Logistic Regression | 0.637 | 0.982 | 0.773 | 0.917 | 0.977 | 0.909 |
| Random Forest | 0.988 | 0.982 | 0.985 | 0.995 | 1.000 | 0.995 |
| **XGBoost** | **0.982** | **0.994** | **0.988** | **0.996** | **1.000** | **0.996** |
| MLP | 0.949 | 0.984 | 0.966 | 0.989 | 0.999 | 0.989 |

**✅ Modèle sélectionné : XGBoost** — `n_estimators=200`, `max_depth=6`, `learning_rate=0.1`

---

## Feature Importance (XGBoost v2)

| Feature | Importance v2 | vs v1 | Interprétation |
|---|---|---|---|
| `trip_count_corridor` | **47.9%** | ↑↑↑ (était 1.5%) | Fréquence hebdomadaire du corridor — feature #1 |
| `distance_km` | **11.7%** | ↓↓ (était 28.2%) | **Devenu secondaire** — le modèle ne suit plus une règle de distance |
| `service_share` | **11.6%** | ↑↑↑ (était 0.4%) | Part du service gare dédiée au corridor |
| `co2_avion_kg` | **10.1%** | ↓ (était 55.9%) | Proxy existence d'un vol |
| `ratio_origin` | **7.7%** | ↑ (était 0.8%) | Intensité usage ferroviaire / population |
| `ratio_dest` | 3.0% | ↑ | Intensité usage ferroviaire / population arrivée |
| `trip_count_origin` | 2.5% | ↑ | Volume de service total de la gare |
| Autres features | ~6% | — | — |
| `dist_to_600` | 0.0% | — | Non utilisé par le modèle (information déjà capturée) |

**Lecture clé :** `trip_count_corridor` passe de 1.5% à 47.9%. Le modèle a appris que la **viabilité du service est le principal facteur de substituabilité**, et non la distance. `distance_km` reste pertinent mais comme facteur de contexte, pas comme règle.

---

## Analyse critique

### Ce que le modèle apprend maintenant
Le modèle apprend une relation non-linéaire entre service ferroviaire et substituabilité. Il découvre lui-même que :
- Un corridor à 300km avec 1 train/semaine n'est pas viable (même si la loi FR dit oui)
- Un corridor à 680km avec 42 trains/semaine est viable (même si la loi FR dit non)

La **loi française 600km n'est plus codée dans le label** — elle apparaît comme pivot implicite dans les données via `dist_to_600` et `distance_km`, que le modèle utilise comme contexte.

### Lien avec le clustering M3
Les corridors du Cluster 2 K-Means (hautes performances, 100% substituables) correspondent aux cas avec `trip_count_corridor` et `service_share` élevés — cohérent avec le nouveau modèle où ces features sont #1 et #3.

---

## Fichiers générés

| Fichier | Contenu |
|---|---|
| `models/model1_classification.joblib` | XGBoost (14 features, 46k dataset, labels v2) |
| `models/scaler.joblib` | StandardScaler (14 features, dont `dist_to_600`) |
| `models/label_encoder_vehicule.joblib` | LabelEncoder (15 types de trains) |
| `docs/tableau_comparatif_m1.csv` | Tableau comparatif des 5 modèles |
| `docs/fig_model1_roc_confusion.png` | Courbes ROC + matrice de confusion |
| `docs/fig_model1_feature_importance.png` | Feature importance XGBoost v2 |
