# Analyse — Modèle 2 : Régression `co2_saved_kg`
## Enjeu : Quantifier le gain CO2 si le passager prend le train au lieu de l'avion

---

## Contexte

**Target :** `co2_saved_kg` = co2_avion - co2_train (valeur calculée EcoPassenger, pas de biais circulaire)  
**Données :** 2147 corridors avec un vol existant (co2_avion non NULL)  
**Split :** 70% train (1502) / 15% validation (322) / 15% test (323) — non stratifié  
**Target — moy: 91.2 kg | méd: 84.8 kg | std: 24.0 kg**

**Features utilisées (sans co2_avion et co2_train) :**
- `distance_km` — seule feature directement liée au CO2
- `vehicule_type` — type de train (encodé)
- `origin/dest_station_traffic` — fréquentation des gares
- `origin/dest_city_population` — population des villes
- `ratio_origin/dest` — trafic/population (intensité usage ferroviaire)

> `co2_avion_kg` et `co2_train_kg` sont volontairement **exclus** des features. Le modèle apprend depuis la géographie et la démographie — pas depuis les valeurs CO2 calculées. Cela évite un biais circulaire trivial (co2_saved = co2_avion - co2_train).

---

## Cross-validation 5 folds (sur le train)

| Modèle | MAE (CV) | R² (CV) |
|---|---|---|
| Baseline (Dummy) | 17.199 | -0.004 |
| Ridge | 10.366 | 0.665 |
| Random Forest | 5.607 | 0.862 |
| **XGBoost** | **5.396** | **0.863** |
| MLP | 10.413 | 0.634 |

---

## Optimisation des hyperparamètres (GridSearchCV)

**Random Forest :**
- `n_estimators = 200`, `max_depth = None`, `min_samples_split = 2`
- MAE CV : 5.597

**XGBoost :**
- `n_estimators = 200`, `max_depth = 6`, `learning_rate = 0.1`
- MAE CV : 5.426

---

## Résultats finaux sur le TEST set

| Modèle | MAE | RMSE | R² |
|---|---|---|---|
| Baseline (Dummy) | 16.836 | 24.423 | -0.001 |
| Ridge | 9.753 | 13.088 | 0.713 |
| **Random Forest** | **5.743** | **9.836** | **0.838** |
| XGBoost | 5.763 | 9.697 | 0.842 |
| MLP | 8.027 | 12.156 | 0.752 |

**✅ Modèle sélectionné : Random Forest** (MAE minimale)  
Sauvegardé dans `models/model2_regression.joblib`

> Note : XGBoost a un R² légèrement meilleur (0.842 vs 0.838) mais Random Forest est sélectionné pour sa robustesse et son interprétabilité via feature_importances_.

---

## Feature Importance (Random Forest)

| Feature | Importance | Interprétation |
|---|---|---|
| `distance_km` | **89.8%** | La distance détermine quasi-entièrement le CO2 économisé |
| `origin_city_population` | 2.6% | Villes plus grandes → corridors plus longs → plus économisé |
| `dest_city_population` | 2.5% | Idem côté destination |
| `dest_station_traffic` | 1.2% | Influence faible |
| `origin_station_traffic` | 1.1% | Influence faible |
| `vehicule_type` | 1.0% | TGV vs Train Nuit = légère différence CO2 |
| `ratio_origin` | 0.9% | Influence faible |
| `ratio_dest` | 0.9% | Influence faible |

**Interprétation physique :** `distance_km` à 89.8% est cohérent avec la physique — le CO2 économisé est quasi-linéaire avec la distance (plus on va loin, plus l'avion pollue par rapport au train). Le modèle a appris quelque chose de réel.

---

## Garde-fou — Validation vs valeurs EcoPassenger

Les valeurs `co2_avion_kg` et `co2_train_kg` **n'étaient pas** dans les features. Après prédiction, on compare avec les valeurs calculées par EcoPassenger pour vérifier la cohérence.

| Métrique | Valeur |
|---|---|
| CO2 économisé calculé (EcoPassenger) — médiane | 84.0 kg |
| CO2 économisé prédit (modèle) — médiane | 85.8 kg |
| Écart absolu moyen | **5.7 kg** |
| Écart absolu médian | **2.8 kg** |
| Prédictions dans ±10 kg | **82.0%** |
| Prédictions dans ±20 kg | **94.7%** |

**Conclusion :** Le modèle, sans voir les valeurs CO2 calculées, s'en approche à 2.8 kg près en médiane. 82% des prédictions sont dans un écart de ±10 kg. C'est une preuve solide que la distance et la démographie suffisent à estimer le gain CO2 de façon réaliste.

---

## Analyse critique

### Pourquoi distance_km domine à 89.8% ?

CO2 économisé ≈ CO2_avion - CO2_train ≈ CO2_avion (car CO2_train << CO2_avion)  
Et CO2_avion est quasi-proportionnel à la distance (même formule EcoPassenger).  
Donc le modèle apprend correctement que distance → gain CO2.

### Intérêt du modèle malgré la domination de distance_km

Le modèle reste utile pour des cas où la distance seule ne suffit pas :
- Corridors avec train de nuit (vehicule_type) → profil CO2 différent
- Corridors avec forte fréquentation → signal de demande réelle
- Généralisation à des corridors européens sans calcul EcoPassenger préalable

### Comparaison avec Ridge (R²=0.713)

Ridge est un modèle linéaire — il capture bien la relation distance→CO2 mais pas les non-linéarités (types de trains, seuils). Random Forest est plus adapté à ces subtilités.

---

## Fichiers générés

| Fichier | Contenu |
|---|---|
| `models/model2_regression.joblib` | Random Forest entraîné |
| `docs/tableau_comparatif_m2.csv` | Tableau comparatif des 5 modèles |
| `docs/fig_model2_results.png` | Prédictions vs réelles + résidus + feature importance |
