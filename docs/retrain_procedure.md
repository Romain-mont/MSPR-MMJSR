# Procédure de Re-entraînement — ObRail Europe
## Quand, comment et comment valider un nouveau cycle ML

---

## Quand re-entraîner ?

| Déclencheur | Seuil | Action |
|---|---|---|
| Nouvelles données GTFS disponibles | Nouvelle livraison semestrielle | Re-entraînement complet |
| Drift détecté sur M2 (co2_saved_kg) | MAE dérive > 20% du baseline (4.07 kg) | Re-entraînement M2 |
| Nouvelles gares SNCF intégrées | > 500 nouvelles gares | Re-entraînement complet |
| Extension géographique (nouveaux pays) | Tout nouveau pays UE | Re-entraînement + validation spécifique |
| Changement législatif (seuil 600 km) | Modification de la loi française | Re-étiquetage + re-entraînement M1 |
| Performance API dégradée | F1 M1 < 0.95 sur échantillon monitoring | Re-entraînement M1 en urgence |

**Fréquence minimale recommandée :** tous les 6 mois (livraisons GTFS SNCF semestrielles).

---

## Pré-requis

```bash
# Environnement Python
python3 -m venv .venv
source .venv/bin/activate          # Linux/Mac
# .venv\Scripts\activate           # Windows
pip install -r requirements.txt

# Vérification
python -c "import sklearn, xgboost, pyspark; print('OK')"
```

**Variables d'environnement nécessaires (fichier `.env`) :**
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=obrail
POSTGRES_USER=obrail
POSTGRES_PASSWORD=...
```

---

## Étape 1 — Mise à jour des données sources

```bash
# 1a. Télécharger les nouveaux fichiers GTFS
#     Source : https://www.data.gouv.fr/fr/datasets/horaires-des-lignes-ferroviaires/
#     Remplacer dans : donnee/

# 1b. Relancer l'ETL complet (PySpark)
python main.py

# Résultat attendu : donnee/staging_fact_route_analysis.csv
# Vérification : le fichier doit avoir ≥ 46 106 lignes
python -c "
import pandas as pd
df = pd.read_csv('donnee/staging_fact_route_analysis.csv')
print(f'Corridors : {len(df)} (baseline : 46 106)')
assert len(df) >= 40000, 'ALERTE : dataset trop petit, vérifier ETL'
print('OK')
"
```

---

## Étape 2 — Enrichissement des features

```bash
# Recalculer duration_h (si nouveaux types de trains)
python scripts/enrich_duration.py

# Recalculer fréquentation GPS (si nouvelles gares SNCF)
python scripts/enrich_station_traffic_gps.py
```

---

## Étape 3 — EDA rapide (optionnel mais recommandé)

```bash
# Ouvrir le notebook pour vérifier les distributions
jupyter notebook notebooks/01_EDA.ipynb
# Vérifier : distribution is_substitutable, valeurs manquantes, nouveaux types véhicules
```

Indicateurs à surveiller :
- Taux `is_substitutable=1` : doit rester entre 85% et 92%
- Taux NULL `co2_avion_kg` : doit rester < 10%
- Nouveaux types `vehicule_type` non vus à l'entraînement → mettre à jour le LabelEncoder

---

## Étape 4 — Re-entraînement des modèles

```bash
# Modèle 1 — Classification is_substitutable
python scripts/train_model1_classification.py
# Durée estimée : ~5 min (GridSearch sur 32k lignes)

# Modèle 2 — Régression co2_saved_kg
python scripts/train_model2_regression.py
# Durée estimée : ~15 min (GridSearch sur 30k lignes)

# Clustering — K-Means corridors
python scripts/clustering_corridors.py
# Durée estimée : ~3 min
```

---

## Étape 5 — Régénérer le Scaler et LabelEncoder

```bash
python - << 'EOF'
import pandas as pd, numpy as np, joblib
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split

df = pd.read_csv('donnee/staging_fact_route_analysis.csv')
df['ratio_origin'] = (df['origin_station_traffic'] / df['origin_city_population'].replace(0, float('nan'))).fillna(0)
df['ratio_dest']   = (df['dest_station_traffic']   / df['dest_city_population'].replace(0, float('nan'))).fillna(0)

le = LabelEncoder()
df['vehicule_type'] = le.fit_transform(df['vehicule_type'])

FEATURES_M1 = ['distance_km','co2_train_kg','co2_avion_kg','vehicule_type',
               'origin_station_traffic','origin_city_population',
               'dest_station_traffic','dest_city_population',
               'ratio_origin','ratio_dest',
               'trip_count_corridor','trip_count_origin','service_share']

df_ml = df[FEATURES_M1 + ['is_substitutable']].copy().fillna(0)
X_train, _, y_train, _ = train_test_split(df_ml[FEATURES_M1], df_ml['is_substitutable'],
                                           test_size=0.30, random_state=42, stratify=df_ml['is_substitutable'])
scaler = StandardScaler()
scaler.fit(X_train)

joblib.dump(scaler, 'models/scaler.joblib')
joblib.dump(le,     'models/label_encoder_vehicule.joblib')
print("✅ scaler.joblib + label_encoder_vehicule.joblib régénérés")
EOF
```

---

## Étape 6 — Validation des nouveaux modèles

### Seuils minimaux à respecter avant mise en production

| Modèle | Métrique | Seuil minimal | Baseline actuel |
|---|---|---|---|
| M1 — Classification | F1 weighted test | ≥ 0.980 | 1.000 |
| M1 — Classification | AUC-ROC test | ≥ 0.970 | 1.000 |
| M2 — Régression | R² test | ≥ 0.920 | 0.948 |
| M2 — Régression | MAE test | ≤ 6.0 kg | 4.07 kg |
| M2 — Garde-fou EcoPassenger | % prédictions dans ±10 kg | ≥ 88% | 91.2% |
| M3 — Clustering | Silhouette Score | ≥ 0.600 | 0.652 |
| M3 — Clustering | k optimal | 3 ou 4 | 4 |

Si un seuil n'est pas atteint → **NE PAS déployer**, investiguer la cause (données, features, hyperparamètres).

---

## Étape 7 — Test de l'API

```bash
# Lancer l'API
uvicorn api.main:app --reload

# Test de santé
curl http://localhost:8000/health

# Test de prédiction (corridor Toulouse → Paris, TGV, 589 km)
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "origin": "Toulouse-Matabiau",
    "destination": "Paris-Montparnasse",
    "distance_km": 589,
    "vehicule_type": "TGV",
    "co2_train_kg": 3.5,
    "co2_avion_kg": 134.0
  }'

# Résultat attendu : is_substitutable=1, co2_saved_kg ≈ 130 kg
```

---

## Étape 8 — Déploiement

```bash
# Via Docker Compose (production)
docker-compose down
docker-compose build --no-cache
docker-compose up -d

# Vérification logs
docker-compose logs api --tail=50
```

---

## Traçabilité

À chaque re-entraînement, noter dans un fichier `docs/retrain_log.md` :

```
## AAAA-MM-JJ — Re-entraînement vX.Y
- Déclencheur : [nouvelle livraison GTFS / drift / extension pays]
- Dataset : N corridors (delta vs précédent : +X)
- M1 — F1=X.XXX | AUC=X.XXX
- M2 — R²=X.XXX | MAE=X.XX kg | Garde-fou=XX.X%
- M3 — Silhouette=X.XXX | k=X
- Validé par : [prénom]
- Déployé : [oui/non]
- Notes : ...
```

---

## Durée totale estimée

| Étape | Durée |
|---|---|
| ETL (PySpark, 46k corridors) | ~20 min |
| Enrichissement features | ~5 min |
| Re-entraînement M1 + M2 + M3 | ~25 min |
| Régénération scaler/encoder | < 1 min |
| Validation + tests API | ~10 min |
| Déploiement Docker | ~5 min |
| **Total** | **~1h** |
