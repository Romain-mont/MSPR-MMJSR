# ObRail Europe — Solution IA de substitution avion → train

<div align="center">

[![CI/CD](https://github.com/Romain-mont/MSPR-MMJSR/actions/workflows/ci.yml/badge.svg)](https://github.com/Romain-mont/MSPR-MMJSR/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.128-009688?logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Playwright](https://img.shields.io/badge/Playwright-E2E-45ba4b?logo=playwright&logoColor=white)
![Coverage](https://img.shields.io/badge/Coverage-83.91%25-brightgreen)

**MSPR TPRE622 + MSPR3 — Bloc E6.2 / E6.3 / E6.4 — DIA/DIADS 2025-2026 — EPSI**

</div>

---

## Table des matières

- [Présentation du projet](#présentation-du-projet)
- [Architecture globale](#architecture-globale)
- [Démarrage rapide](#démarrage-rapide)
- [Services et URLs](#services-et-urls)
- [Modèles ML](#modèles-ml)
- [API REST](#api-rest)
- [Tests](#tests)
- [CI/CD](#cicd)
- [Monitoring](#monitoring)
- [Structure du projet](#structure-du-projet)
- [Développement local](#développement-local)

---

## Présentation du projet

**ObRail Europe** est un observatoire ferroviaire fictif qui analyse **46 106 corridors ferroviaires européens** pour identifier les vols remplaçables par le train et quantifier le gain CO₂ par passager.

Ce dépôt couvre deux MSPRs consécutifs :

| MSPR | Bloc | Périmètre |
|---|---|---|
| **TPRE622** | E6.2 + E6.4 | ETL PySpark · EDA · Entraînement ML · `predict.py` |
| **MSPR3** | E6.3 | Frontend React · Conteneurisation · Tests · CI/CD · Monitoring |

### Contexte réglementaire

La **loi française 2023** impose la suppression de tout vol de moins de 600 km desservi par une liaison ferroviaire directe de moins de 2h30. L'IA ObRail prédit automatiquement si un corridor est substituable et calcule le gain CO₂ exact.

### Résultats clés

- **89%** des corridors analysés sont substituables
- **92.8 kg** de CO₂ économisés en moyenne par passager
- **×23.7** : l'avion émet en moyenne 23,7 fois plus de CO₂ que le train (méthode EcoPassenger UIC/IFEU)

---

## Architecture globale

```
┌──────────────────────────────────────────────────────────────────┐
│                        UTILISATEUR                                │
│                  http://localhost (port 80)                        │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Frontend React  │  Vite · React Router
                    │   (Nginx:80)     │  4 pages : Home / Trajets /
                    └────────┬────────┘  Prediction / Monitoring
                             │ fetch()
                    ┌────────▼────────┐
                    │   API FastAPI    │  Python 3.11 · Pydantic
                    │   (port 8000)    │  /health /trajets /predict
                    └──┬──────────┬───┘  /stats /metrics
                       │          │
              ┌────────▼───┐  ┌───▼──────────┐
              │ PostgreSQL │  │  Modèles ML  │
              │  (port 5432)│  │  .joblib     │
              │  46k lignes │  │  M1 · M2 · M3│
              └────────────┘  └──────────────┘
                       │
              ┌────────▼────────┐
              │   Prometheus     │  Scrape /metrics toutes les 15s
              │   (port 9090)    │
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │    Grafana       │  Dashboard ObRail · 8 panels
              │   (port 3000)    │  latence · débit · erreurs · ML
              └─────────────────┘
```

### Pipeline ETL (TPRE622)

```
Sources GTFS (SNCF)          →  PySpark Transform  →  PostgreSQL
Back on Track (CSV)          →  Enrichissement GPS  →  46 106 corridors
OurAirports                  →  Feature Engineering →  21 colonnes ML
INSEE / SNCF fréquentation   →  Modèles ML          →  .joblib artefacts
```

---

## Démarrage rapide

### Prérequis

- **Docker Desktop** ≥ 20.10 + **Docker Compose** ≥ 2.0
- Git
- 4 Go RAM minimum (8 Go recommandés)

### Lancer l'application complète

```bash
# 1. Cloner le projet
git clone https://github.com/Romain-mont/MSPR-MMJSR.git
cd MSPR-MMJSR

# 2. Configurer les variables d'environnement
cp .env.example .env
# Éditer .env si besoin (les valeurs par défaut fonctionnent en local)

# 3. Lancer tous les services
docker compose up -d

# 4. Vérifier que tout est démarré
docker compose ps
```

L'application est prête quand l'API répond :

```bash
curl http://localhost:8000/health
# {"status":"ok","db":true,"version":"1.0.0"}
```

### Arrêter les services

```bash
# Arrêt propre (données conservées)
docker compose down

# Arrêt + suppression des volumes (réinitialisation complète)
docker compose down -v
```

---

## Services et URLs

| Service | URL | Identifiants |
|---|---|---|
| **Frontend** | http://localhost | — |
| **API REST** | http://localhost:8000 | — |
| **Swagger / OpenAPI** | http://localhost:8000/docs | — |
| **Grafana** | http://localhost:3000 | `admin` / `obrail2025` |
| **Prometheus** | http://localhost:9090 | — |
| **pgAdmin** | http://localhost:5050 | voir `.env` |

### Connexion pgAdmin à la base

1. Ouvrir http://localhost:5050
2. **Add New Server** → onglet **Connection** :
   - Host : `db`
   - Port : `5432`
   - Database : `mspr_db`
   - Username : `mspr_user`
   - Password : `mspr_password`

---

## Modèles ML

Trois modèles complémentaires entraînés sur 46 106 corridors (méthode CRISP-DM) :

### M1 — Classification : est-ce substituable ?

- **Algorithme** : Random Forest Classifier (GridSearchCV)
- **Cible** : `is_substitutable` (loi française 2023 : ≤ 600 km + vol existant)
- **Performances** : F1 = **1.000** · AUC-ROC = **1.000** · Precision = **1.000**
- **Fichier** : `models/model1_classification.joblib`

### M2 — Régression : combien de CO₂ économisé ?

- **Algorithme** : Random Forest Regressor
- **Cible** : `co2_saved_kg` (gain CO₂ par passager avion → train)
- **Performances** : R² = **0.948** · MAE = **4.07 kg** · RMSE = **7.2 kg**
- **Fichier** : `models/model2_regression.joblib`

### M3 — Clustering : profil du corridor

- **Algorithme** : K-Means k=4
- **Objectif** : segmentation des corridors par profil de substituabilité
- **Score Silhouette** : **0.652**
- **Fichier** : `models/kmeans_corridors.joblib`

### Artefacts complémentaires

```
models/
├── model1_classification.joblib    # Classificateur Random Forest
├── model2_regression.joblib        # Régresseur CO₂
├── kmeans_corridors.joblib         # Clustering K-Means
├── scaler.joblib                   # StandardScaler features M1
└── label_encoder_vehicule.joblib   # LabelEncoder vehicule_type
```

### Utiliser `predict.py` en standalone

```bash
python scripts/predict.py \
  --distance_km 450 \
  --vehicule_type InterCity

# Résultat :
# is_substitutable: 1
# proba_substitutable: 0.87
# co2_saved_kg: 95.2
```

---

## API REST

L'API expose les données et les prédictions ML. Documentation interactive : http://localhost:8000/docs

### Endpoints principaux

#### `GET /health`
État de santé du service et de la base de données.

```bash
curl http://localhost:8000/health
```
```json
{"status": "ok", "db": true, "version": "1.0.0"}
```

#### `GET /trajets`
Liste des corridors ferroviaires, filtrable.

```bash
# Tous les trajets
curl http://localhost:8000/trajets

# Filtrer par gare
curl "http://localhost:8000/trajets?origine=Paris&destination=Lyon"

# Uniquement les substituables
curl "http://localhost:8000/trajets?substituable=true&limit=50"
```

#### `GET /trajets/{id}`
Détail d'un corridor par son identifiant.

```bash
curl http://localhost:8000/trajets/1
```

#### `GET /stats/volumes`
Statistiques agrégées : répartition jour/nuit, par type de train, totaux.

```bash
curl http://localhost:8000/stats/volumes
```

#### `POST /predict/substitution`
Modèle M1 — prédit si un corridor est substituable.

```bash
curl -X POST http://localhost:8000/predict/substitution \
  -H "Content-Type: application/json" \
  -d '{
    "origin": "Paris",
    "destination": "Lyon",
    "distance_km": 450,
    "vehicule_type": "InterCity"
  }'
```
```json
{
  "is_substitutable": 1,
  "proba_substitutable": 0.87,
  "vehicule_type_encoded": 1,
  "latency_ms": 12.5
}
```

#### `POST /predict/co2_saved`
Modèles M1 + M2 — prédit la substituabilité ET le gain CO₂.

```bash
curl -X POST http://localhost:8000/predict/co2_saved \
  -H "Content-Type: application/json" \
  -d '{
    "origin": "Paris",
    "destination": "Lyon",
    "distance_km": 450,
    "vehicule_type": "InterCity",
    "flight_exists": true
  }'
```
```json
{
  "is_substitutable": 1,
  "proba_substitutable": 0.87,
  "co2_saved_kg": 95.2,
  "co2_avion_kg_used": 134.0,
  "co2_avion_estimated": true,
  "origin": "Paris",
  "destination": "Lyon",
  "latency_ms": 14.2
}
```

#### `GET /metrics`
Métriques Prometheus (scraped automatiquement par Prometheus toutes les 15s).

---

## Tests

Le projet dispose de **191 tests** répartis sur deux suites indépendantes.

### Tests backend (pytest)

```bash
# Installer les dépendances de test
pip install -r requirements-test.txt
pip install "starlette>=0.40.0,<1.0.0"

# Lancer tous les tests (avec rapport de couverture)
pytest

# Tests unitaires uniquement
pytest tests/unit/

# Tests d'intégration uniquement
pytest tests/integration/
```

| Suite | Fichier | Tests | Couverture |
|---|---|---|---|
| Unitaires CO₂ | `tests/unit/test_co2_estimation.py` | 11 | — |
| Unitaires ML | `tests/unit/test_predict_logic.py` | 19 | — |
| Intégration health | `tests/integration/test_health.py` | 8 | — |
| Intégration trajets | `tests/integration/test_trajets.py` | 18 | — |
| Intégration stats | `tests/integration/test_stats.py` | 8 | — |
| Intégration predict | `tests/integration/test_predict_api.py` | 25 | — |
| Intégration legacy | `tests/integration/test_legacy_endpoints.py` | 20 | — |
| **Total backend** | | **109** | **83.91%** ✅ |

### Tests E2E Playwright (frontend)

```bash
cd frontend

# Installer les dépendances + navigateur Chromium
npm ci
npx playwright install chromium --with-deps

# Lancer les tests E2E (démarre Vite automatiquement)
npm run test:e2e

# Mode interactif avec UI Playwright
npm run test:e2e:ui

# Consulter le rapport HTML
npm run test:e2e:report
```

| Suite | Fichier | Tests |
|---|---|---|
| Navigation | `e2e/navigation.spec.js` | 11 |
| Page d'accueil | `e2e/home.spec.js` | 17 |
| Page Trajets | `e2e/trajets.spec.js` | 17 |
| Page Prédiction | `e2e/prediction.spec.js` | 21 |
| Page Monitoring | `e2e/monitoring.spec.js` | 16 |
| **Total E2E** | | **82** ✅ |

> Les tests E2E mockent entièrement l'API backend (`page.route()`) — aucun serveur backend requis.

---

## CI/CD

Le pipeline GitHub Actions se déclenche à chaque push sur `main` ou `feat/**`.

```
push / PR
    │
    ├── Tests Backend (pytest)    ──┐
    │   Python 3.11                 │
    │   pytest --cov-fail-under=80  ├── en parallèle
    │                               │
    └── Tests E2E (Playwright)    ──┘
        Node 20 + Chromium              │
                                        ▼
                               Build Docker images
                               (seulement si les 2 passent)
                               obrail-api + obrail-frontend
```

**Fichier** : `.github/workflows/ci.yml`

**Artefacts produits à chaque run :**
- Rapport de couverture HTML (`htmlcov/`)
- Rapport Playwright HTML (`playwright-report/`)

---

## Monitoring

### Grafana — tableau de bord

Accès : http://localhost:3000 · login `admin` / `obrail2025`

Le dashboard **ObRail Europe — API Monitoring** s'affiche automatiquement (provisionné via `monitoring/grafana/provisioning/`).

**8 panels disponibles :**

| Panel | Métrique |
|---|---|
| Disponibilité API | `up{job="obrail-api"}` |
| Requêtes totales | `http_requests_total` |
| Taux d'erreurs (%) | `rate(http_requests_total{status=~"5.."}[5m])` |
| Latence médiane (ms) | `histogram_quantile(0.50, ...)` |
| Débit par endpoint | `rate(http_requests_total[5m])` par endpoint |
| Latence P50 / P95 / P99 | percentiles de latence |
| Erreurs HTTP 4xx / 5xx | compteurs erreurs client/serveur |
| Prédictions ML | requêtes sur `/predict/substitution` et `/predict/co2_saved` |

### Prometheus

Accès : http://localhost:9090

Prometheus scrape `http://api:8000/metrics` toutes les **15 secondes**. Les métriques sont générées automatiquement par `prometheus-fastapi-instrumentator`.

**Configuration** : `monitoring/prometheus.yml`

### Logs applicatifs

L'API produit des logs structurés via `logging.basicConfig(level=INFO)` :

```bash
# Consulter les logs en temps réel
docker compose logs -f api
```

---

## Structure du projet

```
MSPR/
│
├── .github/workflows/ci.yml          # Pipeline GitHub Actions
├── docker-compose.yml                # Orchestration complète (8 services)
├── .env.example                      # Variables d'environnement (modèle)
│
├── api/                              # Backend FastAPI
│   ├── main.py                       # 12 endpoints REST + monitoring
│   ├── requirements.txt
│   └── Dockerfile
│
├── frontend/                         # Frontend React + Vite
│   ├── src/
│   │   ├── pages/                    # Home · Trajets · Prediction · Monitoring
│   │   ├── components/               # Navbar · BackButton · StatCard
│   │   └── services/api.js           # Client HTTP centralisé
│   ├── e2e/                          # Tests Playwright
│   │   ├── helpers.js                # Mocks API partagés
│   │   ├── navigation.spec.js
│   │   ├── home.spec.js
│   │   ├── trajets.spec.js
│   │   ├── prediction.spec.js
│   │   └── monitoring.spec.js
│   ├── playwright.config.js
│   ├── package.json
│   └── Dockerfile
│
├── scripts/
│   ├── predict.py                    # Pipeline de prédiction ML (standalone + API)
│   ├── train_model1_classification.py
│   ├── train_model2_regression.py
│   └── clustering_corridors.py
│
├── models/                           # Artefacts ML (.joblib)
│   ├── model1_classification.joblib
│   ├── model2_regression.joblib
│   ├── kmeans_corridors.joblib
│   ├── scaler.joblib
│   └── label_encoder_vehicule.joblib
│
├── tests/                            # Tests backend
│   ├── conftest.py                   # Fixtures · helpers de mock
│   ├── unit/
│   │   ├── test_co2_estimation.py    # 11 tests
│   │   └── test_predict_logic.py    # 19 tests
│   └── integration/
│       ├── test_health.py            # 8 tests
│       ├── test_trajets.py           # 18 tests
│       ├── test_stats.py             # 8 tests
│       ├── test_predict_api.py       # 25 tests
│       └── test_legacy_endpoints.py  # 20 tests
│
├── monitoring/
│   ├── prometheus.yml                # Config scrape
│   └── grafana/
│       ├── provisioning/             # Datasource + dashboard auto-chargés
│       └── dashboards/obrail.json   # Dashboard ObRail (8 panels)
│
├── notebooks/
│   └── 01_EDA.ipynb                  # Analyse exploratoire + préparation ML
│
├── database/
│   └── init.sql                      # Schéma PostgreSQL initial
│
├── docs/
│   ├── stratégie de test.md          # Stratégie AMDEC + pyramide de tests
│   ├── analyse_model1.md
│   ├── analyse_model2.md
│   ├── analyse_clustering.md
│   └── benchmark_ia_cloud.md        # AWS SageMaker · Azure ML · Vertex AI · HuggingFace
│
├── pytest.ini                        # Config pytest (seuil 80%)
├── .coveragerc                       # Périmètre de couverture
└── requirements-test.txt             # pytest · pytest-cov · httpx
```

---

## Développement local

### Backend sans Docker

```bash
# Créer un environnement virtuel
python -m venv .venv
source .venv/bin/activate

# Installer les dépendances
pip install -r api/requirements.txt
pip install "starlette>=0.40.0,<1.0.0"

# Lancer l'API (nécessite PostgreSQL local ou Docker)
uvicorn api.main:app --reload --port 8000
```

### Frontend sans Docker

```bash
cd frontend
npm install
npm run dev
# → http://localhost:5173
```

### Ré-entraîner les modèles ML

```bash
# Modèle 1 — Classification
python scripts/train_model1_classification.py

# Modèle 2 — Régression
python scripts/train_model2_regression.py

# Clustering
python scripts/clustering_corridors.py
```

### Lancer les notebooks

```bash
pip install jupyter
jupyter notebook notebooks/01_EDA.ipynb
```

---

## Conformité RGPD

- Aucune donnée personnelle traitée (données ferroviaires publiques)
- Pas de logs nominatifs
- Données sources open data : GTFS SNCF, Back on Track, OurAirports, INSEE
- Conformité documentée dans `docs/benchmark_ia_cloud.md`

---

## Contributeurs

Projet académique MSPR TPRE622 + MSPR3 — DIA/DIADS 2025-2026 — EPSI

---

<div align="center">
<strong>Pour une mobilité ferroviaire durable en Europe</strong>
</div>
