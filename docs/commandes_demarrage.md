# Commandes utiles pour démarrer le projet

Ce fichier regroupe les commandes les plus utiles pour lancer le projet ObRail Europe.

## 1) Préparer le projet (une seule fois)

```bash
cp .env.example .env
```

Adapter si besoin les variables dans `.env` (les valeurs par défaut fonctionnent en local).

## 2) Démarrage rapide

### Frontend + API uniquement (sans monitoring)

```bash
docker compose up -d db api frontend
```

> `db` est requis car l'API en dépend.

### Stack complète (+ Grafana + Prometheus + pgAdmin)

```bash
docker compose up -d
```

Vérifier que l'API est prête :

```bash
curl http://localhost:8000/health
# {"status":"ok","db":true,"version":"1.0.0"}
```

Voir les logs en temps réel :

```bash
docker compose logs -f
docker compose logs -f api        # API seulement
docker compose logs -f frontend   # Frontend seulement
```

## 3) URLs des services

| Service        | URL                          | Identifiants          |
|----------------|------------------------------|-----------------------|
| Frontend       | http://localhost             | —                     |
| API REST       | http://localhost:8000        | —                     |
| Swagger / docs | http://localhost:8000/docs   | —                     |
| Grafana        | http://localhost:3000        | admin / obrail2025    |
| Prometheus     | http://localhost:9090        | —                     |
| pgAdmin        | http://localhost:5050        | voir `.env`           |

## 4) Arrêt et nettoyage

Arrêter les conteneurs (données conservées) :

```bash
docker compose down
```

Arrêter + supprimer les volumes (réinitialisation complète) :

```bash
docker compose down -v
```

Rebuild des images après modification du code :

```bash
docker compose build
docker compose up -d
```

## 5) Tests backend (pytest)

```bash
# Installer les dépendances
pip install -r api/requirements.txt -r requirements-test.txt
pip install "starlette>=0.40.0,<1.0.0"

# Lancer tous les tests (seuil 80% couverture)
pytest

# Tests unitaires uniquement
pytest tests/unit/

# Tests d'intégration uniquement
pytest tests/integration/

# Avec rapport HTML de couverture
pytest --cov-report=html
open htmlcov/index.html
```

## 6) Tests E2E Playwright (frontend)

```bash
cd frontend

# Installer les dépendances + navigateur (une seule fois)
npm ci
npx playwright install chromium --with-deps

# Lancer les tests E2E
npm run test:e2e

# Mode UI interactif
npm run test:e2e:ui

# Afficher le rapport HTML
npm run test:e2e:report
```

## 7) Développement local sans Docker

### API (backend)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r api/requirements.txt
uvicorn api.main:app --reload --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
# → http://localhost:5173
```

## 8) Ré-entraîner les modèles ML

```bash
python scripts/train_model1_classification.py
python scripts/train_model2_regression.py
python scripts/clustering_corridors.py
```

Prédiction standalone :

```bash
python scripts/predict.py --distance_km 450 --vehicule_type InterCity
```

## 9) Lancer uniquement l'ETL (profil etl)

Le service ETL est disponible en profil séparé pour ne pas démarrer avec la stack principale.

```bash
# ETL complet (extract + transform + load)
docker compose --profile etl up etl

# Avec logs dans un fichier horodaté
mkdir -p logs
docker compose --profile etl up etl 2>&1 | tee logs/etl_$(date +%Y-%m-%d_%H-%M-%S).log

# Par pays (macOS/zsh : guillemets obligatoires)
TARGET_COUNTRIES="['FR','DE','CH']" docker compose --profile etl up etl

# Mode INCREMENTAL (conserve l'existant)
TARGET_COUNTRIES="['IT','ES']" INCREMENTAL_LOAD=true docker compose --profile etl up etl
```

## 10) Dépannage rapide

**L'API ne démarre pas** → vérifier que la DB est prête :
```bash
docker compose logs db
docker compose restart api
```

**Erreur de port déjà utilisé** → identifier le processus :
```bash
lsof -i :8000   # ou :80, :3000, :9090, :5432
```

**Rebuild forcé d'une image** :
```bash
docker compose build --no-cache api
docker compose build --no-cache frontend
```

**Réinitialisation complète** (supprime toutes les données) :
```bash
docker compose down -v --remove-orphans
docker compose up -d
```
