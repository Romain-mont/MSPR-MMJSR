# 🚄 ObRail Europe - Comparatif d'Impact Carbone Train de Jour vs Train de Nuit

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.128-009688?logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.1.1-E25A1C?logo=apache-spark&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.54-FF4B4B?logo=streamlit&logoColor=white)

**Projet MSPR TPRE612 - DIA/DIADS 2025-2026**

*Pipeline ETL pour l'analyse comparative des émissions CO₂e du transport ferroviaire européen*

[📖 Documentation](./documentation.md) • [🔌 API](./Api.md) • [🏗️ Architecture](./architecture_technique_rgpd.md) • [📊 Stack Technique](./STACK_TECHNIQUE.md)

</div>

---

## 📋 Table des Matières

- [À Propos](#-à-propos)
- [Contexte](#-contexte)
- [Architecture Globale](#-architecture-globale)
- [Stack Technique](#-stack-technique)
- [Prérequis](#-prérequis)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Utilisation](#-utilisation)
  - [Pipeline ETL Complet](#1-pipeline-etl-complet)
  - [Mode Incrémental](#2-mode-incrémental-par-pays)
  - [API REST](#3-api-rest)
  - [Dashboard Web](#4-dashboard-web-streamlit)
  - [Interface PostgreSQL](#5-interface-postgresql-pgadmin)
- [Structure du Projet](#-structure-du-projet)
- [Endpoints API](#-endpoints-api)
- [Analyse des Résultats](#-analyse-des-résultats)
- [Documentation Complémentaire](#-documentation-complémentaire)
- [Contributeurs](#-contributeurs)

---

## 🎯 À Propos

**ObRail Europe** est un observatoire fictif œuvrant pour la **promotion de la mobilité durable** et du rail comme alternative écologique à l'avion, dans le cadre du **Green Deal européen**.

Ce projet met en œuvre une **chaîne ETL complète** (Extract, Transform, Load) permettant de :

- ✅ **Extraire** des données ferroviaires multi-sources (GTFS, CSV, API)
- ✅ **Transformer** et unifier les données avec PySpark
- ✅ **Charger** dans un entrepôt PostgreSQL (star schema)
- ✅ **Exposer** via une API REST FastAPI
- ✅ **Visualiser** avec des dashboards interactifs Streamlit

### 🎓 Objectif Pédagogique

Comparer l'**impact carbone** entre :
- 🌞 **Trains de jour** (TGV, ICE, Intercités, etc.)
- 🌙 **Trains de nuit** (Intercités Nuit, Nightjet, etc.)

**Résultat clé** : Les trains de nuit émettent jusqu'à **79.7% de CO₂ en moins** sur certains trajets (ex: Lyon-Paris).

---

## 🌍 Contexte

### Problématique

Les données ferroviaires en Europe souffrent de plusieurs problèmes :

- **Dispersion des sources** : Opérateurs nationaux (SNCF, DB, SBB/CFF), plateformes tierces (Mobility Database, Back on Track)
- **Hétérogénéité des formats** : GTFS, CSV, API, Google Sheets
- **Absence de métriques environnementales** : Émissions CO₂ non standardisées

### Solution

Une architecture ETL modulaire capable de :

1. **Extraire** depuis 3 sources de données
2. **Transformer** avec Apache Spark (gestion Big Data)
3. **Charger** dans un modèle en étoile PostgreSQL
4. **Exposer** via API REST pour réutilisation
5. **Visualiser** avec dashboards décisionnels

---

## 🏗️ Architecture Globale

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                            │
├──────────────────────────────┬──────────────────────────────────────┤
│  Mobility Database (API)     │      Back on Track (Google Sheets)   │
│  • GTFS Feeds (FR, CH, DE)   │      • CSV Export                     │
│  • Format ZIP → txt files    │      • Données trajets européens      │
└──────────────┬───────────────┴──────────────┬───────────────────────┘
               │                              │
               v                              v
       ┌───────────────────────────────────────────────┐
       │         EXTRACTION (Python + PySpark)          │
       │  • extraction.py (Mobility DB + Airports)      │
       │  • Back on Track CSV                           │
       │  • Stockage : data/raw/                        │
       └──────────────────┬────────────────────────────┘
                          │
                          v
       ┌───────────────────────────────────────────────┐
       │      TRANSFORMATION (PySpark + Pandas)         │
       │  • Unification schéma GTFS                     │
       │  • Calcul distance + CO₂e                      │
       │  • Filtrage pays (TARGET_COUNTRIES)            │
       │  • Output : data/staging/final_routes.csv      │
       └──────────────────┬────────────────────────────┘
                          │
                          v
       ┌───────────────────────────────────────────────┐
       │          LOAD (PostgreSQL + SQLAlchemy)        │
       │  • Star Schema : dim_route, dim_vehicle_type,  │
       │                   fact_em                       │
       │  • Mode RESET ou INCRÉMENTAL                   │
       └──────────────────┬────────────────────────────┘
                          │
         ┌────────────────┴────────────────┐
         v                                  v
┌─────────────────────┐          ┌─────────────────────┐
│   API REST (FastAPI) │          │  Dashboard (Streamlit)│
│  • GET /data         │◄─────────┤  • Visualisations    │
│  • GET /search       │          │  • Graphiques        │
│  • GET /compare      │          │  • Statistiques      │
│  Port: 8000          │          │  Port: 8505          │
└─────────────────────┘          └─────────────────────┘
```

### Composants Docker

- **etl** : Pipeline principal (extraction + transformation + load)
- **db** : PostgreSQL 16 Alpine (entrepôt de données)
- **api** : FastAPI (exposer les données)
- **dashboard-web** : Streamlit (visualisation interactive)
- **pgadmin** : Interface graphique PostgreSQL

---

## 🛠️ Stack Technique

| Catégorie | Technologies |
|-----------|-------------|
| **Langage** | Python 3.11 |
| **Big Data** | PySpark 4.1.1, Pandas 2.3.3 |
| **Base de données** | PostgreSQL 16, SQLAlchemy 2.0.46 |
| **API** | FastAPI 0.128.1, Pydantic 2.12.5, Uvicorn 0.40.0 |
| **Visualisation** | Streamlit 1.54.0, Plotly 6.5.2, Matplotlib 3.10.8 |
| **Infrastructure** | Docker, Docker Compose |
| **Sources données** | Mobility Database API, Back on Track, OurAirports |

Voir [STACK_TECHNIQUE.md](./STACK_TECHNIQUE.md) pour la liste exhaustive.

---

## ✅ Prérequis

### Logiciels Nécessaires

- **Docker Desktop** ≥ 20.10
- **Docker Compose** ≥ 2.0
- **Git** (pour cloner le projet)
- **Minimum 8 Go RAM** (recommandé : 16 Go)

### Système Compatible

- macOS (Intel / Apple Silicon)
- Linux (Ubuntu, Debian, etc.)
- Windows 10/11 avec WSL2

### Espace Disque

- **Minimum** : 50 Go (données brutes + staging + PostgreSQL)
- **Recommandé** : 100 Go (pour l'ensemble des pays européens)

---

## 📥 Installation

### 1. Cloner le Projet

```bash
git clone <url-du-repo>
cd MSPR
```

### 2. Créer le Fichier `.env`

```bash
cp .env.example .env
```

Ou créer manuellement avec le contenu suivant :

```env
# Base de données PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mspr_db
DB_USER=mspr_user
DB_PASSWORD=mspr_password

# pgAdmin (interface graphique)
PGADMIN_MAIL=admin@example.com
PGADMIN_PASS=admin123

# API Mobility Database (optionnel si besoin refresh des données)
REFRESH_TOKEN="votre_token_mobility_db"
```

### 3. Configurer les Volumes de Données

**Important** : Les données volumineuses sont montées via volumes.

Modifier dans `docker-compose.yml` selon votre configuration :

```yaml
volumes:
  - /Volumes/Optima/MSPR_DATA/raw:/app/data/raw      # Adapter ce chemin
  - /Volumes/Optima/MSPR_DATA/staging:/app/data/staging
```

**Exemple pour Linux/Windows** :

```yaml
volumes:
  - ./data/raw:/app/data/raw           # Stockage local
  - ./data/staging:/app/data/staging
```

### 4. Construire les Images Docker

```bash
docker compose build
```

---

## ⚙️ Configuration

### Variables d'Environnement Clés

| Variable | Description | Valeur par défaut |
|----------|-------------|-------------------|
| `TARGET_COUNTRIES` | Liste des pays à traiter | `['FR','DE','CH']` |
| `INCREMENTAL_LOAD` | Mode incrémental (true/false) | `false` |
| `DB_HOST` | Hôte PostgreSQL | `db` (Docker) ou `localhost` |
| `DB_PORT` | Port PostgreSQL | `5432` |

### Modes de Chargement

#### Mode RESET (Défaut)

Efface toutes les données existantes avant chargement :

```bash
TARGET_COUNTRIES=['FR','DE','CH'] docker compose up etl
```

#### Mode INCRÉMENTAL

Conserve les données existantes et ajoute les nouvelles :

```bash
INCREMENTAL_LOAD=true TARGET_COUNTRIES=['IT','ES','PT'] docker compose up etl
```

Voir [MODE_INCREMENTAL.md](./MODE_INCREMENTAL.md) pour plus de détails.

---

## 🚀 Utilisation

### 1. Pipeline ETL Complet

#### Lancer l'Extraction, Transformation et Load

**Commande de base** (France, Allemagne, Suisse) :

```bash
docker compose up etl
```

**Avec pays spécifiques** :

```bash
TARGET_COUNTRIES=['FR','DE','AT','CH','IT'] docker compose up etl
```

**Tous les pays européens** :

```bash
TARGET_COUNTRIES=['FR','DE','AT','CH','IT','ES','PT','NL','BE','LU','DK','SE','NO','FI','PL','CZ','HU','RO','BG','HR','SI','SK'] docker compose up etl
```

#### Étapes du Pipeline

Le script `main.py` exécute automatiquement :

1. **Extraction** (parallèle)
   - Mobility Database (API GTFS)
   - Back on Track (CSV)
   - OurAirports (CSV)

2. **Transformation** (PySpark)
   - Unification des schémas
   - Calcul distance Haversine
   - Calcul CO₂e par trajet

3. **Load** (PostgreSQL)
   - Insertion dans `dim_route`, `dim_vehicle_type`, `fact_em`
   - Mode RESET ou INCRÉMENTAL

4. **Analyse** (optionnel)
   - Génération de statistiques
   - Rapport Markdown

#### Logs du Pipeline

Suivez l'exécution en temps réel :

```bash
docker compose logs -f etl
```

---

### 2. Mode Incrémental (par Pays)

Charger progressivement les données sans perdre les précédentes.

#### Plan d'Exécution Recommandé

**Étape 1** : Germanophones (reset initial)

```bash
TARGET_COUNTRIES=['DE','AT','CH'] docker compose up etl
```

**Étape 2** : Ajout pays latins (incrémental)

```bash
INCREMENTAL_LOAD=true TARGET_COUNTRIES=['IT','ES','PT'] docker compose up etl
```

**Étape 3** : Ajout Benelux + Scandinavie (incrémental)

```bash
INCREMENTAL_LOAD=true TARGET_COUNTRIES=['NL','BE','LU','DK','SE','NO'] docker compose up etl
```

**Étape 4** : Ajout Europe de l'Est (incrémental)

```bash
INCREMENTAL_LOAD=true TARGET_COUNTRIES=['PL','CZ','HU','SK','RO','BG','HR','SI'] docker compose up etl
```

#### Vérification des Données

Après chaque chargement :

```bash
docker exec -it etl-postgres psql -U mspr_user -d mspr_db -f /app/database/verification_incremental.sql
```

Ou via script shell :

```bash
./test_incremental.sh
```

---

### 3. API REST

#### Lancer l'API

**Commande autonome** :

```bash
docker compose up api
```

**Avec base de données** :

```bash
docker compose up db api
```

**Tous les services** :

```bash
docker compose up
```

#### Accéder à l'API

- **Documentation interactive** : http://localhost:8000/docs
- **ReDoc** : http://localhost:8000/redoc
- **Health check** : http://localhost:8000/

#### Tests avec cURL

**1. Vérifier le statut** :

```bash
curl http://localhost:8000/
```

**2. Récupérer toutes les données** :

```bash
curl http://localhost:8000/data
```

**3. Rechercher des trajets** :

```bash
curl "http://localhost:8000/search?depart=Lyon&arrivee=Paris"
```

**4. Filtrer par type de véhicule** :

```bash
curl "http://localhost:8000/search?depart=Berlin&arrivee=Munich&vehicle_type=ICE"
```

**5. Comparer train de jour vs train de nuit** :

```bash
curl "http://localhost:8000/compare?depart=Lyon&arrivee=Paris"
```

**Résultat exemple** :

```json
{
  "depart": "Lyon",
  "arrivee": "Paris",
  "trains_jour": {
    "moyenne_co2_kg": 17.38,
    "nombre_trajets": 6,
    "min_co2_kg": 15.2,
    "max_co2_kg": 19.5
  },
  "trains_nuit": {
    "moyenne_co2_kg": 3.52,
    "nombre_trajets": 4,
    "min_co2_kg": 3.0,
    "max_co2_kg": 4.1
  },
  "gain_ecologique_pct": 79.74,
  "recommandation": "✅ Gain significatif ! Le train de nuit émet 79.74% de CO₂ en moins."
}
```

#### Endpoints Disponibles

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| `GET` | `/` | Health check |
| `GET` | `/data` | Récupère toutes les données (pour dashboards) |
| `GET` | `/search` | Recherche trajets (depart, arrivee, vehicle_type?) |
| `GET` | `/compare` | Compare train jour vs nuit (depart, arrivee) |

Voir [Api.md](./Api.md) pour la documentation complète.

---

### 4. Dashboard Web (Streamlit)

#### Lancer le Dashboard

**Avec API** :

```bash
docker compose up db api dashboard-web
```

**Tous les services** :

```bash
docker compose up
```

#### Accéder au Dashboard

Ouvrir dans le navigateur : **http://localhost:8505**

#### Fonctionnalités

- 📊 **Graphiques interactifs** (Plotly)
- 🔍 **Filtres dynamiques** (pays, type de véhicule)
- 📈 **Distribution des émissions CO₂**
- 🗺️ **Top 15 trajets les plus longs**
- 📊 **Comparaison jour/nuit**

#### Dashboard Matplotlib (Alternative)

Pour générer des graphiques statiques :

```bash
docker compose run --rm etl python analyse/analyse_resultat.py
```

Les graphiques sont sauvegardés dans `visualization/output/`.

---

### 5. Interface PostgreSQL (pgAdmin)

#### Lancer pgAdmin

```bash
docker compose up db pgadmin
```

#### Accéder à pgAdmin

- **URL** : http://localhost:5050
- **Email** : `admin@example.com` (défini dans `.env`)
- **Mot de passe** : `admin123` (défini dans `.env`)

#### Ajouter un Serveur

1. Cliquer sur **Add New Server**
2. **General** > Name : `ObRail Europe`
3. **Connection** :
   - Host : `db` (nom du service Docker)
   - Port : `5432`
   - Database : `mspr_db`
   - Username : `mspr_user`
   - Password : `mspr_password`
4. **Save**

#### Requêtes SQL Utiles

**Statistiques générales** :

```sql
SELECT 
    (SELECT COUNT(*) FROM dim_route) as total_routes,
    (SELECT COUNT(*) FROM dim_vehicle_type) as total_types,
    (SELECT COUNT(*) FROM fact_em) as total_trajets;
```

**Top 10 routes par émissions CO₂** :

```sql
SELECT 
    r.dep_name, 
    r.arr_name, 
    r.distance_km,
    AVG(f.co2_kg_passenger) as co2_moyen
FROM fact_em f
JOIN dim_route r ON f.route_id = r.route_id
GROUP BY r.dep_name, r.arr_name, r.distance_km
ORDER BY co2_moyen DESC
LIMIT 10;
```

**Comparaison jour/nuit Lyon-Paris** :

```sql
SELECT 
    v.service_type,
    AVG(f.co2_kg_passenger) as co2_moyen,
    COUNT(*) as nb_trajets
FROM fact_em f
JOIN dim_route r ON f.route_id = r.route_id
JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
WHERE r.dep_name ILIKE '%Lyon%' AND r.arr_name ILIKE '%Paris%'
GROUP BY v.service_type;
```

---

### 6. Arrêter les Services

**Arrêt propre** :

```bash
docker compose down
```

**Arrêt avec suppression des volumes** (⚠️ perte de données PostgreSQL) :

```bash
docker compose down -v
```

**Nettoyer les images Docker** :

```bash
docker system prune -a
```

---

## 📁 Structure du Projet

```
MSPR/
├── 📄 README.md                          # Ce fichier
├── 📄 documentation.md                   # Documentation technique complète
├── 📄 Api.md                             # Documentation API REST
├── 📄 architecture_technique_rgpd.md     # Choix architecturaux et RGPD
├── 📄 STACK_TECHNIQUE.md                 # Technologies utilisées
├── 📄 MODE_INCREMENTAL.md                # Guide mode incrémental
├── 📄 CHANGELOG_INCREMENTAL.md           # Historique modifications
│
├── 🐳 docker-compose.yml                 # Orchestration des services
├── 🐳 Dockerfile                         # Image Docker pipeline ETL
├── 📄 requirements.txt                   # Dépendances Python
├── 🔐 .env                                # Configuration (ne pas commit!)
├── 🚫 .gitignore                          # Fichiers exclus de Git
│
├── 🐍 main.py                             # Point d'entrée pipeline ETL
│
├── 📂 extraction/                         # Phase 1 : Extraction
│   └── extraction.py                      # Extraction Mobility DB + Airports
│
├── 📂 transformation/                     # Phase 2 : Transformation
│   └── transform.py                       # Unification + Calcul CO₂
│
├── 📂 load/                               # Phase 3 : Load
│   └── load_to_db.py                      # Chargement PostgreSQL
│
├── 📂 database/                           # Schéma base de données
│   ├── init.sql                           # Tables dim_route, dim_vehicle_type, fact_em
│   └── verification_incremental.sql       # Requêtes de vérification
│
├── 📂 api/                                # API REST FastAPI
│   └── main.py                            # Endpoints /data, /search, /compare
│
├── 📂 visualization/                      # Dashboards
│   ├── dashboard_web.py                   # Streamlit (interactif)
│   └── dashboard.py                       # Matplotlib (statique)
│
├── 📂 analyse/                            # Scripts d'analyse
│   ├── analyse_resultat.py                # Génération rapports
│   └── rapport_analyse.md                 # Rapport généré
│
├── 📂 data/                               # Données (volumes Docker)
│   ├── raw/                               # Données brutes
│   │   ├── mobility_gtfs/                # GTFS providers (428 dossiers)
│   │   ├── backontrack_csv/              # Back on Track CSV
│   │   └── airports/                      # OurAirports CSV
│   ├── staging/                           # Données intermédiaires
│   │   └── final_routes.csv              # Sortie transformation
│   └── processed/                         # Données finales
│       └── Europe_Rail_Database.csv
│
└── 📂 postgres_data/                      # Volume PostgreSQL (auto-généré)
```

---

## 🔌 Endpoints API

### 1. Health Check

```http
GET /
```

**Réponse** :

```json
{
  "status": "online",
  "message": "Bienvenue sur l'API Euro Rail ! 🚄"
}
```

---

### 2. Récupérer Toutes les Données

```http
GET /data
```

**Description** : Retourne l'intégralité des données pour alimenter les dashboards.

**Réponse** :

```json
{
  "total_trajets": 525,
  "trajets": [
    {
      "dep_name": "Lyon (FR)",
      "arr_name": "Paris (FR)",
      "distance_km": 390.8,
      "co2_kg_passenger": 17.38,
      "vehicle_type": "TGV",
      "service_type": "jour"
    }
  ]
}
```

---

### 3. Rechercher des Trajets

```http
GET /search?depart={ville}&arrivee={ville}&vehicle_type={type}
```

**Paramètres** :

| Paramètre | Type | Obligatoire | Description |
|-----------|------|-------------|-------------|
| `depart` | string | Oui | Ville de départ (ex: "Lyon") |
| `arrivee` | string | Oui | Ville d'arrivée (ex: "Paris") |
| `vehicle_type` | string | Non | Filtre (TGV, ICE, Nightjet, etc.) |

**Exemple** :

```bash
curl "http://localhost:8000/search?depart=Lyon&arrivee=Paris&vehicle_type=TGV"
```

**Réponse** :

```json
{
  "trajets": [
    {
      "dep_name": "Lyon Part-Dieu (FR)",
      "arr_name": "Paris Gare de Lyon (FR)",
      "distance_km": 390.8,
      "co2_kg_passenger": 15.2,
      "vehicle_type": "TGV",
      "service_type": "jour"
    }
  ]
}
```

---

### 4. Comparer Train de Jour vs Train de Nuit

```http
GET /compare?depart={ville}&arrivee={ville}
```

**Paramètres** :

| Paramètre | Type | Obligatoire | Description |
|-----------|------|-------------|-------------|
| `depart` | string | Oui | Ville de départ |
| `arrivee` | string | Oui | Ville d'arrivée |

**Exemple** :

```bash
curl "http://localhost:8000/compare?depart=Lyon&arrivee=Paris"
```

**Réponse** :

```json
{
  "depart": "Lyon",
  "arrivee": "Paris",
  "trains_jour": {
    "moyenne_co2_kg": 17.38,
    "nombre_trajets": 6,
    "min_co2_kg": 15.2,
    "max_co2_kg": 19.5
  },
  "trains_nuit": {
    "moyenne_co2_kg": 3.52,
    "nombre_trajets": 4,
    "min_co2_kg": 3.0,
    "max_co2_kg": 4.1
  },
  "gain_ecologique_pct": 79.74,
  "recommandation": "✅ Gain significatif ! Le train de nuit émet 79.74% de CO₂ en moins."
}
```

---

## 📊 Analyse des Résultats

### Générer un Rapport Automatique

```bash
docker compose run --rm etl python analyse/analyse_resultat.py
```

### Contenu du Rapport

Le script génère :

- **Statistiques globales** (nombre de trajets, moyenne CO₂)
- **Distribution des émissions** (histogramme)
- **Top 15 trajets les plus longs** (graphique horizontal)
- **Répartition par type de véhicule** (camembert)

### Lire le Rapport

```bash
cat analyse/rapport_analyse.md
```

Ou consulter les graphiques dans `visualization/output/`.

---

## 📚 Documentation Complémentaire

### Documents Disponibles

| Document | Description |
|----------|-------------|
| [documentation.md](./documentation.md) | Documentation technique complète (50+ pages) |
| [Api.md](./Api.md) | Spécifications API REST avec exemples |
| [architecture_technique_rgpd.md](./architecture_technique_rgpd.md) | Choix d'architecture et conformité RGPD |
| [STACK_TECHNIQUE.md](./STACK_TECHNIQUE.md) | Liste exhaustive des technologies |
| [MODE_INCREMENTAL.md](./MODE_INCREMENTAL.md) | Guide mode incrémental par pays |
| [CHANGELOG_INCREMENTAL.md](./CHANGELOG_INCREMENTAL.md) | Historique des modifications |

### Guides Spécifiques

- **Ajout de nouveaux pays** : Voir [MODE_INCREMENTAL.md](./MODE_INCREMENTAL.md)
- **Dépannage port 5432** : Voir [DEBUG_LYON_PARIS.md](./DEBUG_LYON_PARIS.md)
- **Vérification des données** : `database/verification_incremental.sql`

---

## 🐛 Dépannage

### Port 5432 déjà utilisé

**Problème** : PostgreSQL local bloque le port Docker.

**Solution** :

```bash
# Identifier le processus
lsof -i :5432

# Tuer le processus local
sudo kill <PID>

# Ou utiliser un autre port dans docker-compose.yml
ports:
  - "5433:5432"
```

### Erreur de Mémoire PySpark

**Problème** : `OutOfMemoryError` lors de la transformation.

**Solution** :

1. Augmenter la RAM Docker (≥ 8 Go)
2. Utiliser le mode incrémental avec moins de pays

### Données Manquantes

**Vérifier les volumes** :

```bash
docker compose run --rm etl ls -lh /app/data/raw
```

**Réextraire les données** :

```bash
docker compose run --rm etl python -c "from extraction.extraction import *; run_extraction()"
```

---

## 👥 Contributeurs

**Projet académique MSPR TPRE612**  
DIA/DIADS 2025-2026  
EPSI

---

## 📄 Licence

Projet à but pédagogique uniquement.

Les données proviennent de :
- [Mobility Database](https://database.mobilitydata.org/)
- [Back on Track](https://back-on-track.eu/)
- [OurAirports](https://ourairports.com/)

---

<div align="center">

**🚄 Pour une mobilité ferroviaire durable en Europe 🌱**

</div>
