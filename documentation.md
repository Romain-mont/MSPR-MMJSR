# 📄 Documentation Technique du Projet ObRail Europe

---

## 1. Introduction et Page de Garde

**Titre du Projet :** Mise en œuvre d'un processus ETL pour l'Observatoire ferroviaire ObRail Europe

**Objectif :** Comparatif d'impact carbone entre l'offre "Train de Jour" et "Train de Nuit"

**Contexte académique :** MSPR TPRE612 - DIA/DIADS

**Date :** Février 2026

---

## 2. Contexte et Objectifs

### 2.1 Mission d'ObRail Europe

ObRail Europe est un observatoire fictif œuvrant pour la **promotion de la mobilité durable** et du rail comme alternative écologique à l'avion, dans le cadre du **Green Deal européen**. L'organisation s'inscrit dans une démarche de transition énergétique visant à :

- Réduire les émissions de CO₂ du secteur des transports
- Favoriser l'adoption du train pour les déplacements longue distance
- Comparer l'efficience environnementale des différentes offres ferroviaires

### 2.2 Problématique

Les données ferroviaires en Europe souffrent de plusieurs problèmes structurels :

- **Dispersion des sources** : Les données sont réparties entre opérateurs nationaux (SNCF, DB, SBB/CFF), plateformes tierces (Mobility Database, Back on Track) et formats hétérogènes (GTFS, CSV, API)
- **Hétérogénéité des formats** : Absence de standardisation complète entre les différents systèmes de données
- **Difficulté de comparaison** : Impossible de réaliser des analyses cross-border sans un processus d'unification préalable
- **Absence de métriques environnementales** : Les émissions de CO₂ ne sont pas systématiquement calculées ou disponibles

Ces contraintes rendent les analyses comparatives complexes et chronophages, nécessitant une automatisation du processus de collecte et de transformation.

### 2.3 Objectifs Techniques

Le projet vise à mettre en place une **chaîne ETL complète** permettant :

1. **Automatisation de la collecte** : 
   - Extraction des données GTFS depuis l'API Mobility Database (FR, CH, DE)
   - Extraction des données Back on Track depuis Google Sheets
   - Filtrage intelligent des sources pertinentes (trains uniquement, exclusion des transports urbains)

2. **Unification des données** :
   - Transformation des formats hétérogènes vers un schéma unique
   - Calcul automatique des distances géographiques (formule de Haversine)
   - Calcul des émissions CO₂ selon le type de train (jour/nuit)
   - Normalisation des noms de gares et des horaires

3. **Centralisation dans un entrepôt de données** :
   - Modélisation en schéma en étoile (fact & dimensions)
   - Stockage PostgreSQL conteneurisé
   - Garantie de l'intégrité référentielle

4. **Exposition des résultats** :
   - API REST développée avec FastAPI
   - Endpoints de recherche d'itinéraires
   - Retour des métriques environnementales par trajet

---

## 3. Choix Architecturaux, Techniques et RGPD

### 3.1 Architecture Globale

Le projet adopte une **architecture modulaire en pipeline ETL classique** :

```
┌─────────────────────────────────────────────────────────────┐
│                    SOURCES DE DONNÉES                        │
├──────────────────────┬──────────────────────────────────────┤
│  Mobility Database   │      Back on Track                    │
│  (API GTFS)          │      (Google Sheets CSV)              │
└──────────┬───────────┴────────────┬─────────────────────────┘
           │                        │
           ▼                        ▼
   ┌───────────────────────────────────────────┐
   │          EXTRACTION (Python)               │
   │  - extraction.py (combiné)                 │
   │  - Téléchargement ZIP GTFS                 │
   │  - Téléchargement CSV Google Sheets        │
   └────────────────┬──────────────────────────┘
                    │
                    ▼
          ┌─────────────────┐
          │   DATA/RAW/     │ (Stockage temporaire)
          └────────┬────────┘
                   │
                   ▼
   ┌───────────────────────────────────────────┐
   │       TRANSFORMATION (Python/PySpark)      │
   │  - transform_backontrack.py                │
   │  - extraction_final_transfo_mobility.py    │
   │  - Calcul distances (Haversine)            │
   │  - Calcul durées et CO₂                    │
   │  - Classification jour/nuit                │
   └────────────────┬──────────────────────────┘
                    │
                    ▼
        ┌─────────────────────┐
        │   DATA/PROCESSED/   │ (CSV unifié)
        └──────────┬──────────┘
                   │
                   ▼
   ┌───────────────────────────────────────────┐
   │          LOAD (Python + SQLAlchemy)        │
   │  - load_to_db.py                           │
   │  - Insertion dans PostgreSQL               │
   │  - Respect du schéma en étoile             │
   └────────────────┬──────────────────────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │   PostgreSQL DB      │
         │  (Docker Container)  │
         │  - dim_route         │
         │  - dim_vehicle_type  │
         │  - fact_em           │
         └──────────┬───────────┘
                    │
                    ▼
   ┌───────────────────────────────────────────┐
   │           API REST (FastAPI)               │
   │  - api/main.py                             │
   │  - Endpoint /search                        │
   │  - Retour JSON structuré                   │
   └───────────────────────────────────────────┘
```

**Principes architecturaux** :

- **Découplage des étapes** : Chaque phase (E, T, L) est un script Python indépendant
- **Idempotence** : Les scripts peuvent être relancés sans duplication de données (TRUNCATE avant insertion)
- **Traçabilité** : Logs détaillés à chaque étape du pipeline
- **Scalabilité** : Utilisation de PySpark pour le traitement des gros volumes GTFS

### 3.2 Stack Technique

#### 3.2.1 Langage et Frameworks

- **Python 3.10+** : Langage principal pour l'ensemble du pipeline
  - **Pandas** : Manipulation de données tabulaires (Back on Track)
  - **PySpark** : Traitement distribué des fichiers GTFS volumineux
  - **SQLAlchemy** : ORM pour l'interaction avec PostgreSQL
  - **FastAPI** : Framework web moderne pour l'API REST
  - **python-dotenv** : Gestion sécurisée des variables d'environnement

#### 3.2.2 Base de Données

- **PostgreSQL 16 (Alpine)** : 
  - Base de données relationnelle robuste
  - Support natif des types géographiques (future évolution possible avec PostGIS)
  - Conteneurisée via Docker pour la portabilité

**Modèle de données (Schéma en étoile)** :

```sql
-- Table de dimension : Routes (géographie)
dim_route
├── route_id (PK, SERIAL)
├── dep_name (VARCHAR) -- Gare de départ
├── arr_name (VARCHAR) -- Gare d'arrivée
├── distance_km (NUMERIC)
└── is_long_distance (BOOLEAN)

-- Table de dimension : Types de véhicules
dim_vehicle_type
├── vehicle_type_id (PK, SERIAL)
├── label (VARCHAR) -- "TGV/Intercités", "Intercités Nuit"
├── co2_vt (NUMERIC) -- Coefficient d'émission par km
└── service_type (VARCHAR) -- "Jour" / "Nuit"

-- Table de faits : Émissions
fact_em
├── fact_id (PK, SERIAL)
├── route_id (FK → dim_route)
├── vehicle_type_id (FK → dim_vehicle_type)
└── co2_kg_passenger (NUMERIC) -- Émission totale par passager
```

**Justification du schéma en étoile** :
- Optimisé pour les requêtes analytiques (agrégations, filtres)
- Séparation logique entre dimensions (contexte) et faits (métriques)
- Facilite les futures extensions (ajout de nouvelles dimensions : temps, opérateur, etc.)

#### 3.2.3 Conteneurisation

- **Docker** : Isolation des services dans des conteneurs
- **Docker Compose** : Orchestration multi-conteneurs (PostgreSQL + PgAdmin)

**Services déployés** :

```yaml
services:
  db:
    image: postgres:16-alpine
    ports: 5432
    volumes:
      - ./postgres_data (persistance)
      - ./database/init.sql (initialisation du schéma)
  
  pgadmin:
    image: dpage/pgadmin4
    ports: 5050
    interface graphique pour administrer la base
```

**Avantages** :
- Environnement reproductible sur n'importe quelle machine
- Isolation des dépendances (pas de conflit avec d'autres projets)
- Facilite le déploiement en production (cloud-ready)

#### 3.2.4 Calculs métiers

**Calcul de distance (Formule de Haversine)** :

```python
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Rayon de la Terre en km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c  # Distance en km
```

**Calcul des émissions CO₂** :

```python
def calculate_co2(distance_km, train_type):
    # Facteurs d'émission (gCO₂/passager/km)
    factor = 14.0 if train_type == "Nuit" else 4.0
    return round(distance_km * factor / 1000, 2)  # Conversion en kg
```

**Classification jour/nuit** :

```python
def determine_train_type(departure_h, duration_h):
    dep_mod = departure_h % 24
    # Train de nuit : départ entre 22h et 5h ET durée > 4h
    if (dep_mod >= 22 or dep_mod <= 5) and duration_h > 4:
        return "Nuit"
    return "Jour"
```

### 3.3 Sécurité et RGPD

#### 3.3.1 Analyse de conformité RGPD

**Données traitées** :
- Noms de gares (données publiques)
- Horaires de circulation (données publiques)
- Distances géographiques (calculées, non personnelles)
- Émissions de CO₂ (métriques environnementales)

**Conclusion** : Le projet **ne traite aucune donnée à caractère personnel** (pas de noms, prénoms, adresses emails, identifiants de passagers, historiques de réservation, etc.).

**Exemption RGPD** : Conformément à l'article 4(1) du RGPD, le projet n'entre pas dans le champ d'application du règlement car il ne manipule que des **données techniques et environnementales agrégées**.

#### 3.3.2 Bonnes pratiques de sécurité implémentées

1. **Gestion des secrets via `.env`** :

```bash
# .env (non versionné dans Git)
DB_USER=admin_rail
DB_PASSWORD=SecureP@ssw0rd2026
DB_NAME=euro_rail_db
REFRESH_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
PGADMIN_MAIL=admin@obrail.eu
PGADMIN_PASS=AdminPass2026
```

- Fichier `.env` ajouté au `.gitignore` pour éviter toute fuite
- Variables chargées dynamiquement via `python-dotenv`

2. **Protection contre les injections SQL** :

```python
# Utilisation de requêtes paramétrées avec SQLAlchemy
query = text("""
    SELECT * FROM fact_em 
    WHERE route_id = :route_id
""")
conn.execute(query, {"route_id": user_input})  # Paramètre sécurisé
```

3. **Validation des entrées API** :

```python
# FastAPI avec Pydantic pour la validation
class TrajetResponse(BaseModel):
    depart: str
    arrivee: str
    distance_km: float
    type_train: str
    co2_kg: float
```

4. **Principe du moindre privilège** :
   - Utilisateur PostgreSQL dédié avec droits limités (pas de superuser)
   - PgAdmin accessible uniquement en local (port non exposé en production)

5. **Logs sans données sensibles** :
   - Aucun mot de passe n'est affiché dans les logs
   - Seules les informations de debug techniques sont enregistrées

#### 3.3.3 Recommandations pour une mise en production

Bien que hors du cadre du projet académique, voici les mesures à implémenter en production :

- **Chiffrement des communications** : HTTPS/TLS pour l'API
- **Authentification API** : Tokens JWT ou clés API pour restreindre l'accès
- **Rotation des secrets** : Changement régulier des mots de passe
- **Surveillance des accès** : Logs d'audit PostgreSQL
- **Sauvegarde automatisée** : Backup quotidien de la base de données

---

## 4. Structure du Projet

```
MSPR/
├── api/
│   └── main.py                 # API FastAPI (endpoints REST)
├── data/
│   ├── raw/                    # Données brutes extraites
│   │   ├── mobility_gtfs/      # ZIP GTFS décompressés par provider
│   │   └── backontrack_csv/    # CSV Back on Track
│   ├── processed/              # Données transformées
│   │   └── Europe_Rail_Database.csv
│   └── staging/                # Données intermédiaires (temporaire)
├── database/
│   └── init.sql                # Schéma SQL initial (étoile)
├── extraction/
│   ├── extraction.py           # Script d'extraction combiné
│   └── extraction_csv_backontrack.py  # (Ancien, remplacé par extraction.py)
├── transformation/
│   ├── transform_backontrack.py         # Transformation Back on Track
│   └── extraction_final_transfo_mobility.py  # Transformation Mobility DB (PySpark)
├── load/
│   └── load_to_db.py           # Chargement dans PostgreSQL
├── docker-compose.yml          # Orchestration Docker
├── .env                        # Variables d'environnement (secret)
├── .gitignore                  # Exclusions Git (.env, postgres_data/)
└── documentation.md            # Ce document
```

---

## 5. Guide d'Installation et d'Exécution

### 5.1 Prérequis

- **Docker Desktop** installé et lancé
- **Python 3.10+** avec pip
- **Git** pour cloner le projet
- Connexion Internet (pour télécharger les données)

### 5.2 Installation

```bash
# 1. Cloner le dépôt
git clone <url_du_projet>
cd MSPR

# 2. Créer l'environnement virtuel Python
python3 -m venv .venv
source .venv/bin/activate  # Sur Windows : .venv\Scripts\activate

# 3. Installer les dépendances
pip install pandas pyspark sqlalchemy fastapi uvicorn python-dotenv psycopg2-binary requests

# 4. Configurer les variables d'environnement
# Créer un fichier .env à la racine avec :
DB_USER=admin_rail
DB_PASSWORD=VotreMotDePasse
DB_NAME=euro_rail_db
DB_PORT=5432
REFRESH_TOKEN=VotreTokenMobilityDatabase
PGADMIN_MAIL=admin@example.com
PGADMIN_PASS=AdminPassword

# 5. Démarrer PostgreSQL via Docker
docker-compose up -d
```

### 5.3 Exécution du Pipeline ETL

```bash
# Étape 1 : EXTRACTION
python extraction/extraction.py
# ⏱️ Durée : ~10-15 minutes (selon le nombre de providers)
# 📂 Résultat : data/raw/ rempli avec fichiers GTFS et CSV

# Étape 2 : TRANSFORMATION
# Traiter Back on Track
python transformation/transform_backontrack.py

# Traiter Mobility Database (PySpark)
python transformation/extraction_final_transfo_mobility.py
# ⏱️ Durée : ~5-20 minutes (selon le volume)
# 📂 Résultat : data/processed/Europe_Rail_Database.csv

# Étape 3 : LOAD
python load/load_to_db.py
# ⏱️ Durée : ~1-2 minutes
# ✅ Résultat : Tables PostgreSQL remplies

# Étape 4 : Lancer l'API
uvicorn api.main:app --reload
# 🌐 API accessible sur http://127.0.0.1:8000
```

### 5.4 Test de l'API

```bash
# Documentation interactive
http://127.0.0.1:8000/docs

# Requête d'exemple
curl "http://127.0.0.1:8000/search?depart=Paris&arrivee=Lyon"
```

**Réponse attendue** :

```json
[
  {
    "depart": "Paris Gare de Lyon",
    "arrivee": "Lyon Part-Dieu",
    "distance_km": 462.3,
    "type_train": "TGV/Intercités",
    "co2_kg": 1.85
  },
  {
    "depart": "Paris Bercy",
    "arrivee": "Lyon Perrache",
    "distance_km": 467.1,
    "type_train": "Intercités Nuit",
    "co2_kg": 6.54
  }
]
```

---

## 6. Résultats et Métriques

### 6.1 Volumétrie des données

- **Sources extraites** : 
  - Mobility Database : ~15-20 providers GTFS (FR, CH, DE)
  - Back on Track : 6 fichiers CSV (agencies, routes, trips, stops, calendar, trip_stop)

- **Données transformées** :
  - ~500-2000 trajets uniques (selon filtres)
  - ~200-500 paires origine-destination
  - ~2-5 types de véhicules distincts

- **Base de données finale** :
  - Table `dim_route` : ~200-500 lignes
  - Table `dim_vehicle_type` : ~2-5 lignes
  - Table `fact_em` : ~500-2000 lignes

### 6.2 Comparaison Train de Jour vs Train de Nuit

**Exemple de requête analytique** :

```sql
SELECT 
    v.label AS type_train,
    AVG(r.distance_km) AS distance_moyenne_km,
    AVG(f.co2_kg_passenger) AS co2_moyen_kg
FROM fact_em f
JOIN dim_route r ON f.route_id = r.route_id
JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
GROUP BY v.label;
```

**Résultats attendus** :

| Type de train     | Distance moyenne (km) | CO₂ moyen (kg) |
|-------------------|----------------------|----------------|
| TGV/Intercités    | 420                  | 1.68           |
| Intercités Nuit   | 650                  | 9.10           |

**Interprétation** :
- Les trains de jour émettent **4g CO₂/passager/km** (facteur optimisé)
- Les trains de nuit émettent **14g CO₂/passager/km** (moins efficaces énergétiquement en raison des wagons-lits et de la vitesse réduite)
- Toutefois, les trains de nuit couvrent généralement des **distances plus longues**, évitant ainsi des vols domestiques (30-150 kg CO₂ selon distance)

---

## 7. Limites et Axes d'Amélioration

### 7.1 Limites actuelles

1. **Couverture géographique limitée** : Seulement FR, CH, DE (extensible)
2. **Qualité des données GTFS variable** : Certains providers ont des données incomplètes
3. **Absence de données temps réel** : Uniquement des horaires théoriques
4. **Facteurs d'émission simplifiés** : Pas de prise en compte de la charge réelle des trains
5. **Pas d'interface utilisateur** : API uniquement (pas de dashboard visuel)

### 7.2 Améliorations futures

**Court terme** :
- Ajouter d'autres pays européens (IT, ES, NL, BE, AT)
- Implémenter un système de cache Redis pour l'API
- Créer un dashboard interactif (Streamlit ou React)

**Moyen terme** :
- Intégrer des données temps réel (retards, suppressions)
- Ajouter des coefficients d'émission plus précis (source ADEME/UIC)
- Implémenter un système de recommandation d'itinéraires

**Long terme** :
- Comparaison multimodale (train + avion + voiture)
- Intégration d'un module de réservation (API partenaires)
- Extension à l'échelle mondiale (Amtrak, JR East, etc.)

---

## 8. Conclusion

Ce projet démontre la faisabilité technique d'un **pipeline ETL automatisé** pour l'analyse environnementale des transports ferroviaires en Europe. 

**Réalisations principales** :
- ✅ Extraction automatisée depuis sources multiples (API + CSV)
- ✅ Transformation et unification des données hétérogènes
- ✅ Calcul automatique des métriques environnementales
- ✅ Stockage structuré dans un modèle en étoile
- ✅ Exposition via API REST documentée
- ✅ Conformité RGPD (absence de données personnelles)

**Impact attendu** :
En automatisant la collecte et la comparaison des données ferroviaires, ce projet pose les bases d'un **outil d'aide à la décision** pour :
- Les voyageurs soucieux de leur empreinte carbone
- Les décideurs publics (évaluation des politiques de mobilité)
- Les chercheurs en mobilité durable

**Alignement avec le Green Deal européen** :
Ce type d'infrastructure de données est essentiel pour atteindre les objectifs de **réduction de 55% des émissions d'ici 2030**, en rendant visible et accessible l'impact environnemental des choix de transport.

---

## 9. Références

### Sources de données
- **Mobility Database** : https://mobilitydatabase.org/
- **Back on Track** : https://back-on-track.eu/

### Standards et protocoles
- **GTFS (General Transit Feed Specification)** : https://gtfs.org/
- **Format GTFS Schedule** : https://gtfs.org/schedule/reference/

### Facteurs d'émission
- **ADEME Base Carbone** : https://bilans-ges.ademe.fr/
- **UIC (Union Internationale des Chemins de fer)** : https://uic.org/

### Technologies
- **FastAPI** : https://fastapi.tiangolo.com/
- **PostgreSQL** : https://www.postgresql.org/
- **Apache Spark** : https://spark.apache.org/
- **Docker** : https://www.docker.com/

### Réglementation
- **RGPD (Règlement Général sur la Protection des Données)** : https://eur-lex.europa.eu/eli/reg/2016/679/oj
- **Green Deal européen** : https://commission.europa.eu/strategy-and-policy/priorities-2019-2024/european-green-deal_fr

---

**Document rédigé dans le cadre du projet MSPR TPRE612**  
**Promotion DIA/DIADS 2025-2026**  
**Février 2026**
