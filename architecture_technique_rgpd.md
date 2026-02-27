# 🏗️ Choix Architecturaux, Techniques et RGPD

**Projet :** ObRail Europe - Comparatif d'impact carbone Train de Jour vs Train de Nuit  
**Contexte :** MSPR TPRE612 - DIA/DIADS 2025-2026  
**Date :** Février 2026

---

## 3.1 Architecture Globale

### Vue d'ensemble

Le projet adopte une **architecture modulaire en pipeline ETL** suivant le flux :  
**Sources → Extraction → Transformation → Load → PostgreSQL → API REST → Dashboard**

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
       │          PHASE 1 : EXTRACTION                  │
       ├───────────────────────────────────────────────┤
       │  • extraction/extraction.py (Mobility DB)     │
       │  • extraction/extraction_csv_backontrack.py   │
       │                                               │
       │  Stockage intermédiaire : data/staging/       │
       └───────────────┬───────────────────────────────┘
                       │
                       v
       ┌───────────────────────────────────────────────┐
       │        PHASE 2 : TRANSFORMATION                │
       ├───────────────────────────────────────────────┤
       │  • transformation/transform.py (PySpark)      │
       │  • transformation/transform_backontrack.py    │
       │                                               │
       │  Actions :                                    │
       │   - Nettoyage et dédoublonnage                │
       │   - Calcul distances (Haversine)              │
       │   - Attribution coefficients CO₂              │
       │   - Normalisation des noms de gares           │
       │                                               │
       │  Sortie : data/processed/*.csv                │
       └───────────────┬───────────────────────────────┘
                       │
                       v
       ┌───────────────────────────────────────────────┐
       │          PHASE 3 : LOAD (CHARGEMENT)           │
       ├───────────────────────────────────────────────┤
       │  • load/load_to_db.py (SQLAlchemy)            │
       │                                               │
       │  Stratégie : TRUNCATE + INSERT                │
       │  (idempotence garantie)                       │
       └───────────────┬───────────────────────────────┘
                       │
                       v
       ┌───────────────────────────────────────────────┐
       │       BASE DE DONNÉES PostgreSQL 16            │
       ├───────────────────────────────────────────────┤
       │  Modèle en étoile (Star Schema) :            │
       │   • dim_route (géographie)                    │
       │   • dim_vehicle_type (matériel)               │
       │   • fact_em (mesures CO₂)                     │
       │                                               │
       │  Conteneurisée : Docker + PostgreSQL Alpine   │
       └───────────────┬───────────────────────────────┘
                       │
                       v
       ┌───────────────────────────────────────────────┐
       │           API REST (FastAPI)                   │
       ├───────────────────────────────────────────────┤
       │  Endpoints :                                  │
       │   • GET /search?depart=X&arrivee=Y            │
       │   → Retourne émissions CO₂ par type de train  │
       │                                               │
       │  Formats : JSON                               │
       └───────────────┬───────────────────────────────┘
                       │
                       v
       ┌───────────────────────────────────────────────┐
       │            DASHBOARD / CLIENT                  │
       ├───────────────────────────────────────────────┤
       │  • Visualisation comparée Jour/Nuit           │
       │  • Graphiques d'impact environnemental        │
       └───────────────────────────────────────────────┘
```

### Principes de conception

#### 1. **Modularité**
Chaque étape du pipeline est **indépendante et réutilisable** :
- Les scripts d'extraction peuvent tourner sans les transformations
- Les transformations peuvent être rejouées sur des données existantes
- Le chargement en base est découplé de l'API

**Avantage :** Facilite la maintenance, le débogage et l'évolution du système.

#### 2. **Idempotence**
Les scripts sont **reproductibles sans effet de bord** :
- Stratégie `TRUNCATE` avant chaque insertion (évite les doublons)
- Les extractions écrasent les fichiers staging existants
- Pas de dépendance à l'état précédent du système

**Avantage :** Permet de relancer le pipeline complet en cas d'erreur sans corruption de données.

#### 3. **Séparation des responsabilités**
Chaque module a un rôle unique et bien défini :
- `extraction/` → Collecte des données brutes
- `transformation/` → Logique métier et calculs
- `load/` → Persistance en base
- `api/` → Exposition des résultats

**Avantage :** Respect du principe SOLID (Single Responsibility), code plus maintenable.

#### 4. **Traçabilité**
Logs détaillés à chaque étape :
```python
# Exemple dans load_to_db.py
print(f"✅ Insertion réussie : {len(df)} lignes insérées dans {table_name}")
```

**Avantage :** Débogage facilité, monitoring de la qualité des données.

---

## 3.2 Stack Technique

### 3.2.1 Langage Principal : Python 3.10+

**Justification :** Python est l'écosystème de référence pour la Data Science et l'ETL grâce à :
- Richesse des bibliothèques (Pandas, PySpark, SQLAlchemy)
- Simplicité de syntaxe et rapidité de développement
- Large communauté et documentation abondante

#### Bibliothèques utilisées

**Extraction**
- **`requests`** (v2.31.0+)
  - Appels HTTP vers l'API Mobility Database
  - Téléchargement des fichiers GTFS (ZIP)
  - Parsing de CSV depuis Google Sheets (Back on Track)

**Transformation**
- **`pandas`** (v2.0.0+)
  - Manipulation de DataFrames pour Back on Track
  - Nettoyage, filtrage et agrégations
  - Export vers CSV

- **`pyspark`** (v3.5.0+)
  - Traitement distribué des fichiers GTFS volumineux
  - Meilleure performance que Pandas sur plusieurs Go de données
  - Calculs parallèles (distances Haversine, jointures)

**Load (Chargement)**
- **`sqlalchemy`** (v2.0.0+)
  - ORM (Object-Relational Mapping) pour abstraction SQL
  - Gestion de la connexion PostgreSQL
  - Exécution de requêtes sécurisées (évite les injections SQL)

- **`psycopg2-binary`** (v2.9.9+)
  - Driver PostgreSQL bas niveau
  - Utilisé en arrière-plan par SQLAlchemy

**API**
- **`fastapi`** (v0.109.0+)
  - Framework moderne pour APIs REST
  - Validation automatique des paramètres (Pydantic)
  - Documentation interactive auto-générée (Swagger UI)

- **`uvicorn`** (v0.27.0+)
  - Serveur ASGI haute performance
  - Support de requêtes asynchrones

**Configuration et Sécurité**
- **`python-dotenv`** (v1.0.0+)
  - Chargement des variables d'environnement depuis `.env`
  - Évite de hardcoder les secrets dans le code

### 3.2.2 Base de Données : PostgreSQL 16

**Choix de PostgreSQL** :
- **Standard de l'industrie** pour les entrepôts de données analytiques
- **Performance** : Optimisations pour les jointures et agrégations
- **Intégrité des données** : Contraintes `FOREIGN KEY`, `UNIQUE`, `NOT NULL`
- **Extensions géospatiales** : PostGIS (non utilisé ici, mais disponible pour évolutions futures)

**Version utilisée :** PostgreSQL 16 Alpine
- Image Docker officielle légère (~240 MB vs ~380 MB pour la version standard)
- Sécurité renforcée (Alpine Linux minimal)

#### Modèle de données (Star Schema)

Le schéma suit une **modélisation en étoile** optimisée pour l'analyse OLAP :

```sql
-- DIMENSION : Routes (géographie)
dim_route
├── route_id (PK, SERIAL)          ← Clé primaire auto-incrémentée
├── dep_name (VARCHAR)             ← Gare de départ
├── arr_name (VARCHAR)             ← Gare d'arrivée
├── distance_km (NUMERIC)          ← Distance géographique
├── is_long_distance (BOOLEAN)     ← Flag longue distance (> 300 km)
└── UNIQUE(dep_name, arr_name)     ← Prévient les doublons

-- DIMENSION : Types de véhicules
dim_vehicle_type
├── vehicle_type_id (PK, SERIAL)
├── label (VARCHAR)                ← "TGV/Intercités", "Intercités Nuit"
├── co2_vt (NUMERIC)               ← Coefficient CO₂ par voyageur⋅km
└── service_type (VARCHAR)         ← "Jour" / "Nuit"

-- FAIT : Émissions calculées
fact_em
├── fact_id (PK, SERIAL)
├── route_id (FK → dim_route)      ← Relation vers la géographie
├── vehicle_type_id (FK → dim_vehicle_type) ← Relation vers le type de train
└── co2_kg_passenger (NUMERIC)     ← Émission totale pour le trajet
```

**Avantages du modèle en étoile** :
- **Requêtes simplifiées** : Jointures directes entre fait et dimensions
- **Performance** : Index sur les clés primaires et étrangères
- **Évolutivité** : Ajout facile de nouvelles dimensions (météo, prix, etc.)

#### Configuration Docker

Le fichier `docker-compose.yml` orchestre deux services :

**Service PostgreSQL** :
```yaml
db:
  image: postgres:16-alpine
  environment:
    POSTGRES_USER: ${DB_USER}         # Depuis .env
    POSTGRES_PASSWORD: ${DB_PASSWORD}
    POSTGRES_DB: ${DB_NAME}
  volumes:
    - ./postgres_data:/var/lib/postgresql/data  # Persistance des données
    - ./database/init.sql:/docker-entrypoint-initdb.d/init.sql  # Init automatique
```

**Service PgAdmin (optionnel)** :
```yaml
pgadmin:
  image: dpage/pgadmin4
  environment:
    PGADMIN_DEFAULT_EMAIL: ${PGADMIN_MAIL}
    PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_PASS}
  ports:
    - "5050:80"  # Interface web accessible sur http://localhost:5050
```

### 3.2.3 Conteneurisation : Docker & Docker Compose

**Justification du choix Docker** :

1. **Portabilité** :
   - Environnement identique en développement, test et production
   - Évite les problèmes "ça marche sur ma machine"

2. **Isolation** :
   - PostgreSQL tourne dans son propre conteneur
   - Pas de conflit avec d'autres installations locales

3. **Reproductibilité** :
   - Une seule commande pour lancer l'infrastructure :
     ```bash
     docker-compose up -d
     ```

4. **Versioning** :
   - Images taggées (`postgres:16-alpine`) garantissent la stabilité

**Fichiers de configuration** :
- `docker-compose.yml` : Orchestration des services
- `.env` : Variables d'environnement (credentials, ports)
- `.gitignore` : Exclut `postgres_data/` et `.env` du versioning Git

---

## 3.3 Sécurité et RGPD

### 3.3.1 Analyse des Données Traitées

**Nature des données** :
Le projet traite **exclusivement des données non personnelles** :
- ✅ **Noms de gares** (infrastructure publique)
- ✅ **Horaires de train** (information publique)
- ✅ **Distances géographiques** (calculs mathématiques)
- ✅ **Coefficients d'émission CO₂** (constantes physiques)

❌ **Aucune donnée personnelle** :
- Pas de noms de passagers
- Pas d'adresses email
- Pas de numéros de téléphone
- Pas de données de localisation en temps réel d'individus
- Pas d'historique d'achats

### 3.3.2 Conformité RGPD

**Article 4(1) du RGPD** définit une donnée personnelle comme :  
> "Toute information se rapportant à une personne physique identifiée ou identifiable"

**Conclusion** : Les données du projet ObRail Europe **ne tombent pas sous le coup du RGPD** car elles ne permettent pas d'identifier directement ou indirectement une personne physique.

**Justification détaillée** :

| Type de donnée        | Stockée ? | Personnel ? | Justification                                          |
|-----------------------|-----------|-------------|--------------------------------------------------------|
| Nom de gare           | ✅ Oui    | ❌ Non      | Infrastructure publique (ex: "Gare de Lyon")          |
| Coordonnées GPS       | ✅ Oui    | ❌ Non      | Position de la gare, pas d'un individu                 |
| Horaires de passages  | ✅ Oui    | ❌ Non      | Information publique des opérateurs ferroviaires       |
| Type de train         | ✅ Oui    | ❌ Non      | Caractéristique du matériel roulant                    |
| Coefficient CO₂       | ✅ Oui    | ❌ Non      | Constante physique (ex: 0.0029 kg/voyageur⋅km)        |
| Nom de passager       | ❌ Non    | ✅ Oui      | Non collecté ni stocké                                 |
| Email / Téléphone     | ❌ Non    | ✅ Oui      | Non collecté ni stocké                                 |

### 3.3.3 Bonnes Pratiques de Sécurité Appliquées

Bien que les données ne soient pas personnelles, le projet respecte les **principes de sécurité standard** :

#### 1. **Gestion des Secrets**

**Problème** : Les credentials PostgreSQL (user, password) ne doivent jamais être exposés dans le code source.

**Solution** : Utilisation d'un fichier `.env` **exclus du Git** :

```bash
# Fichier .env (non versionné)
DB_USER=admin_rail
DB_PASSWORD=root
DB_NAME=euro_rail_db
```

```python
# Chargement sécurisé dans le code
from dotenv import load_dotenv
import os

load_dotenv()
db_user = os.getenv("DB_USER")  # Récupération depuis l'environnement
```

**Protection Git** :
```gitignore
# .gitignore
.env
.env.local
*.env
**/.env
```

#### 2. **Prévention des Injections SQL**

**Risque** : Utiliser des concaténations de strings pour construire des requêtes SQL expose à des attaques.

**Solution** : Paramétrage des requêtes via SQLAlchemy :

❌ **Mauvaise pratique** :
```python
query = f"SELECT * FROM dim_route WHERE dep_name = '{user_input}'"
# Vulnérable à : user_input = "Paris'; DROP TABLE dim_route; --"
```

✅ **Bonne pratique** :
```python
from sqlalchemy import text

query = text("""
    SELECT * FROM dim_route 
    WHERE dep_name = :depart AND arr_name = :arrivee
""")
result = conn.execute(query, {"depart": user_input, "arrivee": user_input2})
```

#### 3. **Isolation des Services**

**Docker Network** : Les conteneurs communiquent via un réseau interne isolé :
- L'API Python appelle PostgreSQL via `localhost:5432`
- Pas d'exposition publique de la base de données

**Principe du moindre privilège** :
- L'utilisateur PostgreSQL `admin_rail` a uniquement les droits nécessaires (pas de `SUPERUSER`)

#### 4. **Logs et Traçabilité**

Documentation de toutes les opérations :
```python
# Exemple dans load/load_to_db.py
print(f"[INFO] Connexion à la base {DB_NAME} en tant que {DB_USER}")
print(f"[SUCCESS] {len(df_routes)} routes insérées")
```

**Avantage** : Détection rapide d'anomalies ou d'erreurs.

#### 5. **Validation des Entrées API**

FastAPI + Pydantic garantissent la validation automatique :
```python
@app.get("/search")
def search_route(depart: str, arrivee: str):
    # depart et arrivee sont automatiquement validés comme strings
    # Rejet automatique si types incorrects
```

### 3.3.4 Recommandations pour une Mise en Production

Si le projet devait évoluer vers un environnement de production, les mesures suivantes seraient nécessaires :

1. **HTTPS obligatoire** : Chiffrement des communications avec certificat SSL/TLS
2. **Authentification API** : Tokens JWT ou API Keys pour limiter les accès
3. **Rate Limiting** : Protection contre les abus (ex: max 100 requêtes/minute)
4. **Backup automatisé** : Sauvegarde quotidienne de PostgreSQL
5. **Monitoring** : Logs centralisés (ELK Stack) et alertes (Prometheus)
6. **Mise à jour régulière** : Patch de sécurité des images Docker

---

## 3.4 Avantages de l'Architecture Proposée

| Critère               | Avantage                                                                 |
|-----------------------|--------------------------------------------------------------------------|
| **Maintenabilité**    | Modules indépendants, code clair et documenté                           |
| **Scalabilité**       | PySpark permet le traitement de millions de lignes                       |
| **Fiabilité**         | Idempotence, validation des données, contraintes en base                 |
| **Portabilité**       | Docker garantit le même environnement partout                            |
| **Performance**       | PostgreSQL optimisé, index sur clés, schéma en étoile                    |
| **Sécurité**          | Gestion des secrets, prévention injections SQL, isolation réseau         |
| **Évolutivité**       | Ajout facile de nouvelles sources (APIs aéroportuaires, bus, etc.)      |

---

## Conclusion

L'architecture proposée pour ObRail Europe repose sur des **standards de l'industrie** (Python, PostgreSQL, Docker) et suit les **meilleures pratiques du génie logiciel** (modularité, idempotence, séparation des responsabilités).

Le choix d'un pipeline ETL classique combiné à une modélisation en étoile garantit :
- ✅ **Simplicité de maintenance**
- ✅ **Performance des requêtes analytiques**
- ✅ **Reproductibilité de l'environnement**

Concernant le **RGPD**, l'absence de données personnelles exempte le projet d'obligations lourdes, mais les **bonnes pratiques de sécurité** restent appliquées (gestion des secrets, paramétrage SQL, isolation).

Cette architecture est **prête pour une mise en production** avec quelques ajustements (HTTPS, authentification, monitoring).

---

**Références techniques :**
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [PostgreSQL Official Docs](https://www.postgresql.org/docs/)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [RGPD - Texte officiel](https://eur-lex.europa.eu/eli/reg/2016/679/oj)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
