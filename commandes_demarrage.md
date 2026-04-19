# Commandes utiles pour démarrer le projet

Ce fichier regroupe les commandes les plus utiles pour lancer le projet rapidement.

## 1) Préparer le projet (une seule fois)

```bash
cp .env.example .env
```

Puis adapter les variables dans `.env` (DB_USER, DB_PASSWORD, DB_NAME, REFRESH_TOKEN, etc.).

Construire les images Docker :

```bash
docker compose build
```

## 2) Démarrage rapide

Lancer le pipeline ETL complet (extract + transform + load) :

```bash
docker compose up etl
```

Voir les logs ETL en temps réel :

```bash
docker compose logs -f etl
```

Voir les logs ETL en temps réel + les écrire dans un fichier :

```bash
mkdir -p logs
docker compose logs -f etl | tee -a logs/etl_live.log
```

Lancer l'ETL et enregistrer toute la sortie dans un fichier horodaté :

```bash
mkdir -p logs
TARGET_COUNTRIES="['FR','DE','CH']" docker compose up etl 2>&1 | tee logs/etl_$(date +%Y-%m-%d_%H-%M-%S).log
```

Note : `TARGET_COUNTRIES` s'applique au lancement de `up/run`, pas à la commande `logs`.

## 3) Lancer seulement une partie de l'extraction (par pays)

Important sur macOS/zsh : mettre `TARGET_COUNTRIES` entre guillemets.

Extraction pour la France uniquement :

```bash
TARGET_COUNTRIES="['FR']" docker compose up etl
```

Extraction pour France + Allemagne + Suisse :

```bash
TARGET_COUNTRIES="['FR','DE','CH']" docker compose up etl
```

Extraction pour un petit lot de pays (ex: Italie + Espagne + Portugal) :

```bash
TARGET_COUNTRIES="['IT','ES','PT']" docker compose up etl
```

## 4) Lancer uniquement l'extraction (sans transform/load)

Cette commande lance seulement la phase extract de `main.py` :

```bash
docker compose run --rm --no-deps \
  -e TARGET_COUNTRIES="['FR','DE']" \
  etl python main.py --extract
```

Si tu veux aussi voir les 3 sources d'extraction en parallèle (comportement par défaut),
`--extract` utilise déjà le mode parallèle prévu dans `main.py`.

## 5) Mode RESET vs INCREMENTAL

### RESET (reconstruction propre)

Efface les tables puis recharge tout.

```bash
TARGET_COUNTRIES="['FR','DE','CH']" INCREMENTAL_LOAD=false docker compose up etl
```

### INCREMENTAL (ajout sans supprimer)

Conserve l'existant et ajoute le nouveau.

```bash
TARGET_COUNTRIES="['IT','ES','PT']" INCREMENTAL_LOAD=true docker compose up etl
```

## 6) Lancer une phase précise avec `main.py`

Extraction seule :

```bash
docker compose run --rm --no-deps etl python main.py --extract
```

Transformation seule :

```bash
docker compose run --rm --no-deps etl python main.py --transform
```

Load seul (avec nettoyage par défaut) :

```bash
docker compose run --rm etl python main.py --load
```

Load seul sans vider les tables (`--no-clean`) :

```bash
docker compose run --rm etl python main.py --load --no-clean
```

## 7) API, Dashboard, PostgreSQL

Lancer seulement la base + API + dashboard :

```bash
docker compose up db api dashboard-web
```

Tout lancer :

```bash
docker compose up
```

URLs utiles :
- API Swagger : http://localhost:8000/docs
- API Redoc : http://localhost:8000/redoc
- Dashboard : http://localhost:8505
- pgAdmin : http://localhost:5050

## 8) Arrêt et nettoyage

Arrêter les conteneurs :

```bash
docker compose down
```

Arrêter + supprimer volumes (attention, supprime les données DB locales) :

```bash
docker compose down -v
```

## 9) Exemples de workflow recommandés

### Workflow A : test rapide sur 1 pays

```bash
TARGET_COUNTRIES="['FR']" INCREMENTAL_LOAD=false docker compose up etl
```

### Workflow B : chargement progressif multi-pays

1) Initialiser sur 3 pays (RESET)

```bash
TARGET_COUNTRIES="['DE','AT','CH']" INCREMENTAL_LOAD=false docker compose up etl
```

2) Ajouter des pays (INCREMENTAL)

```bash
TARGET_COUNTRIES="['IT','ES','PT']" INCREMENTAL_LOAD=true docker compose up etl
```

```bash
TARGET_COUNTRIES="['NL','BE','LU']" INCREMENTAL_LOAD=true docker compose up etl
```

## 10) Dépannage rapide

Si une commande avec `TARGET_COUNTRIES` échoue, vérifier :
- Les guillemets autour de la liste (important en zsh)
- Le format exact : `"['FR','DE']"`
- La présence du fichier `.env`
- Le `REFRESH_TOKEN` si extraction Mobility Database

Si Spark se comporte bizarrement après un arrêt brutal,
relancer un run propre de l'ETL via :

```bash
docker compose run --rm etl python main.py --extract
```
