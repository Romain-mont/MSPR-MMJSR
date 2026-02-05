# Script d’extraction OurAirports (airports.csv)

Ce script télécharge **airports.csv** depuis https://ourairports.com/data/.
L’idée est d’avoir une **exécution automatique tous les jours** pour garder une copie datée du fichier.

## 1) Prérequis
- Windows
- Python 3.8+ installé

## 2) Lancer une fois (test)
Depuis ce dossier, exécuter :

```
python download_airports.py
```

Résultat : un fichier sera créé dans le dossier `data/`, par exemple :

```
data/airports_2026-02-05.csv
```

## 3) Exécution automatique quotidienne (Windows Task Scheduler)
Le Planificateur de tâches lance le script **tout seul** chaque jour, à l’heure choisie.

1. Ouvrir **Planificateur de tâches** (Task Scheduler)
2. **Créer une tâche de base**
3. Nom : `OurAirports daily`
4. Déclencheur : **Tous les jours** (choisir l’heure souhaitée)
5. Action : **Démarrer un programme**
6. Programme/script :
   - `python` (ou le chemin complet vers python si besoin)
7. Ajouter des arguments :
   - `download_airports.py`
8. Démarrer dans :
   - Le chemin du dossier (celui qui contient `download_airports.py`)
9. (Optionnel) Cocher **Exécuter même si l’utilisateur n’est pas connecté** pour que ce soit 100% automatique.

## 4) Options utiles
- Choisir un dossier de sortie :
  ```
  python download_airports.py --output C:\chemin\vers\dossier
  ```
- Réécrire le fichier du jour :
  ```
  python download_airports.py --overwrite
  ```

## 5) Où se trouvent les fichiers ?
Par défaut : `data/airports_YYYY-MM-DD.csv`

---
Si tu veux, je peux aussi ajouter :
- un journal de logs (fichier .log)
- un nettoyage automatique (garder X jours)
