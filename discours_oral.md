

### 1) Slide Load - Injection PostgreSQL (environ 2 minutes)

Dans cette phase, notre rôle est de rendre les données exploitables dans un datamart.

Concrètement, le script [load/load_to_db.py](load/load_to_db.py) lit le fichier de staging qui est une zone tampon entre la transformation et la base. Dans notre cas, c'est l'endroit où nous posons les fichiers intermédiaires, dont `final_routes.csv`.
Ce dossier est important pour trois raisons :
1. Nous pouvons contrôler la qualité avant insertion.
2. Nous pouvons rejouer seulement la phase Load sans relancer toute l'extraction.
3. Nous gardons une trace claire de ce qui a été chargé dans la base 

Ensuite , il  alimente trois tables principales :
la dimension route,
la dimension type de véhicule,
et la table de faits des émissions.

Nous utilisons SQLAlchemy avec psycopg2,
et ce choix est volontaire.

psycopg2 est le driver PostgreSQL  :
il gère la connexion de façon fiable et performante.

SQLAlchemy ajoute une couche plus pratique :
il simplifie les transactions,
la gestion des connexions,
et l'écriture des requêtes SQL paramétrées.

Donc cela  nous donne la robustesse de connexion, et de  la lisibilité et la maintenabilité.


Nous avons mis l'accent sur la sécurité d'exécution : 
Le script vérifie dès le début les variables d'environnement de connexion.

MAis également sur  l'idempotence.
Pour ce faire on a deux modes.

En mode RESET,
on vide les tables avec `TRUNCATE TABLE ... RESTART IDENTITY CASCADE`.
Le but est de repartir d'un état totalement propre.

`TRUNCATE` supprime très vite toutes les lignes,
`RESTART IDENTITY` remet les compteurs d'identifiants à zéro,
et `CASCADE` gère les dépendances entre tables liées par clés étrangères.

Donc en mode RESET,
nous effaçons l'ancien chargement,
puis nous rechargeons tout depuis le staging.
Ce mode est idéal quand nous voulons reconstruire une base propre,
par exemple après une évolution du pipeline.

En mode INCREMENTAL,

on garde les données existantes,
et on insère uniquement les nouvelles lignes.


Concrètement, nous comparons les routes du CSV de staging
avec les routes déjà présentes dans `dim_route`.
Ensuite, nous gardons seulement les routes absentes en base,
et ce sont uniquement ces lignes qui sont insérées.

Ce mode est utile pour les mises à jour régulières,
car il évite de tout recharger,
et il réduit le temps d'exécution.



Troisième point clé : la qualité des données avant insertion.
Le script supprime les lignes sans distance ou sans CO2.

On normalise aussi certaines colonnes,
par exemple `train_type` renommée en `vehicule_type`,
pour garder un schéma cohérent.

Enfin, la phase load est structurée dans un ordre logique :
on charge d'abord les dimensions,
puis la table de faits,
avec les clés qui relient les entités.

---

### 2) Slide API REST - FastAPI (environ 1 minute 45)

Une fois la base prête,
on expose les données via l'API 

L'API a trois endpoints principaux :
- `/data` pour alimenter le dashboard,
- `/search` pour chercher des trajets,
- `/compare` pour comparer train de jour et train de nuit.

Le choix de FastAPI est important,
car il apporte du typage fort avec Pydantic.
Dans le code, on voit des modèles comme `DataResponse` et `CompareResponse`.
Ça veut dire qu'on formalise ce que l'API doit renvoyer,
et on limite les réponses incohérentes.

Autre point concret à dire :
la validation des paramètres.
Par exemple, sur `/data`, le paramètre `limit` est converti en entier,
ce qui réduit le risque d'injection dans la requête SQL.

Sur `/search`, la requête SQL utilise des paramètres nommés,
et non une concaténation brute de chaînes.
Donc c'est plus sécurisé.

En cas de problème,
l'API renvoie des erreurs HTTP propres,
par exemple 404 si aucun trajet n'est trouvé,
ou 500 en cas d'erreur interne.

Le résultat,
c'est une couche d'accès stable entre la base et les interfaces.


Donc ici, l'API joue le rôle de  centralisation de la logique métier.

---

### 3) Slide Dashboard - Streamlit (environ 1 minute 25)

La dernière étape,
c'est la restitution avec Streamlit 

Le dashboard ne lit pas directement la base.
Il appelle l'API,
récupère un JSON,
puis transforme ces données pour les visualisations.

Dans le code, on a une fonction de chargement avec cache,
ce qui évite de solliciter l'API à chaque interaction utilisateur.
Le cache améliore la fluidité,
et réduit la charge côté serveur.

Ensuite, on applique des contrôles simples mais importants :
conversion numérique des colonnes,
suppression des valeurs incohérentes,
et création de variables utiles comme :
- le mode de transport (Train ou Avion),
- le type de train (Jour ou Nuit),
- le CO2 par kilomètre.

Ces étapes permettent d'afficher des graphes lisibles et comparables :
barres, nuages de points, comparatif train vs avion,
et mise en évidence des routes où le gain carbone est le plus fort.

