# Questions / Reponses techniques (jury)

## 1) Vision globale du pipeline

Q1. Quelle fonction orchestre tout le pipeline ETL ?
R. Le point d'entree est la fonction main dans main.py. Elle route vers run_parallel_pipeline ou run_sequential_pipeline. Ces fonctions enchainent extraction, transformation, load, puis analyse/visualisation.

Q2. Pourquoi avoir separe extraction, transformation et load ?
R. Pour decoupler les responsabilites :
- extraction/extraction.py = recuperer les sources brutes,
- transformation/transform.py = unifier + calculer distance et CO2,
- load/load_to_db.py = charger le datamart PostgreSQL.
Ce decouplage facilite le debug et la relance partielle.

Q3. Comment lancez-vous une etape seule ?
R. Via les flags CLI de main.py :
- --extract
- --transform
- --load

Q4. Pourquoi l'extraction est en parallele ?
R. run_extraction_all_parallel utilise ThreadPoolExecutor pour lancer simultanement Mobility, Back on Track et Airports. Les 3 sources etant independantes, cela reduit le temps total.

Q5. Comment evitez-vous les problemes de sessions Spark zombies ?
R. Avec hard_stop_spark dans main.py avant/apres transformation. Cela stoppe SparkSession/SparkContext et nettoie les variables Py4J.

---

## 2) Questions ETL PySpark

Q6. Quelle fonction calcule la distance geodesique ?
R. haversine_distance dans transformation/transform.py.

Q7. Ou est calcule le CO2 ?
R. co2_kg dans transformation/transform.py. Le facteur est choisi selon vehicule_type et tranche de distance pour l'avion.

Q8. Ou est geree la limite de providers GTFS ?
R. MAX_MOBILITY_PROVIDERS dans transformation/transform.py (via variable d'environnement). Elle est appliquee dans read_all_mobility lors du parcours des dossiers providers.

Q9. Comment est construit un trajet Mobility (origine/destination) ?
R. Dans read_mobility_provider :
- filtrage des routes rail,
- join routes-trips,
- join stop_times,
- calcul min/max stop_sequence par trip,
- extraction de la premiere et derniere gare,
- join avec stops pour recuperer noms et coordonnees.

Q10. Pourquoi vous ecrivez/rechargez souvent des CSV intermediaires dans transform ?
R. Pour casser la lineage Spark, limiter la RAM et eviter les accumulations de plans tres longs. C'est une strategie de stabilisation pour gros volumes.

Q11. Pourquoi utiliser persist(StorageLevel.DISK_ONLY) ?
R. Pour forcer le cache sur disque plutot qu'en memoire et reduire le risque d'Out Of Memory lors des jointures lourdes.

Q12. A quoi sert validate_routes ?
R. A supprimer les trajets invalides : origine/destination nulles, vides, ou identiques.

Q13. A quoi sert normalize_station_col ?
R. A harmoniser les noms de gares (trim, normalisation d'espaces, retrait de suffixes entre parentheses/crochets) afin d'ameliorer les correspondances.

Q14. Comment detectez-vous train jour vs train nuit ?
R. Deux voies :
- classification par code de ligne (EuroNight, Nightjet, etc.),
- heuristique horaire depart (22h-6h) dans read_mobility_provider.

Q15. Quelles sources alimentent les trajets rail ?
R. read_all_mobility (GTFS Mobility Database) + read_backontrack (CSV Back on Track), puis unionByName.

---

## 3) Questions sur les JOIN (important jury)

Q16. Donnez un exemple de INNER JOIN dans votre code et pourquoi.
R. Dans read_mobility_provider : trips join rail_routes sur route_id en inner. Objectif : ne garder que les trips appartenant a des routes rail valides.

Q17. Donnez un exemple de LEFT JOIN et pourquoi.
R. Dans read_backontrack : trips join routes en left puis enrichissement avec stops_coords en left. Objectif : conserver le trajet meme si certaines coordonnees de station manquent.

Q18. Pourquoi pas tout en INNER JOIN ?
R. Un inner sur les tables de coordonnees ferait perdre des trajets utiles. On reserve inner aux cas ou la relation est obligatoire pour la validite metier.

Q19. Quel join permet la comparaison train vs avion sur meme corridor ?
R. Dans generate_plane_routes :
- pairs (origin, destination) est join en inner avec les liens intermodaux origine et destination.
Cela garantit que l'avion est genere uniquement pour des corridors existants cote rail et relies a des aeroports.

Q20. Comment evitez-vous une jointure cartesienne station-aeroport massive ?
R. Dans build_intermodal_links :
- prefiltre geographique avec bounding box (_bbox_join_cond),
- broadcast des aeroports,
- puis calcul haversine + filtre rayon <= 50 km.

Q21. Pourquoi prendre l'aeroport le plus proche par gare ?
R. Toujours dans build_intermodal_links : groupBy station_name + min(distance) puis join de reprise. Cela force un mapping 1 gare -> 1 aeroport de reference.

Q22. Quelle difference entre join Spark et merge Pandas dans votre projet ?
R. Spark est utilise en transformation big data (DataFrame distribue). Pandas merge est utilise en load pour mapper vers les IDs SQL et gerer l'incremental.

---

## 4) Questions SQL / Datamart

Q23. Quel est votre modele de donnees ?
R. Un schema en etoile simple :
- dim_route,
- dim_vehicle_type,
- fact_em.
Defini dans database/init.sql.

Q24. Quelle est la mesure principale en table de faits ?
R. co2_kg_passenger dans fact_em.

Q25. Quelles cles et contraintes sont posees ?
R.
- PK sur chaque table,
- FK fact_em.route_id -> dim_route.route_id,
- FK fact_em.vehicle_type_id -> dim_vehicle_type.vehicle_type_id,
- contrainte UNIQUE(dep_name, arr_name) dans dim_route.

Q26. Pourquoi UNIQUE(dep_name, arr_name) ?
R. Pour eviter de dupliquer une meme route logique et stabiliser les correspondances lors du chargement incremental.

Q27. Comment gerez-vous le mode incremental ?
R. Dans run_ingestion (load_to_db.py) :
- lecture des dimensions existantes,
- merge avec indicator,
- insertion uniquement des left_only.

Q28. Comment construisez-vous la table de faits ?
R. Dans run_ingestion :
- merge du CSV transforme avec dim_route (origin/destination -> route_id),
- merge avec dim_vehicle_type (vehicule_type -> vehicle_type_id),
- projection finale route_id, vehicle_type_id, co2_kg_passenger.

Q29. Quelle requete alimente le dashboard ?
R. Endpoint /data dans api/main.py : jointure SQL fact_em + dim_route + dim_vehicle_type avec selection des champs metier.

Q30. Pourquoi utiliser text() SQLAlchemy dans l'API ?
R. Pour executer des requetes SQL parametrees de maniere explicite et limiter le risque d'injection sur les endpoints recherches.

---

## 5) Questions train vs avion (corridor metier)

Q31. Comment rendez-vous train et avion comparables sur la meme route ?
R. En harmonisant la cle metier origin/destination sur le corridor gare/ville. Les noms d'aeroports sont gardes en colonnes separees (airport_origin_name, airport_dest_name) lors de la generation avion.

Q32. Un trajet avion est-il cree pour toutes les paires de villes ?
R. Non. Il faut que chaque extremite du corridor ait une gare associee a un aeroport dans le rayon intermodal (50 km), et que la distance aeroport-aeroport soit >= 100 km.

Q33. Ou est regle ce rayon de 50 km ?
R. INTERMODAL_RADIUS_KM dans transformation/transform.py (utilise par build_intermodal_links).

Q34. Pourquoi filtre distance >= 100 km ?
R. Pour exclure les trajets tres courts peu realistes en avion, applique dans run_transform et generate_plane_routes (MIN_PLANE_DISTANCE_KM).

Q35. Pourquoi la colonne train_type est nulle pour les avions dans le dashboard ?
R. C'est volontaire : train_type ne s'applique qu'aux trains. Dans dashboard_web.py, si vehicule_type contient Avion alors train_type = None.

---

## 6) Questions API

Q36. Quels endpoints cles avez-vous ?
R.
- GET /data : dataset complet pour visualisation,
- GET /search : recherche de trajets,
- GET /compare : comparaison train jour vs nuit.

Q37. Comment gerez-vous les erreurs API ?
R. Try/except avec HTTPException : 404 quand rien trouve, 500 pour erreur interne SQL ou execution.

Q38. Le endpoint /compare compare-t-il aussi avec l'avion ?
R. Non, il compare uniquement trains jour vs trains nuit (filtre sur label contenant nuit).

---

## 7) Questions qualite / limites

Q39. Comment controlez-vous la qualite des donnees cote dashboard ?
R. quality_report calcule : volume, nulls par colonne, doublons logiques et outliers CO2 par IQR.

Q40. Limite technique principale observee pendant les tests gros volume ?
R. I/O disque sur volume externe (et saturation espace/temp). La mitigation a ete de separer les ecritures temporaires lourdes et les exports finaux.

Q41. Pourquoi votre ETL est robuste aux schemas incomplets ?
R. Utilisation de unionByName(allowMissingColumns=True), checks de colonnes conditionnels et cast defensifs (try_cast, to_numeric errors=coerce).

Q42. Si une source est partiellement KO, tout s'arrete ?
R. Non pour l'extraction parallele : on comptabilise les succes/erreurs par source. Le pipeline peut continuer, mais la transformation requiert au moins une source trajets exploitable.

---

## 8) Questions "pieges" frequentes du jury (reponses courtes)

Q43. Pourquoi PySpark et pas seulement Pandas ?
R. Volume multi-providers GTFS + nombreuses jointures. Spark apporte execution distribuee, meilleure gestion memoire/disque et tolerance sur gros jeux.

Q44. Difference entre dimension et table de faits dans votre cas ?
R. Dimensions = contexte descriptif (route, type vehicule). Faits = mesure numerique analysee (co2_kg_passenger) referencee par des cles.

Q45. Pourquoi une moyenne de CO2 par route dans dim_route ?
R. distance_km est agregee au niveau route logique pour stabiliser la dimension et eviter la duplication par observation brute.

Q46. Que se passe-t-il si une station n'a pas de coordonnees ?
R. Avec les left join Back on Track, la ligne peut survivre mais distance/co2 peuvent devenir null, puis etre filtrees avant load.

Q47. Quelle est la principale garantie de non-duplication ?
R. Double niveau :
- dedup Spark final (origin, destination, vehicule_type, source),
- contraintes SQL + filtrage incremental par merge left_only.

Q48. Comment justifiez-vous vos facteurs CO2 ?
R. Table de facteurs explicite dans le code (CO2_FACTORS_PER_100KM) et rappel du referentiel ADEME/UIC dans les commentaires et dashboard.

---

## 9) Reponse type en 30 secondes (si question ouverte)

Notre pipeline part de 3 sources heterogenes, les extrait en parallele, puis standardise les trajets avec PySpark. La transformation calcule distance haversine et CO2, cree des liens intermodaux gare-aeroport, puis genere des trajets avion comparables au meme corridor que le train. Ensuite on charge un schema en etoile PostgreSQL, avec dimensions route et vehicule, et une table de faits CO2. L'API FastAPI expose les donnees via des jointures SQL propres, et le dashboard fournit comparaison train-avion, train jour-nuit et controles qualite.

