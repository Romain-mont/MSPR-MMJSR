-- ============================================================
-- SCRIPT DE VÉRIFICATION DU MODE INCRÉMENTAL
-- ============================================================
-- Usage : docker exec -it etl-postgres psql -U $DB_USER -d $DB_NAME -f /path/to/verification_incremental.sql

-- 1️⃣ Statistiques générales
\echo '\n📊 STATISTIQUES GÉNÉRALES'
\echo '================================'
SELECT 
    (SELECT COUNT(*) FROM dim_route) as total_routes,
    (SELECT COUNT(*) FROM dim_vehicle_type) as total_vehicules,
    (SELECT COUNT(*) FROM fact_em) as total_faits;

-- 2️⃣ Top 10 pays par nombre de gares
\echo '\n🌍 TOP 10 PAYS (par gares origine)'
\echo '================================'
SELECT 
    SUBSTRING(dep_name FROM '\(([A-Z]{2})\)$') as pays,
    COUNT(*) as nb_gares
FROM dim_route
WHERE dep_name ~ '\([A-Z]{2}\)$'
GROUP BY pays
ORDER BY nb_gares DESC
LIMIT 10;

-- 3️⃣ Répartition par type de véhicule
\echo '\n🚂 RÉPARTITION PAR TYPE DE VÉHICULE'
\echo '================================'
SELECT 
    v.label,
    v.service_type,
    COUNT(f.fact_id) as nb_trajets,
    ROUND(AVG(f.co2_kg_passenger), 2) as co2_moyen_kg
FROM dim_vehicle_type v
LEFT JOIN fact_em f ON v.vehicle_type_id = f.vehicle_type_id
GROUP BY v.label, v.service_type
ORDER BY nb_trajets DESC;

-- 4️⃣ Top 15 routes les plus longues
\echo '\n📏 TOP 15 ROUTES LES PLUS LONGUES'
\echo '================================'
SELECT 
    dep_name as origine,
    arr_name as destination,
    ROUND(distance_km, 1) as distance_km,
    CASE WHEN is_long_distance THEN 'Oui' ELSE 'Non' END as longue_distance
FROM dim_route
ORDER BY distance_km DESC
LIMIT 15;

-- 5️⃣ Vérification des doublons (ne devrait rien retourner)
\echo '\n🔍 VÉRIFICATION DES DOUBLONS'
\echo '================================'
\echo 'Routes dupliquées (devrait être vide) :'
SELECT dep_name, arr_name, COUNT(*) as nb_doublons
FROM dim_route
GROUP BY dep_name, arr_name
HAVING COUNT(*) > 1;

\echo '\nVéhicules dupliqués (devrait être vide) :'
SELECT label, service_type, COUNT(*) as nb_doublons
FROM dim_vehicle_type
GROUP BY label, service_type
HAVING COUNT(*) > 1;

-- 6️⃣ Statistiques des contraintes d'unicité
\echo '\n🔒 CONTRAINTES D''UNICITÉ'
\echo '================================'
SELECT 
    conname as constraint_name,
    conrelid::regclass as table_name
FROM pg_constraint
WHERE conname IN ('unique_route', 'unique_vehicle');

\echo '\n✅ Vérification terminée!'
