#!/bin/bash

# ============================================================
# Script de test du mode incrémental
# ============================================================
# Usage : ./test_incremental.sh

set -e  # Arrêt si erreur

echo "🧪 TEST DU MODE INCRÉMENTAL"
echo "============================================================"
echo ""

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 1. Test avec Luxembourg (petit pays)
echo -e "${YELLOW}📍 Étape 1 : Test avec Luxembourg (pays test)${NC}"
echo "Commande : INCREMENTAL_LOAD=true TARGET_COUNTRIES=['LU'] docker compose up etl"
echo ""
read -p "Appuyez sur Entrée pour continuer (ou Ctrl+C pour annuler)..."

INCREMENTAL_LOAD=true TARGET_COUNTRIES="['LU']" docker compose up etl

# 2. Vérification SQL
echo ""
echo -e "${GREEN}✅ Étape 1 terminée${NC}"
echo ""
echo -e "${YELLOW}🔍 Vérification des données...${NC}"
docker exec -it etl-postgres psql -U ${DB_USER:-mspr_user} -d ${DB_NAME:-mspr_db} -c "SELECT COUNT(*) as total_trajets FROM fact_em;"

echo ""
read -p "Le nombre de trajets a-t-il augmenté ? (o/n) " response
if [[ ! "$response" =~ ^[Oo]$ ]]; then
    echo -e "${RED}❌ Problème détecté. Vérifiez les logs ci-dessus.${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✅ Test du mode incrémental réussi !${NC}"
echo ""
echo -e "${YELLOW}📊 Statistiques finales :${NC}"
docker exec -it etl-postgres psql -U ${DB_USER:-mspr_user} -d ${DB_NAME:-mspr_db} << EOF
SELECT 
    'Routes' as type, 
    COUNT(*) as total 
FROM dim_route
UNION ALL
SELECT 
    'Véhicules', 
    COUNT(*) 
FROM dim_vehicle_type
UNION ALL
SELECT 
    'Faits', 
    COUNT(*) 
FROM fact_em;
EOF

echo ""
echo -e "${GREEN}🎉 Vous pouvez maintenant lancer les groupes de pays !${NC}"
echo ""
echo "Commandes recommandées :"
echo "  1. TARGET_COUNTRIES=['DE','AT','CH'] docker compose up etl"
echo "  2. INCREMENTAL_LOAD=true TARGET_COUNTRIES=['IT','ES','PT'] docker compose up etl"
echo "  3. INCREMENTAL_LOAD=true TARGET_COUNTRIES=['BE','NL','LU','DK','SE'] docker compose up etl"
echo ""
