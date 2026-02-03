import sys
from clean import run_clean
from sort import run_sort
from calc import run_calc
from extraction_final import run_extraction

# === ORCHESTRATION ETL ===

def main():
    """Orchestration complète du pipeline ETL"""
    
    print("LANCEMENT PIPELINE ETL")
    
    try:
        # 1. EXTRACTION - Récupérer les données brutes
        print("ÉTAPE 1 : EXTRACTION - Récupération des données")
        run_extraction()
        
        # 2. TRANSFORMATION - Clean grossier
        print("ÉTAPE 2 : CLEAN GROSSIER - Élimination des datasets inexploitables")
        run_clean()

        # 3. TRANSFORMATION - Clean fin
        print("ÉTAPE 3 : CLEAN FIN - Suppression des colonnes inutiles")
        run_sort()

        # 4. TRANSFORMATION - Calculs
        print("ÉTAPE 4 : CALCULS - Données prêtes pour le load")
        run_calc()
        
        print("PIPELINE ETL TERMINÉ AVEC SUCCÈS")
        
    except Exception as e:
        print(f"ERREUR PIPELINE ETL : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
