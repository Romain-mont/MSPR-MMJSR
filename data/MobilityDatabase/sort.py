import os
from pyspark.sql import SparkSession

# === CONFIGURATION ===
INPUT_PATH = "data/Europe_Rail_Database_clean"
OUTPUT_PATH = "data/Europe_Rail_Database_sorted"

KEEP_COLUMNS = [
    "provider",
    "country",
    "trip_id",
    "origin",
    "destination",
    "dep_time_first",
    "arr_time_last",
    "distance_km",
]


def _get_spark():
    return SparkSession.builder \
        .appName("EuroRailSort") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


def run_sort():
    """Clean fin : suppression des colonnes inutiles"""
    print("=== LANCEMENT CLEAN FIN (SORT) ===")

    if not os.path.exists(INPUT_PATH):
        print(f"Dossier d'entrée introuvable : {INPUT_PATH}")
        return False

    spark = _get_spark()
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
        existing_cols = [c for c in KEEP_COLUMNS if c in df.columns]

        if not existing_cols:
            print("Aucune colonne attendue trouvée, arrêt.")
            return False

        df_sorted = df.select(*existing_cols)
        df_sorted.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)
        print(f"Sortie clean fin : {OUTPUT_PATH}")
        return True
    except Exception as e:
        print(f"Erreur clean fin : {e}")
        return False
    finally:
        spark.stop()


if __name__ == "__main__":
    run_sort()
