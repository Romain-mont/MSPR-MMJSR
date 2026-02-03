import os
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, udf
from pyspark.sql.types import DoubleType, StringType

# === CONFIGURATION ===
INPUT_PATH = "data/Europe_Rail_Database_sorted"
OUTPUT_PATH = "data/Europe_Rail_Database_calc"


def _get_spark():
    return SparkSession.builder \
        .appName("EuroRailCalc") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


def gtfs_time_to_hours(time_str):
    try:
        if not isinstance(time_str, str):
            return 0.0
        h, m, s = map(int, time_str.split(':'))
        return h + m / 60 + s / 3600
    except Exception:
        return 0.0


def determine_train_type(departure_h, duration_h):
    dep_mod = departure_h % 24
    if (dep_mod >= 22 or dep_mod <= 5) and duration_h > 4:
        return "Nuit"
    return "Jour"


def calculate_co2(distance_km, train_type):
    factor = 14.0 if train_type == "Nuit" else 4.0
    return round(distance_km * factor / 1000, 2)


def run_calc():
    """Calculs métiers pour préparer le load"""
    print("=== LANCEMENT CALCULS METIERS ===")

    if not os.path.exists(INPUT_PATH):
        print(f"Dossier d'entrée introuvable : {INPUT_PATH}")
        return False

    spark = _get_spark()
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)

        time_udf = udf(gtfs_time_to_hours, DoubleType())
        type_udf = udf(determine_train_type, StringType())
        co2_udf = udf(calculate_co2, DoubleType())

        df_calc = df.withColumn("dep_dec", time_udf(col("dep_time_first"))) \
            .withColumn("arr_dec", time_udf(col("arr_time_last"))) \
            .withColumn("duration_h", spark_round(col("arr_dec") - col("dep_dec"), 2)) \
            .withColumn("train_type", type_udf(col("dep_dec"), col("duration_h"))) \
            .withColumn("co2_kg", co2_udf(col("distance_km"), col("train_type")))

        df_calc.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)
        print(f"Sortie calculs : {OUTPUT_PATH}")
        return True
    except Exception as e:
        print(f"Erreur calculs : {e}")
        return False
    finally:
        spark.stop()


if __name__ == "__main__":
    run_calc()
