"""
TRANSFORMATION ETL - PySpark (Étape 1 : Filtre Grossier)
=========================================================
Pipeline de nettoyage initial des données brutes.

OBJECTIF : Nettoyer les données en ne gardant que les datasets
           qui possèdent des données dans les colonnes minimales exploitables.

Colonnes minimales obligatoires :
    - origin        : Nom gare/aéroport départ
    - destination   : Nom gare/aéroport arrivée
    - train_type    : Type de transport

ÉTAPES DU PIPELINE :
    1. Rassembler les DataFrames de toutes les sources (déjà extraites)
    2. Nettoyer les noms de gares
    3. Filtre grossier : supprimer datasets sans données dans les colonnes minimales
    4. Dédoublonner
    5. Export staging

DONNÉES EN ENTRÉE (depuis extraction.py) :
    - ./data/raw/mobility_gtfs/  → Dossiers GTFS par provider
    - ./data/raw/backontrack_csv/ → Fichiers CSV Back on Track

À VENIR (étapes suivantes) :
    - Enrichissement lat/lon depuis source externe
    - Calcul distance_km via Haversine
    - Calcul co2_kg avec facteurs fixes
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, regexp_replace, when, 
    first, last, coalesce, lower
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from functools import reduce

# === CONFIGURATION ===
RAW_MOBILITY_DIR = "./data/raw/mobility_gtfs"
RAW_BACKONTRACK_DIR = "./data/raw/backontrack_csv"
OUTPUT_DIR = "./data/staging"
OUTPUT_FILE = "staging_routes.csv"

# Colonnes MINIMALES obligatoires (sans lesquelles on ne peut rien faire)
REQUIRED_COLUMNS = ['origin', 'destination', 'train_type']


# ===========================
# INITIALISATION SPARK
# ===========================

def get_spark_session():
    """Crée ou récupère la session Spark."""
    return SparkSession.builder \
        .appName("CO2_ETL_Stage1_Filter") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


# ===========================
# FONCTIONS DE NETTOYAGE
# ===========================

def clean_station_name(df, column_name):
    """
    Nettoie les noms de gares/aéroports dans une colonne.
    - Trim des espaces
    - Suppression des codes type "(ABC)" ou "[XYZ]"
    - Suppression des espaces multiples
    """
    return df.withColumn(
        column_name,
        trim(
            regexp_replace(
                regexp_replace(col(column_name), r'\s*[\(\[][A-Z0-9]{2,5}[\)\]]\s*$', ''),
                r'\s+', ' '
            )
        )
    )


def validate_required_columns(df):
    """
    Filtre les lignes qui n'ont PAS les colonnes minimales remplies.
    Retourne uniquement les lignes exploitables.
    """
    # Filtrer : origin et destination non nulls, non vides, et différents
    df_valid = df.filter(
        (col('origin').isNotNull()) & 
        (col('destination').isNotNull()) &
        (trim(col('origin')) != '') &
        (trim(col('destination')) != '') &
        (col('origin') != col('destination'))
    )
    
    return df_valid


# ===========================
# LECTURE MOBILITY DATABASE (GTFS)
# ===========================

def read_mobility_gtfs(spark, provider_dir):
    """
    Lit les données GTFS d'un provider déjà extrait.
    Retourne un DataFrame avec origin, destination, train_type.
    """
    provider_name = os.path.basename(provider_dir)
    print(f"   📂 {provider_name}...", end=" ", flush=True)
    
    try:
        # Vérifier les fichiers requis
        required = ['stops.txt', 'stop_times.txt', 'trips.txt', 'routes.txt']
        for f in required:
            if not os.path.exists(os.path.join(provider_dir, f)):
                print(f"⚠️ (manque {f})")
                return None
        
        # === LECTURE DES FICHIERS GTFS ===
        stops = spark.read.option("header", "true").csv(f"{provider_dir}/stops.txt")
        stop_times = spark.read.option("header", "true").csv(f"{provider_dir}/stop_times.txt")
        trips = spark.read.option("header", "true").csv(f"{provider_dir}/trips.txt")
        routes = spark.read.option("header", "true").csv(f"{provider_dir}/routes.txt")
        
        # === FILTRE : TRAINS UNIQUEMENT ===
        # route_type : 2 = Rail, 100-117 = Railway Service
        routes = routes.withColumn('route_type', col('route_type').cast('int'))
        rail_routes = routes.filter(
            (col('route_type') == 2) | 
            ((col('route_type') >= 100) & (col('route_type') <= 117))
        )
        
        if rail_routes.count() == 0:
            print("⚠️ (pas de trains)")
            return None
        
        # Récupérer le route_type pour déterminer train_type plus tard
        rail_trips = trips.join(
            rail_routes.select('route_id', 'route_type'), 
            'route_id', 
            'inner'
        )
        
        rail_stop_times = stop_times.join(
            rail_trips.select('trip_id', 'route_type'), 
            'trip_id', 
            'inner'
        )
        
        # === AGRÉGATION : ORIGINE & DESTINATION PAR TRIP ===
        rail_stop_times = rail_stop_times.withColumn(
            'stop_sequence', col('stop_sequence').cast('int')
        )
        
        window_asc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').asc())
        window_desc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').desc())
        
        trip_bounds = rail_stop_times \
            .withColumn('origin_stop_id', first('stop_id').over(window_asc)) \
            .withColumn('dest_stop_id', first('stop_id').over(window_desc)) \
            .withColumn('route_type_val', first('route_type').over(window_asc)) \
            .select('trip_id', 'origin_stop_id', 'dest_stop_id', 'route_type_val') \
            .distinct()
        
        # === ENRICHISSEMENT : NOMS DES GARES ===
        # Jointure origine
        df = trip_bounds.join(
            stops.select(
                col('stop_id').alias('origin_stop_id'),
                col('stop_name').alias('origin')
            ),
            'origin_stop_id'
        )
        
        # Jointure destination
        df = df.join(
            stops.select(
                col('stop_id').alias('dest_stop_id'),
                col('stop_name').alias('destination')
            ),
            'dest_stop_id'
        )
        
        # === DÉTERMINATION DU TYPE DE TRAIN ===
        # route_type 102 = Train de nuit, sinon Jour
        df = df.withColumn(
            'train_type',
            when(col('route_type_val') == 102, 'Nuit').otherwise('Jour')
        )
        
        # === SÉLECTION COLONNES MINIMALES ===
        df = df.select('origin', 'destination', 'train_type')
        
        # Ajouter métadonnées source
        df = df.withColumn('source', lit('mobility_db')) \
               .withColumn('provider', lit(provider_name))
        
        count = df.count()
        print(f"✅ ({count} trajets)")
        return df
        
    except Exception as e:
        print(f"❌ ({e})")
        return None


def read_all_mobility(spark):
    """
    Lit tous les providers GTFS de Mobility Database (déjà extraits).
    """
    print("\n🌐 Lecture Mobility Database...")
    
    if not os.path.exists(RAW_MOBILITY_DIR):
        print(f"   ❌ Dossier introuvable : {RAW_MOBILITY_DIR}")
        return None
    
    all_dfs = []
    
    for provider_name in os.listdir(RAW_MOBILITY_DIR):
        provider_path = os.path.join(RAW_MOBILITY_DIR, provider_name)
        if os.path.isdir(provider_path):
            df = read_mobility_gtfs(spark, provider_path)
            if df is not None:
                all_dfs.append(df)
    
    if all_dfs:
        result = reduce(lambda a, b: a.union(b), all_dfs)
        total = result.count()
        print(f"   📊 Mobility Database : {total} trajets lus")
        return result
    
    return None


# ===========================
# LECTURE BACK ON TRACK
# ===========================

def read_backontrack(spark):
    """
    Lit les données Back on Track (déjà extraites).
    """
    print("\n📊 Lecture Back on Track...")
    
    try:
        files = {
            'routes': 'back_on_track_routes.csv',
            'trips': 'back_on_track_trips.csv',
            'stops': 'back_on_track_stops.csv',
            'trip_stop': 'back_on_track_trip_stop.csv'
        }
        
        # Vérifier existence des fichiers
        for key, filename in files.items():
            filepath = os.path.join(RAW_BACKONTRACK_DIR, filename)
            if not os.path.exists(filepath):
                print(f"   ⚠️ Fichier manquant : {filename}")
                return None
        
        # === LECTURE ===
        routes = spark.read.option("header", "true").csv(f"{RAW_BACKONTRACK_DIR}/back_on_track_routes.csv")
        trips = spark.read.option("header", "true").csv(f"{RAW_BACKONTRACK_DIR}/back_on_track_trips.csv")
        stops = spark.read.option("header", "true").csv(f"{RAW_BACKONTRACK_DIR}/back_on_track_stops.csv")
        trip_stop = spark.read.option("header", "true").csv(f"{RAW_BACKONTRACK_DIR}/back_on_track_trip_stop.csv")
        
        print(f"   ✅ Fichiers chargés")
        
        # === FUSIONS ===
        merged = trip_stop.join(trips, 'trip_id', 'left') \
                          .join(routes.select('route_id', 'route_type'), 'route_id', 'left') \
                          .join(stops.select('stop_id', 'stop_name'), 'stop_id', 'left')
        
        # === AGRÉGATION O-D ===
        merged = merged.withColumn('stop_sequence', col('stop_sequence').cast('int'))
        
        window_asc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').asc())
        window_desc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').desc())
        
        df = merged \
            .withColumn('origin', first('stop_name').over(window_asc)) \
            .withColumn('destination', first('stop_name').over(window_desc)) \
            .withColumn('route_type_val', first('route_type').over(window_asc)) \
            .select('trip_id', 'origin', 'destination', 'route_type_val') \
            .distinct()
        
        # === TYPE DE TRAIN ===
        # route_type 102 = Train de nuit
        df = df.withColumn(
            'train_type',
            when(col('route_type_val') == '102', 'Nuit').otherwise('Jour')
        )
        
        # === SÉLECTION COLONNES MINIMALES ===
        df = df.select('origin', 'destination', 'train_type')
        
        # Ajouter métadonnées
        df = df.withColumn('source', lit('back_on_track')) \
               .withColumn('provider', lit('BackOnTrack'))
        
        count = df.count()
        print(f"   📊 Back on Track : {count} trajets lus")
        return df
        
    except Exception as e:
        print(f"   ❌ Erreur : {e}")
        return None


# ===========================
# ORCHESTRATION ETL
# ===========================

def run_transform():
    """
    Pipeline de transformation (Filtre Grossier).
    
    Prérequis : extraction.py doit avoir été exécuté avant !
    Les données brutes doivent être dans :
        - ./data/raw/mobility_gtfs/
        - ./data/raw/backontrack_csv/
    
    ÉTAPES :
        1. Rassembler les DataFrames de toutes les sources
        2. Nettoyer les noms de gares
        3. Filtre grossier : supprimer les datasets sans données dans les colonnes minimales
        4. Dédoublonner
        5. Export staging
    """
    print("=" * 60)
    print("🚀 TRANSFORMATION - Filtre Grossier")
    print("=" * 60)
    print(f"📋 Colonnes minimales requises : {REQUIRED_COLUMNS}")
    print(f"📂 Données en entrée :")
    print(f"   - {RAW_MOBILITY_DIR}")
    print(f"   - {RAW_BACKONTRACK_DIR}")
    print("=" * 60)
    
    # Initialiser Spark
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # =========================================
    # ÉTAPE 1 : RASSEMBLER LES DATAFRAMES
    # =========================================
    print("\n" + "=" * 40)
    print("📥 ÉTAPE 1 : Rassemblement des DataFrames")
    print("=" * 40)
    
    df_mobility = read_all_mobility(spark)
    df_backontrack = read_backontrack(spark)
    
    all_dfs = [df for df in [df_mobility, df_backontrack] if df is not None]
    
    if not all_dfs:
        print("\n❌ Aucune donnée trouvée. As-tu exécuté extraction.py ?")
        spark.stop()
        return None
    
    # Union de tous les DataFrames
    df_combined = reduce(lambda a, b: a.union(b), all_dfs)
    total_brut = df_combined.count()
    print(f"\n✅ Total rassemblé : {total_brut} trajets")
    
    # =========================================
    # ÉTAPE 2 : NETTOYAGE DES NOMS
    # =========================================
    print("\n" + "=" * 40)
    print("🧹 ÉTAPE 2 : Nettoyage des noms de gares")
    print("=" * 40)
    
    df_cleaned = clean_station_name(df_combined, 'origin')
    df_cleaned = clean_station_name(df_cleaned, 'destination')
    print("   ✅ Noms nettoyés (espaces, codes supprimés)")
    
    # =========================================
    # ÉTAPE 3 : FILTRE GROSSIER
    # =========================================
    print("\n" + "=" * 40)
    print("🔍 ÉTAPE 3 : Filtre grossier (colonnes minimales)")
    print("=" * 40)
    
    df_valid = validate_required_columns(df_cleaned)
    total_valid = df_valid.count()
    
    dropped = total_brut - total_valid
    print(f"   ✅ Lignes valides : {total_valid}")
    print(f"   🗑️ Lignes supprimées (données manquantes) : {dropped}")
    
    # =========================================
    # ÉTAPE 4 : DÉDOUBLONNAGE
    # =========================================
    print("\n" + "=" * 40)
    print("🔄 ÉTAPE 4 : Dédoublonnage")
    print("=" * 40)
    
    df_dedup = df_valid.dropDuplicates(['origin', 'destination', 'train_type', 'source'])
    total_dedup = df_dedup.count()
    
    duplicates = total_valid - total_dedup
    print(f"   🔄 Doublons supprimés : {duplicates}")
    print(f"   📊 Total final : {total_dedup} trajets uniques")
    
    # =========================================
    # ÉTAPE 5 : EXPORT STAGING
    # =========================================
    print("\n" + "=" * 40)
    print("💾 ÉTAPE 5 : Export staging")
    print("=" * 40)
    
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    
    # Export CSV
    df_dedup.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f"{OUTPUT_DIR}/temp_output")
    
    # Renommer le fichier part-* en nom propre
    import glob
    import shutil
    temp_files = glob.glob(f"{OUTPUT_DIR}/temp_output/part-*.csv")
    if temp_files:
        shutil.move(temp_files[0], output_path)
        shutil.rmtree(f"{OUTPUT_DIR}/temp_output")
    
    print(f"   📂 Fichier exporté : {output_path}")
    
    # =========================================
    # RÉSUMÉ
    # =========================================
    print("\n" + "=" * 60)
    print("🎉 TRANSFORMATION TERMINÉE")
    print("=" * 60)
    
    print("\n📈 Répartition par source :")
    df_dedup.groupBy('source').count().show()
    
    print("📈 Répartition par type de train :")
    df_dedup.groupBy('train_type').count().show()
    
    print("\n📋 Prochaines étapes :")
    print("   1. Enrichir avec lat/lon depuis source externe")
    print("   2. Calculer distance_km via Haversine")
    print("   3. Calculer co2_kg avec facteurs fixes")
    print("   4. Préparer pour le load dans le datamart")
    
    spark.stop()
    return df_dedup


if __name__ == "__main__":
    run_transform()
