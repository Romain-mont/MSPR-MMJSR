import os, sys, glob, re, csv, shutil, gc
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

# Regex part-xxxxx*.csv (Spark)
_PART_RE = re.compile(r"part-(\d+).*\.csv$")


# Détection d'exécution dans Docker
def running_in_docker():
    path = '/.dockerenv'
    return os.path.exists(path) or os.environ.get('RUNNING_IN_DOCKER') == '1'

# Fix encoding uniquement si non-Docker et Windows
if sys.platform == "win32" and not running_in_docker():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

RAW_MOBILITY_DIR    = os.environ.get("RAW_MOBILITY_DIR", "./data/raw/mobility_gtfs")
RAW_BACKONTRACK_DIR = os.environ.get("RAW_BACKONTRACK_DIR", "./data/raw/backontrack_csv")
RAW_AIRPORTS_DIR    = os.environ.get("RAW_AIRPORTS_DIR", "./data/raw/airports")
OUTPUT_DIR          = os.environ.get("OUTPUT_DIR", "./data/staging")

OUTPUT_AIRPORTS_FILE   = os.environ.get("OUTPUT_AIRPORTS_FILE", "staging_airports.csv")
OUTPUT_INTERMODAL_FILE = os.environ.get("OUTPUT_INTERMODAL_FILE", "staging_intermodal.csv")
OUTPUT_FINAL_FILE      = os.environ.get("OUTPUT_FINAL_FILE", "final_routes.csv")

REQUIRED_COLUMNS_AIRPORTS = ["airport_name", "aero_lat", "aero_long", "category", "iata_code", "country_code"]

CO2_FACTORS_PER_100KM = {
    "TGV": 0.29, "Train Jour": 0.29,
    "Intercité": 0.9, "Train Nuit": 0.9,
    "Avion Court": 22.5, "Avion Moyen": 18.45, "Avion Moyen-Long": 16.68, "Avion Long": 17.79,
    "default": 0.9
}
EARTH_RADIUS_KM = 6371.0

INTERMODAL_RADIUS_KM  = 50
BBOX_MARGIN_DEG       = 1.0
MIN_PLANE_DISTANCE_KM = 100


# ===========================
# 2) HELPERS
# ===========================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _spark_warmup(df):
    # Matérialise "light" sans scanner tout le dataset (show évite un collect massif)
    df.limit(1).show()

def read_csv(spark, path: str):
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )

def _merge_part_csvs(tmp_dir: str, out_csv_path: str):
    """
    Fusionne les part-*.csv en un seul fichier SANS charger tout en RAM.
    - garde un seul header
    - ignore fichiers parasites (_SUCCESS, .crc, etc.)
    - vérifie l'identité des headers
    """
    if not os.path.exists(tmp_dir):
        raise RuntimeError(f"tmp_dir introuvable: {tmp_dir}")

    files = []
    for f in os.listdir(tmp_dir):
        if not (f.startswith("part-") and f.endswith(".csv")):
            continue
        if f.endswith(".crc") or f.startswith("."):
            continue
        files.append(f)

    if not files:
        raise RuntimeError(f"Aucun fichier part-*.csv trouvé dans: {tmp_dir}")

    def part_key(name: str):
        m = _PART_RE.search(name)
        return (0, int(m.group(1))) if m else (1, name)

    part_files = [os.path.join(tmp_dir, f) for f in sorted(files, key=part_key)]
    ensure_dir(os.path.dirname(out_csv_path) or ".")

    expected_header = None
    wrote_header = False

    with open(out_csv_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        for idx, pf in enumerate(part_files):
            if not os.path.exists(pf) or os.path.getsize(pf) == 0:
                continue

            with open(pf, "r", newline="", encoding="utf-8", errors="replace") as in_f:
                reader = csv.reader(in_f)
                try:
                    header = next(reader)
                except StopIteration:
                    continue

                if not header:
                    continue

                if expected_header is None:
                    expected_header = header

                if header != expected_header:
                    raise RuntimeError(
                        f"Header mismatch dans {os.path.basename(pf)}.\n"
                        f"Attendu: {expected_header}\n"
                        f"Reçu:    {header}"
                    )

                if not wrote_header:
                    writer.writerow(expected_header)
                    wrote_header = True

                for row in reader:
                    if row:
                        writer.writerow(row)

            if (idx + 1) % 10 == 0:
                out_f.flush()

def export_to_hdfs(df, hdfs_path: str):
    """
    Écriture directe sur HDFS.
    Hadoop gère le commit, pas de surcharge RAM.
    """
    (
        df.write
          .mode("overwrite")
          .option("header", "true")
          .option("compression", "gzip")  # important: réduit IO + disque
          .csv(hdfs_path)
    )

def normalize_station_col(df, colname: str):
    return df.withColumn(
        colname,
        F.trim(
            F.regexp_replace(
                F.regexp_replace(F.col(colname), r"\s*[\(\[][A-Z0-9]{2,5}[\)\]]\s*$", ""),
                r"\s+", " "
            )
        )
    )

def validate_routes(df):
    return df.filter(
        F.col("origin").isNotNull() &
        F.col("destination").isNotNull() &
        (F.trim("origin") != "") &
        (F.trim("destination") != "") &
        (F.col("origin") != F.col("destination"))
    )


# ===========================
# 1) SPARK SESSION (Windows + Hadoop + Low RAM)
# ===========================
def get_spark_session():
    py = sys.executable
    builder = SparkSession.builder.appName("CO2_ETL_Europe_HDFS")
    builder = builder.master("local[*]")
    builder = builder.config("spark.pyspark.driver.python", py)
    builder = builder.config("spark.pyspark.python", py)
    builder = builder.config("spark.driver.memory", "2g")
    builder = builder.config("spark.driver.maxResultSize", "1g")
    builder = builder.config("spark.sql.shuffle.partitions", "32")
    builder = builder.config("spark.default.parallelism", "32")
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.shuffle.compress", "true")
    builder = builder.config("spark.shuffle.spill.compress", "true")
    builder = builder.config("spark.sql.sources.commitProtocolClass",
                             "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    # FIX : Désactiver les vérifications de permissions (pour disques externes exFAT/FAT32)
    builder = builder.config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    builder = builder.config("spark.hadoop.fs.file.impl.disable.cache", "true")
    
    # Pour Docker, on laisse le fs.defaultFS local (pas HDFS)
    if not running_in_docker():
        builder = builder.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    spark = builder.getOrCreate()
    return spark


# ===========================
# 3) DISTANCE + CO2
# ===========================
def haversine_distance(lat1, lon1, lat2, lon2):
    lat1r, lat2r = F.radians(lat1), F.radians(lat2)
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2) + (F.cos(lat1r) * F.cos(lat2r) * (F.sin(dlon / 2) ** 2))
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    dist = F.lit(EARTH_RADIUS_KM) * c

    return F.when(
        lat1.isNotNull() & lon1.isNotNull() & lat2.isNotNull() & lon2.isNotNull(),
        dist
    ).otherwise(F.lit(None).cast(DoubleType()))

def co2_kg(distance_km, vehicule_type):
    factor = (
        F.when(vehicule_type.contains("Avion") & (distance_km <= 1000), F.lit(CO2_FACTORS_PER_100KM["Avion Court"]))
         .when(vehicule_type.contains("Avion") & (distance_km <= 2000), F.lit(CO2_FACTORS_PER_100KM["Avion Moyen"]))
         .when(vehicule_type.contains("Avion") & (distance_km <= 5000), F.lit(CO2_FACTORS_PER_100KM["Avion Moyen-Long"]))
         .when(vehicule_type.contains("Avion"), F.lit(CO2_FACTORS_PER_100KM["Avion Long"]))
         .when(vehicule_type.isin("Train Jour", "Train Nuit"), F.lit(CO2_FACTORS_PER_100KM["default"]))
         .otherwise(F.lit(CO2_FACTORS_PER_100KM["default"]))
    )
    return F.when(
        distance_km.isNotNull() & vehicule_type.isNotNull(),
        (distance_km * factor / 100.0)
    ).otherwise(F.lit(None).cast(DoubleType()))


# ===========================
# 4) MOBILITY (GTFS)
# ===========================
def read_mobility_provider(spark, provider_dir: str):
    provider_name = os.path.basename(provider_dir)
    required = ["stops.txt", "stop_times.txt", "trips.txt", "routes.txt"]
    if any(not os.path.exists(os.path.join(provider_dir, f)) for f in required):
        return None

    import gc
    # Checkpoint après chaque gros read_csv
    stops      = read_csv(spark, f"{provider_dir}/stops.txt")
    stops_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_stops.csv")
    stops.write.mode("overwrite").option("header", "true").csv(stops_path)
    del stops
    spark.catalog.clearCache(); gc.collect()
    stops = spark.read.option("header", "true").csv(stops_path)

    stop_times = read_csv(spark, f"{provider_dir}/stop_times.txt")
    stop_times_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_stop_times.csv")
    stop_times.write.mode("overwrite").option("header", "true").csv(stop_times_path)
    del stop_times
    spark.catalog.clearCache(); gc.collect()
    stop_times = spark.read.option("header", "true").csv(stop_times_path)

    trips      = read_csv(spark, f"{provider_dir}/trips.txt")
    trips_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_trips.csv")
    trips.write.mode("overwrite").option("header", "true").csv(trips_path)
    del trips
    spark.catalog.clearCache(); gc.collect()
    trips = spark.read.option("header", "true").csv(trips_path)

    routes     = read_csv(spark, f"{provider_dir}/routes.txt")
    routes_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_routes.csv")
    routes.write.mode("overwrite").option("header", "true").csv(routes_path)
    del routes
    spark.catalog.clearCache(); gc.collect()
    routes = spark.read.option("header", "true").csv(routes_path)

    routes = routes.withColumn("route_type", F.col("route_type").cast("int"))
    rail_routes = routes.filter((F.col("route_type") == 2) | ((F.col("route_type") >= 100) & (F.col("route_type") <= 117)))

    # Low-RAM: éviter count() complet
    if not rail_routes.take(1):
        return None

    trip_sel = ["trip_id", "route_id"] + (["shape_id"] if "shape_id" in trips.columns else [])
    # Checkpoint après grosse jointure rail_trips
    rail_trips = trips.select(*trip_sel).join(rail_routes.select("route_id", "route_type"), "route_id", "inner")
    rail_trips_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_rail_trips.csv")
    rail_trips.write.mode("overwrite").option("header", "true").csv(rail_trips_path)
    del rail_trips
    spark.catalog.clearCache(); gc.collect()
    rail_trips = spark.read.option("header", "true").csv(rail_trips_path)

    rail_stop_times = (
        stop_times.join(rail_trips.select("trip_id", "route_type"), "trip_id", "inner")
        .withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
        .select("trip_id", "stop_id", "stop_sequence", "departure_time", "arrival_time", "route_type")
    )
    rail_stop_times_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_rail_stop_times.csv")
    rail_stop_times.write.mode("overwrite").option("header", "true").csv(rail_stop_times_path)
    del rail_stop_times
    spark.catalog.clearCache(); gc.collect()
    rail_stop_times = spark.read.option("header", "true").csv(rail_stop_times_path)

    seqs = (
        rail_stop_times.groupBy("trip_id")
        .agg(
            F.min("stop_sequence").alias("min_seq"),
            F.max("stop_sequence").alias("max_seq"),
            F.first("route_type", ignorenulls=True).alias("route_type_val"),
        )
    )
    seqs_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_seqs.csv")
    seqs.write.mode("overwrite").option("header", "true").csv(seqs_path)
    del seqs
    spark.catalog.clearCache(); gc.collect()
    seqs = spark.read.option("header", "true").csv(seqs_path)

    origin = (
        rail_stop_times.alias("st")
        .join(seqs.alias("s"),
              (F.col("st.trip_id") == F.col("s.trip_id")) & (F.col("st.stop_sequence") == F.col("s.min_seq")),
              "inner")
        .select(
            F.col("st.trip_id").alias("trip_id"),
            F.col("st.stop_id").alias("origin_stop_id"),
            F.col("st.departure_time").alias("departure_time"),
        )
    )
    origin_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_origin.csv")
    origin.write.mode("overwrite").option("header", "true").csv(origin_path)
    del origin
    spark.catalog.clearCache(); gc.collect()
    origin = spark.read.option("header", "true").csv(origin_path)

    dest = (
        rail_stop_times.alias("st")
        .join(seqs.alias("s"),
              (F.col("st.trip_id") == F.col("s.trip_id")) & (F.col("st.stop_sequence") == F.col("s.max_seq")),
              "inner")
        .select(
            F.col("st.trip_id").alias("trip_id"),
            F.col("st.stop_id").alias("dest_stop_id"),
            F.col("st.arrival_time").alias("arrival_time"),
        )
    )
    dest_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_dest.csv")
    dest.write.mode("overwrite").option("header", "true").csv(dest_path)
    del dest
    spark.catalog.clearCache(); gc.collect()
    dest = spark.read.option("header", "true").csv(dest_path)

    bounds = (
        seqs.select("trip_id", "route_type_val")
        .join(origin, "trip_id", "inner")
        .join(dest, "trip_id", "inner")
        .withColumn("shape_distance_km", F.lit(None).cast(DoubleType()))
    )
    bounds_path = os.path.join(OUTPUT_DIR, f"staging_{os.path.basename(provider_dir)}_bounds.csv")
    bounds.write.mode("overwrite").option("header", "true").csv(bounds_path)
    del bounds
    spark.catalog.clearCache(); gc.collect()
    bounds = spark.read.option("header", "true").csv(bounds_path)

    origin_stops = stops.select(
        F.col("stop_id").alias("origin_stop_id"),
        F.col("stop_name").alias("origin"),
        F.col("stop_lat").cast(DoubleType()).alias("station_lat"),
        F.col("stop_lon").cast(DoubleType()).alias("station_long"),
    )
    dest_stops = stops.select(
        F.col("stop_id").alias("dest_stop_id"),
        F.col("stop_name").alias("destination"),
        F.col("stop_lat").cast(DoubleType()).alias("station_lat_dest"),
        F.col("stop_lon").cast(DoubleType()).alias("station_long_dest"),
    )

    df = bounds.join(origin_stops, "origin_stop_id").join(dest_stops, "dest_stop_id")
    df = df.withColumn(
        "vehicule_type",
        F.when(F.col("route_type_val") == 102, F.lit("Train Nuit")).otherwise(F.lit("Train Jour"))
    )

    return (
        df.select(
            "origin", "destination", "vehicule_type",
            "station_lat", "station_long", "station_lat_dest", "station_long_dest",
            "departure_time", "arrival_time", "shape_distance_km"
        )
        .withColumn("source", F.lit("mobility_db"))
        .withColumn("provider", F.lit(provider_name))
    )

def read_all_mobility(spark, country_filter=None):
    EU = ['FR','DE','CH','BE','NL','AT','IT','ES','PT','PL','CZ','SK','HU','SI','HR','DK','SE','NO','FI','IE','GB','LU','RO','BG','GR','EE','LV','LT']
    countries = EU if country_filter is None else ([country_filter] if isinstance(country_filter, str) else country_filter)

    if not os.path.exists(RAW_MOBILITY_DIR):
        print(f"❌ Dossier introuvable : {RAW_MOBILITY_DIR}")
        return None

    dfs = []
    for provider_name in os.listdir(RAW_MOBILITY_DIR):
        if not any(provider_name.startswith(f"{c}_") for c in countries):
            continue
        p = os.path.join(RAW_MOBILITY_DIR, provider_name)
        if not os.path.isdir(p):
            continue
        df = read_mobility_provider(spark, p)
        if df is not None:
            dfs.append(df)

    if not dfs:
        return None

    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


# ===========================
# 5) BACK ON TRACK
# ===========================
def read_backontrack(spark):
    files = {
        "routes": f"{RAW_BACKONTRACK_DIR}/back_on_track_routes.csv",
        "trips":  f"{RAW_BACKONTRACK_DIR}/back_on_track_trips.csv",
        "stops":  f"{RAW_BACKONTRACK_DIR}/back_on_track_stops.csv",
    }
    if any(not os.path.exists(p) for p in files.values()):
        return None

    import gc
    routes = read_csv(spark, files["routes"])
    routes_path = os.path.join(OUTPUT_DIR, "staging_backontrack_routes.csv")
    routes.write.mode("overwrite").option("header", "true").csv(routes_path)
    del routes
    spark.catalog.clearCache(); gc.collect()
    routes = spark.read.option("header", "true").csv(routes_path)

    trips  = read_csv(spark, files["trips"])
    trips_path = os.path.join(OUTPUT_DIR, "staging_backontrack_trips.csv")
    trips.write.mode("overwrite").option("header", "true").csv(trips_path)
    del trips
    spark.catalog.clearCache(); gc.collect()
    trips = spark.read.option("header", "true").csv(trips_path)

    stops  = read_csv(spark, files["stops"])
    stops_path = os.path.join(OUTPUT_DIR, "staging_backontrack_stops.csv")
    stops.write.mode("overwrite").option("header", "true").csv(stops_path)
    del stops
    spark.catalog.clearCache(); gc.collect()
    stops = spark.read.option("header", "true").csv(stops_path)

    origin_col = "trip_origin"   if "trip_origin" in trips.columns else None
    dest_col   = "trip_headsign" if "trip_headsign" in trips.columns else None
    if not origin_col or not dest_col:
        return None

    stops_coords = (
        stops.select(
            F.col("station_name").alias("stop_name_match"),
            F.col("station_lat").cast(DoubleType()).alias("lat"),
            F.col("station_long").cast(DoubleType()).alias("lon"),
        )
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    )

    df = trips.join(routes.select("route_id", "vehicule_type"), "route_id", "left")
    df_path = os.path.join(OUTPUT_DIR, "staging_backontrack_joined.csv")
    df.write.mode("overwrite").option("header", "true").csv(df_path)
    del df
    spark.catalog.clearCache(); gc.collect()
    df = spark.read.option("header", "true").csv(df_path)

    df = df.join(
        stops_coords.withColumnRenamed("lat", "station_lat").withColumnRenamed("lon", "station_long"),
        F.col(origin_col) == F.col("stop_name_match"),
        "left",
    ).drop("stop_name_match")

    df = df.join(
        stops_coords.withColumnRenamed("lat", "station_lat_dest").withColumnRenamed("lon", "station_long_dest"),
        F.col(dest_col) == F.col("stop_name_match"),
        "left",
    ).drop("stop_name_match")

    df = df.withColumn(
        "vehicule_type",
        F.when(F.col("vehicule_type") == F.lit("102"), F.lit("Train Nuit"))
         .when(F.col("vehicule_type") == F.lit("2"),   F.lit("Train Jour"))
         .otherwise(F.lit("Train Jour"))
    )

    return (
        df.select(
            F.col(origin_col).alias("origin"),
            F.col(dest_col).alias("destination"),
            "vehicule_type",
            "station_lat", "station_long", "station_lat_dest", "station_long_dest",
            F.col("origin_departure_time").alias("departure_time"),
            F.col("destination_arrival_time").alias("arrival_time"),
        )
        .filter(
            F.col("origin").isNotNull() & F.col("destination").isNotNull() &
            (F.col("origin") != "#N/A") & (F.col("destination") != "#N/A") &
            (F.trim("origin") != "") & (F.trim("destination") != "")
        )
        .withColumn("shape_distance_km", F.lit(None).cast(DoubleType()))
        .withColumn("source", F.lit("back_on_track"))
        .withColumn("provider", F.lit("BackOnTrack"))
    )


# ===========================
# 6) AIRPORTS (OurAirports)
# ===========================
def read_airports(spark):
    if not os.path.exists(RAW_AIRPORTS_DIR):
        return None

    airport_files = glob.glob(f"{RAW_AIRPORTS_DIR}/airports_*.csv")
    if not airport_files:
        return None

    latest = sorted(airport_files)[-1]
    import gc
    df = read_csv(spark, latest)
    airports_path = os.path.join(OUTPUT_DIR, "staging_airports_loaded.csv")
    df.write.mode("overwrite").option("header", "true").csv(airports_path)
    del df
    spark.catalog.clearCache(); gc.collect()
    df = spark.read.option("header", "true").csv(airports_path)

    mapping = {
        "latitude_deg": "aero_lat",
        "longitude_deg": "aero_long",
        "type": "category",
        "name": "airport_name",
        "iso_country": "country_code",
    }
    for old, new in mapping.items():
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)

    keep = [c for c in REQUIRED_COLUMNS_AIRPORTS if c in df.columns]
    if "ident" in df.columns and "ident" not in keep:
        keep.append("ident")
    if not keep:
        return None

    df = df.select(*keep)
    df = df.withColumn("aero_lat", F.col("aero_lat").cast(DoubleType())) \
           .withColumn("aero_long", F.col("aero_long").cast(DoubleType()))

    df = df.filter(
        F.col("aero_lat").isNotNull() & F.col("aero_long").isNotNull() &
        F.col("category").isin("large_airport", "medium_airport", "small_airport")
    )

    if "country_code" in df.columns:
        eu = ['FR','DE','CH','IT','ES','BE','NL','AT','GB','PT','PL','CZ','DK','SE','NO','FI']
        df = df.filter(F.col("country_code").isin(eu))

    return df


# ===========================
# 7) INTERMODALITÉ + AVION (Low RAM)
# ===========================
def _bbox_join_cond(st_lat, st_lon, ap_lat, ap_lon, margin_deg=BBOX_MARGIN_DEG):
    return (
        (F.abs(st_lat - ap_lat) <= F.lit(margin_deg)) &
        (F.abs(st_lon - ap_lon) <= F.lit(margin_deg))
    )

def build_intermodal_links(df_routes, df_airports, radius_km=INTERMODAL_RADIUS_KM):
    """
    Low-RAM intermodalité:
    - pas de repartition() gratuit
    - cache DISK_ONLY minimal
    - Window remplacé par min-distance + join (souvent moins gourmand)
    """
    if df_airports is None:
        return None

    stations = (
        df_routes.select(
            F.col("origin").alias("station_name"),
            F.col("station_lat").alias("station_lat"),
            F.col("station_long").alias("station_long"),
        )
        .unionByName(
            df_routes.select(
                F.col("destination").alias("station_name"),
                F.col("station_lat_dest").alias("station_lat"),
                F.col("station_long_dest").alias("station_long"),
            ),
            allowMissingColumns=True
        )
        .filter(
            F.col("station_name").isNotNull() &
            F.col("station_lat").isNotNull() &
            F.col("station_long").isNotNull()
        )
        .dropDuplicates(["station_name", "station_lat", "station_long"])
    )

    # Cache disque uniquement si dataset stations est gros
    stations = stations.persist(StorageLevel.DISK_ONLY)
    _spark_warmup(stations)

    airports = (
        df_airports.select(
            "airport_name",
            F.col("aero_lat").alias("aero_lat"),
            F.col("aero_long").alias("aero_long"),
            "iata_code",
            "country_code"
        )
        .filter(F.col("aero_lat").isNotNull() & F.col("aero_long").isNotNull())
        .dropDuplicates(["iata_code", "aero_lat", "aero_long"])
    )

    airports_b = F.broadcast(airports)

    joined = stations.join(
        airports_b,
        _bbox_join_cond(
            F.col("station_lat"), F.col("station_long"),
            F.col("aero_lat"), F.col("aero_long"),
            margin_deg=BBOX_MARGIN_DEG
        ),
        "inner"
    )

    joined = joined.withColumn(
        "station_airport_km",
        haversine_distance(
            F.col("station_lat"), F.col("station_long"),
            F.col("aero_lat"), F.col("aero_long")
        )
    ).filter(
        F.col("station_airport_km").isNotNull() &
        (F.col("station_airport_km") <= F.lit(radius_km))
    ).select(
        "station_name", "station_lat", "station_long",
        "airport_name", "aero_lat", "aero_long",
        "iata_code", "country_code",
        "station_airport_km"
    )

    # Low-RAM: min distance + join (évite row_number + sort complet)
    best_dist = joined.groupBy("station_name").agg(F.min("station_airport_km").alias("min_km"))
    best = joined.join(
        best_dist,
        (joined.station_name == best_dist.station_name) & (joined.station_airport_km == best_dist.min_km),
        "inner"
    ).drop(best_dist.station_name).drop("min_km").dropDuplicates(["station_name"])

    stations.unpersist(blocking=False)
    return best

def generate_plane_routes(df_routes, df_intermodal, min_plane_km=MIN_PLANE_DISTANCE_KM):
    if df_intermodal is None:
        return None

    # Low-RAM: pas de repartition() => on garde la distribution existante
    pairs = df_routes.select("origin", "destination").dropDuplicates()

    inter_o = df_intermodal.select(
        F.col("station_name").alias("origin"),
        F.col("airport_name").alias("airport_origin"),
        F.col("aero_lat").alias("airport_origin_lat"),
        F.col("aero_long").alias("airport_origin_long"),
        F.col("iata_code").alias("iata_origin"),
    )
    inter_d = df_intermodal.select(
        F.col("station_name").alias("destination"),
        F.col("airport_name").alias("airport_dest"),
        F.col("aero_lat").alias("airport_dest_lat"),
        F.col("aero_long").alias("airport_dest_long"),
        F.col("iata_code").alias("iata_dest"),
    )

    dfp = pairs.join(inter_o, "origin", "inner").join(inter_d, "destination", "inner")

    # Conversion explicite des coordonnées aéroports en DOUBLE (évite DATATYPE_MISMATCH)
    for airport_coord in ["airport_origin_lat", "airport_origin_long", "airport_dest_lat", "airport_dest_long"]:
        dfp = dfp.withColumn(airport_coord, F.col(airport_coord).cast(DoubleType()))

    dfp = dfp.withColumn(
        "distance_km",
        haversine_distance(
            F.col("airport_origin_lat"), F.col("airport_origin_long"),
            F.col("airport_dest_lat"), F.col("airport_dest_long")
        )
    ).filter(
        F.col("distance_km").isNotNull() &
        (F.col("distance_km") >= F.lit(min_plane_km))
    )

    dfp = (
        dfp.withColumn("vehicule_type", F.lit("Avion"))
           .withColumn("co2_kg", co2_kg(F.col("distance_km"), F.col("vehicule_type")))
    )

    return dfp.select(
        F.col("airport_origin").alias("origin"),
        F.col("airport_dest").alias("destination"),
        "vehicule_type",
        F.col("airport_origin_lat").alias("station_lat"),
        F.col("airport_origin_long").alias("station_long"),
        F.col("airport_dest_lat").alias("station_lat_dest"),
        F.col("airport_dest_long").alias("station_long_dest"),
        "distance_km", "co2_kg",
        F.lit(None).cast("string").alias("departure_time"),
        F.lit(None).cast("string").alias("arrival_time"),
        F.lit("airports").alias("source"),
        F.lit("OurAirports").alias("provider"),
    )


# ===========================
# 8) PIPELINE ORCHESTRATION
# ===========================
def run_transform():
    print("=" * 60)
    print("🚀 TRANSFORMATION - Pipeline datamart (train + avion) [LOW RAM]")
    print("=" * 60)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    ensure_dir(OUTPUT_DIR)

    df = None
    df_intermodal = None

    try:
        print("\n[1] Chargement des sources...")
        df_mob = read_all_mobility(spark)
        df_bot = read_backontrack(spark)
        df_air = read_airports(spark)

        routes_sources = [d for d in (df_mob, df_bot) if d is not None]
        if not routes_sources:
            print("❌ Aucune donnée trajets (Mobility/BackOnTrack vides).")
            return

        df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), routes_sources)

        # Sauvegarde intermédiaire après union pour libérer la RAM
        staging_union_path = os.path.join(OUTPUT_DIR, "staging_union.csv")
        print(f"[INFO] Sauvegarde intermédiaire après union → {staging_union_path}")
        df.write.mode("overwrite").option("header", "true").csv(staging_union_path)
        del df
        spark.catalog.clearCache()

        # Recharge le CSV pour repartir d'un DataFrame plat
        df = spark.read.option("header", "true").csv(staging_union_path)

        # Low-RAM: réduire tôt la largeur (moins de mémoire en shuffle)
        base_cols = [
            "origin","destination","vehicule_type",
            "station_lat","station_long","station_lat_dest","station_long_dest",
            "departure_time","arrival_time","shape_distance_km","source","provider"
        ]
        df = df.select(*[c for c in base_cols if c in df.columns])

        print("\n[2] Nettoyage + filtre grossier...")

        import gc
        df = validate_routes(df)
        validate_path = os.path.join(OUTPUT_DIR, "staging_validated.csv")
        df.write.mode("overwrite").option("header", "true").csv(validate_path)
        del df
        spark.catalog.clearCache(); gc.collect()
        df = spark.read.option("header", "true").csv(validate_path)

        df = normalize_station_col(df, "origin")
        norm_origin_path = os.path.join(OUTPUT_DIR, "staging_norm_origin.csv")
        df.write.mode("overwrite").option("header", "true").csv(norm_origin_path)
        del df
        spark.catalog.clearCache(); gc.collect()
        df = spark.read.option("header", "true").csv(norm_origin_path)

        df = normalize_station_col(df, "destination")
        norm_dest_path = os.path.join(OUTPUT_DIR, "staging_norm_dest.csv")
        df.write.mode("overwrite").option("header", "true").csv(norm_dest_path)
        del df
        spark.catalog.clearCache(); gc.collect()
        df = spark.read.option("header", "true").csv(norm_dest_path)

        # Cache disque seulement si on réutilise df plusieurs fois (distance + intermodal + plane)

        df = df.persist(StorageLevel.DISK_ONLY)
        _spark_warmup(df)

        print("\n[3] Calcul distance + CO2...")
        has_coords = all(c in df.columns for c in ("station_lat", "station_long", "station_lat_dest", "station_long_dest"))

        # Conversion explicite des coordonnées en DOUBLE (évite DATATYPE_MISMATCH)
        if has_coords:
            for coord_col in ["station_lat", "station_long", "station_lat_dest", "station_long_dest"]:
                df = df.withColumn(coord_col, F.col(coord_col).cast(DoubleType()))
        
        if has_coords:
            df = df.withColumn(
                "distance_km",
                haversine_distance(
                    F.col("station_lat"), F.col("station_long"),
                    F.col("station_lat_dest"), F.col("station_long_dest")
                )
            )
        else:
            df = df.withColumn("distance_km", F.lit(None).cast(DoubleType()))

        dist_path = os.path.join(OUTPUT_DIR, "staging_distance.csv")
        df.write.mode("overwrite").option("header", "true").csv(dist_path)
        del df
        spark.catalog.clearCache(); gc.collect()
        df = spark.read.option("header", "true").csv(dist_path)

        df = df.withColumn("co2_kg", co2_kg(F.col("distance_km"), F.col("vehicule_type")))
        co2_path = os.path.join(OUTPUT_DIR, "staging_co2.csv")
        df.write.mode("overwrite").option("header", "true").csv(co2_path)
        del df
        spark.catalog.clearCache(); gc.collect()
        df = spark.read.option("header", "true").csv(co2_path)

        print("\n[4] Intermodalité (gare -> aéroport proche)...")

        if df_air is not None and has_coords:
            df_intermodal = build_intermodal_links(df, df_air)
            if df_intermodal is not None:
                intermodal_tmp_dir = os.path.join(OUTPUT_DIR, "tmp_staging_intermodal_links")
                df_intermodal.write.mode("overwrite").option("header", "true").csv(intermodal_tmp_dir)
                out_inter = os.path.join(OUTPUT_DIR, OUTPUT_INTERMODAL_FILE)
                _merge_part_csvs(intermodal_tmp_dir, out_inter)
                del df_intermodal
                spark.catalog.clearCache(); gc.collect()
                df_intermodal = spark.read.option("header", "true").csv(out_inter)
                print(f"   ✅ Intermodal exporté : {out_inter}")
            else:
                print("   ⚠️ Intermodal : aucun lien généré")
        else:
            print("   ⚠️ Intermodal : airports ou coords manquants")

        print("\n[5] Génération trajets avion...")

        df_planes = generate_plane_routes(df, df_intermodal) if df_intermodal is not None else None
        if df_planes is not None:
            planes_path = os.path.join(OUTPUT_DIR, "staging_planes.csv")
            df_planes.write.mode("overwrite").option("header", "true").csv(planes_path)
            del df_planes
            spark.catalog.clearCache(); gc.collect()
            df_planes = spark.read.option("header", "true").csv(planes_path)
            df = df.unionByName(df_planes, allowMissingColumns=True)
            union_planes_path = os.path.join(OUTPUT_DIR, "staging_union_planes.csv")
            df.write.mode("overwrite").option("header", "true").csv(union_planes_path)
            del df
            spark.catalog.clearCache(); gc.collect()
            df = spark.read.option("header", "true").csv(union_planes_path)
            print("   ✅ Trajets avion ajoutés")
        else:
            print("   ⚠️ Aucun trajet avion généré")

        print("\n[6] Dédup + exports...")
        df = df.dropDuplicates(["origin", "destination", "vehicule_type", "source"])
        dedup_path = os.path.join(OUTPUT_DIR, "staging_dedup.csv")
        df.write.mode("overwrite").option("header", "true").csv(dedup_path)
        del df
        spark.catalog.clearCache(); gc.collect()
        df = spark.read.option("header", "true").csv(dedup_path)

        final_cols = [
            "origin", "destination", "vehicule_type",
            "station_lat", "station_long", "station_lat_dest", "station_long_dest",
            "distance_km", "co2_kg", "departure_time", "arrival_time",
            "source", "provider"
        ]
        df = df.select(*[c for c in final_cols if c in df.columns])

        # Export final en CSV unique (fusion des parts pour le load)
        out_routes = os.path.join(OUTPUT_DIR, OUTPUT_FINAL_FILE)
        tmp_routes_dir = os.path.join(OUTPUT_DIR, "tmp_final_routes")
        df.write.mode("overwrite").option("header", "true").csv(tmp_routes_dir)
        _merge_part_csvs(tmp_routes_dir, out_routes)
        print(f"   ✅ Routes exportées : {out_routes}")

        if df_air is not None:
            out_air = os.path.join(OUTPUT_DIR, OUTPUT_AIRPORTS_FILE)
            tmp_airports_dir = os.path.join(OUTPUT_DIR, "tmp_final_airports")
            df_air.write.mode("overwrite").option("header", "true").csv(tmp_airports_dir)
            _merge_part_csvs(tmp_airports_dir, out_air)
            print(f"   ✅ Aéroports exportés : {out_air}")

        print("\n🎉 Transformation terminée (LOW RAM).")

    finally:
        try:
            if df is not None:
                df.unpersist(blocking=False)
        except Exception:
            pass
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    run_transform()