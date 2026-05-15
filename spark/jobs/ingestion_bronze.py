from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, input_file_name, from_unixtime, year, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType
from datetime import datetime

# ============================================================
# CONFIGURACION
# ============================================================
MINIO_ENDPOINT  = "http://minio:9000"
MINIO_USER      = "minioadmin"
MINIO_PASSWORD  = "minioadmin123"
DATA_PATH       = "/home/jovyan/data"

spark = SparkSession.builder \
    .appName("ST1630-Bronze-Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",          MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key",          MINIO_PASSWORD) \
    .config("spark.hadoop.fs.s3a.path.style.access",   "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local",                 "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type",            "hadoop") \
    .config("spark.sql.catalog.local.warehouse",       "s3a://warehouse/") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# FUNCION: cargar un lote y hacer append en Bronze
# ============================================================
def ingest_batch(df, table_name, batch_id, description):
    print(f"\n>>> INICIANDO: {description}")

    df_with_meta = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file",         input_file_name()) \
        .withColumn("batch_id",            lit(batch_id))

    df_with_meta.writeTo(f"local.bronze.{table_name}") \
        .tableProperty("format-version", "2") \
        .append()

    count = spark.table(f"local.bronze.{table_name}").count()
    print(f"    Registros totales en bronze.{table_name}: {count:,}")
    print(f">>> COMPLETADO: {description}\n")

# ============================================================
# LEER ARCHIVOS CSV con esquemas explícitos
# ============================================================
ratings_schema = StructType([
    StructField("userId",    IntegerType(), True),
    StructField("movieId",   IntegerType(), True),
    StructField("rating",    DoubleType(),  True),
    StructField("timestamp", LongType(),    True)
])

movies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title",   StringType(),  True),
    StructField("genres",  StringType(),  True)
])

tags_schema = StructType([
    StructField("userId",    IntegerType(), True),
    StructField("movieId",   IntegerType(), True),
    StructField("tag",       StringType(),  True),
    StructField("timestamp", LongType(),    True)
])

ratings = spark.read.csv(f"{DATA_PATH}/ratings.csv", header=True, schema=ratings_schema)
movies  = spark.read.csv(f"{DATA_PATH}/movies.csv",  header=True, schema=movies_schema)
tags    = spark.read.csv(f"{DATA_PATH}/tags.csv",    header=True, schema=tags_schema)

total_ratings = ratings.count()

# ============================================================
# CREAR NAMESPACES Y TABLAS BRONZE (solo la primera vez)
# ============================================================
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.ratings (
        userId        INT,
        movieId       INT,
        rating        DOUBLE,
        timestamp     BIGINT,
        ingestion_timestamp TIMESTAMP,
        source_file   STRING,
        batch_id      STRING
    ) USING iceberg
    PARTITIONED BY (batch_id)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.movies (
        movieId   INT,
        title     STRING,
        genres    STRING,
        ingestion_timestamp TIMESTAMP,
        source_file         STRING,
        batch_id            STRING
    ) USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.tags (
        userId    INT,
        movieId   INT,
        tag       STRING,
        timestamp BIGINT,
        ingestion_timestamp TIMESTAMP,
        source_file         STRING,
        batch_id            STRING
    ) USING iceberg
""")

# ============================================================
# LIMPIAR TABLAS BRONZE (para idempotencia)
# ============================================================
print("\n>>> Limpiando tablas Bronze para garantizar ejecución limpia...")
spark.sql("DELETE FROM local.bronze.ratings")
spark.sql("DELETE FROM local.bronze.movies")
spark.sql("DELETE FROM local.bronze.tags")
print(">>> Tablas limpias.\n")

# ============================================================
# SPLIT DETERMINISTA POR TIMESTAMP
# ============================================================
CUTOFF_YEAR = 2018

ratings_with_year = ratings.withColumn(
    "rating_year", year(from_unixtime(col("timestamp")))
)

ratings_lote1 = ratings_with_year.filter(col("rating_year") <= CUTOFF_YEAR).drop("rating_year")
ratings_lote2 = ratings_with_year.filter(col("rating_year") > CUTOFF_YEAR).drop("rating_year")

count_lote1 = ratings_lote1.count()
count_lote2 = ratings_lote2.count()

print(f"Total ratings en el dataset: {total_ratings:,}")
print(f"Lote 1: ratings con rating_year <= {CUTOFF_YEAR}: {count_lote1:,}")
print(f"Lote 2: ratings con rating_year >  {CUTOFF_YEAR}: {count_lote2:,}")

# ============================================================
# LOTE 1 — ratings + movies + tags completos
# ============================================================
ingest_batch(ratings_lote1, "ratings", "lote_1_<=2018", "Lote 1: ratings hasta 2018")
ingest_batch(movies,        "movies",  "lote_1",           "Lote 1: movies completo")
ingest_batch(tags,          "tags",    "lote_1",           "Lote 1: tags completo")

# ============================================================
# TIME TRAVEL — ver snapshot antes del lote 2
# ============================================================
snapshots_antes = spark.sql("SELECT snapshot_id, committed_at FROM local.bronze.ratings.snapshots")
print("\nSnapshots de bronze.ratings ANTES del lote 2:")
snapshots_antes.show()

# ============================================================
# LOTE 2 — ratings posteriores al 2018
# ============================================================
ingest_batch(ratings_lote2, "ratings", "lote_2_>2018", "Lote 2: ratings posteriores a 2018")

# ============================================================
# VERIFICACIONES FINALES
# ============================================================
print("\n=== VERIFICACIONES BRONZE ===")

total_bronze = spark.table("local.bronze.ratings").count()
assert total_bronze == total_ratings, f"ERROR: se esperaban {total_ratings} pero hay {total_bronze}"
print(f"Integridad OK: {total_bronze:,} ratings en Bronze")

rangos = spark.table("local.bronze.ratings") \
    .filter("rating < 0.5 OR rating > 5.0").count()
assert rangos == 0, "ERROR: hay ratings fuera de rango"
print("Rangos OK: todos los ratings entre 0.5 y 5.0")

snaps = spark.sql("SELECT COUNT(*) as total FROM local.bronze.ratings.snapshots") \
    .collect()[0]["total"]
assert snaps >= 2, "ERROR: se esperaban al menos 2 snapshots"
print(f"Snapshots OK: {snaps} snapshots en bronze.ratings")

print("\n=== BRONZE COMPLETADO EXITOSAMENTE ===")
spark.stop()