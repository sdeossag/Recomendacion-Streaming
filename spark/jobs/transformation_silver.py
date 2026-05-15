from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, regexp_extract, when, isnan,
    current_timestamp, lit, from_unixtime, year, floor, broadcast
)

MINIO_ENDPOINT = "http://minio:9000"
MINIO_USER     = "minioadmin"
MINIO_PASSWORD = "minioadmin123"

spark = SparkSession.builder \
    .appName("ST1630-Silver-Transformation") \
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_PASSWORD) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local",               "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type",          "hadoop") \
    .config("spark.sql.catalog.local.warehouse",     "s3a://warehouse/") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# LEER BRONZE
# ============================================================
print("\n>>> Leyendo Bronze...")
ratings_raw = spark.table("local.bronze.ratings")
movies_raw  = spark.table("local.bronze.movies")
tags_raw    = spark.table("local.bronze.tags")

# ============================================================
# LIMPIAR Y TRANSFORMAR MOVIES
# ============================================================
print(">>> Transformando movies...")
movies_clean = movies_raw \
    .dropDuplicates(["movieId"]) \
    .withColumn("release_year",
        regexp_extract(col("title"), r"\((\d{4})\)$", 1).cast("int")) \
    .withColumn("decade",
        (floor(col("release_year") / 10) * 10).cast("int")) \
    .withColumn("genre_list",
        split(col("genres"), "\|")) \
    .withColumn("is_recent", col("release_year") > 2018) \
    .withColumn("title_clean",
        regexp_extract(col("title"), r"^(.+)\s\(\d{4}\)$", 1)) \
    .filter(col("movieId").isNotNull()) \
    .select("movieId", "title_clean", "genres", "genre_list", "release_year", "decade", "is_recent")

# ============================================================
# LIMPIAR Y TRANSFORMAR RATINGS
# ============================================================
print(">>> Transformando ratings...")
ratings_clean = ratings_raw \
    .dropDuplicates(["userId", "movieId", "timestamp"]) \
    .filter(col("rating").between(0.5, 5.0)) \
    .filter(col("userId").isNotNull()) \
    .filter(col("movieId").isNotNull()) \
    .withColumn("rating_date",
        from_unixtime(col("timestamp")).cast("timestamp")) \
    .withColumn("rating_year",
        year(col("rating_date"))) \
    .withColumn("rating_normalized",
        (col("rating") - 0.5) / (5.0 - 0.5)) \
    .select("userId", "movieId", "rating", "rating_normalized",
            "rating_date", "rating_year", "timestamp")

# ============================================================
# LIMPIAR TAGS
# ============================================================
print(">>> Transformando tags...")
from pyspark.sql.functions import lower, trim, length

tags_clean = tags_raw \
    .dropDuplicates(["userId", "movieId", "tag"]) \
    .filter(col("tag").isNotNull()) \
    .withColumn("tag", lower(trim(col("tag")))) \
    .filter(length(col("tag")) > 0) \
    .select("userId", "movieId", "tag", "timestamp")

# ============================================================
# JOIN: tabla Silver principal
# ratings + movies (LEFT JOIN para no perder ratings de peliculas
# que no esten en movies.csv)
# ============================================================
print(">>> Haciendo JOIN ratings + movies...")
silver_main = ratings_clean.join(
    broadcast(movies_clean),
    on="movieId",
    how="left"
)

print(f"    Registros en Silver principal: {silver_main.count():,}")

# ============================================================
# SCHEMA EVOLUTION: agregar columna nueva sin reescribir
# Esto demuestra el feature de Iceberg
# ============================================================
print(">>> Escribiendo Silver con Schema Evolution demo...")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.ratings_enriched (
        userId           INT,
        movieId          INT,
        rating           DOUBLE,
        rating_normalized DOUBLE,
        rating_date      TIMESTAMP,
        rating_year      INT,
        timestamp        BIGINT,
        title_clean      STRING,
        genres           STRING,
        genre_list       ARRAY<STRING>,
        release_year     INT,
        decade           INT,
        is_recent        BOOLEAN
    ) USING iceberg
    PARTITIONED BY (rating_year)
""")

silver_main.writeTo("local.silver.ratings_enriched").append()

# ============================================================
# TABLA DE TAGS EN SILVER
# ============================================================
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.tags_clean (
        userId  INT,
        movieId INT,
        tag     STRING,
        timestamp BIGINT
    ) USING iceberg
""")
tags_clean.writeTo("local.silver.tags_clean").append()

# ============================================================
# VERIFICACIONES
# ============================================================
print("\n=== VERIFICACIONES SILVER ===")
silver_count = spark.table("local.silver.ratings_enriched").count()
print(f"Registros en Silver: {silver_count:,}")

nulos = spark.table("local.silver.ratings_enriched") \
    .filter(col("userId").isNull() | col("movieId").isNull()).count()
assert nulos == 0, f"ERROR: hay {nulos} registros con userId o movieId nulo"
print("Nulos OK: no hay userId ni movieId nulos")

print("\n=== SILVER COMPLETADO EXITOSAMENTE ===")
spark.stop()