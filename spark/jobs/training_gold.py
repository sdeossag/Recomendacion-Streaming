from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round, explode, explode as explode_arr
from pyspark.ml.recommendation import ALS

MINIO_ENDPOINT = "http://minio:9000"
MINIO_USER     = "minioadmin"
MINIO_PASSWORD = "minioadmin123"

spark = SparkSession.builder \
    .appName("ST1630-Gold-Training") \
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

print(">>> Limpiando tablas Gold para garantizar ejecucion limpia...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
for tabla in ["local.gold.recommendations", "local.gold.stats_genre_decade", "local.gold.rating_distribution"]:
    try:
        spark.sql(f"DELETE FROM {tabla}")
        print(f"    {tabla} limpiada")
    except Exception:
        print(f"    {tabla} no existe aun, se creara")

# ============================================================
# LEER SILVER
# ============================================================
print("\n>>> Leyendo Silver...")
silver = spark.table("local.silver.ratings_enriched") \
    .repartition(12, col("rating_year"))

# ============================================================
# ENTRENAR MODELO ALS
# ALS = Alternating Least Squares
# Es el algoritmo estandar de filtrado colaborativo.
# Encuentra patrones latentes entre usuarios y peliculas.
# ============================================================
print(">>> Entrenando modelo ALS...")
print("    Esto puede tomar 10-20 minutos con 32M de registros...")

SAMPLE_FRACTION = 0.1
train_data = silver.select("userId", "movieId", "rating") \
    .sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)

als = ALS(
    maxIter=3,
    regParam=0.1,
    rank=10,
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)

model = als.fit(train_data)
print("    Modelo ALS entrenado correctamente")

# ============================================================
# GENERAR RECOMENDACIONES TOP 10 POR USUARIO
# ============================================================
print(">>> Generando recomendaciones para todos los usuarios...")
recomendaciones = model.recommendForAllUsers(10)

recom_flat = recomendaciones \
    .select("userId", explode("recommendations").alias("rec")) \
    .select(
        col("userId"),
        col("rec.movieId").alias("movieId"),
        spark_round(col("rec.rating"), 4).alias("predicted_score")
    )

print(f"    Recomendaciones generadas: {recom_flat.count():,} filas")

# ============================================================
# GUARDAR RECOMENDACIONES EN GOLD
# ============================================================
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.recommendations (
        userId          INT,
        movieId         INT,
        title           STRING,
        genres          STRING,
        predicted_score DOUBLE
    ) USING iceberg
""")

# Enriquecer recomendaciones con título y géneros para que el dashboard pueda mostrarlos
print("    Enriqueciendo recomendaciones con títulos desde Silver...")
movies_lookup = spark.table("local.silver.ratings_enriched").select("movieId", "title_clean", "genres").dropDuplicates(["movieId"])
recom_with_meta = recom_flat.join(movies_lookup, on="movieId", how="left") \
    .select(
        col("userId"),
        col("movieId"),
        col("title_clean").alias("title"),
        col("genres"),
        col("predicted_score")
    )

recom_with_meta.writeTo("local.gold.recommendations").append()
print("    Recomendaciones enriquecidas y guardadas en Gold")

# ============================================================
# TABLA GOLD: estadisticas por genero y decada
# Responde la pregunta analitica del enunciado
# ============================================================
print(">>> Calculando estadisticas por genero y decada...")

stats_genre_decade = silver \
    .filter(col("genre_list").isNotNull()) \
    .withColumn("genre", explode_arr(col("genre_list"))) \
    .filter(col("decade").isNotNull()) \
    .groupBy("genre", "decade") \
    .agg(
        spark_round(avg("rating"), 3).alias("avg_rating"),
        count("*").alias("total_ratings")
    ) \
    .orderBy("genre", "decade")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.stats_genre_decade (
        genre         STRING,
        decade        INT,
        avg_rating    DOUBLE,
        total_ratings BIGINT
    ) USING iceberg
""")

stats_genre_decade.writeTo("local.gold.stats_genre_decade").append()
print("    Estadisticas por genero y decada guardadas en Gold")

# ============================================================
# TABLA GOLD: distribucion de ratings
# ============================================================
dist_ratings = silver \
    .groupBy("rating") \
    .agg(count("*").alias("total")) \
    .orderBy("rating")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.rating_distribution (
        rating DOUBLE,
        total  BIGINT
    ) USING iceberg
""")

dist_ratings.writeTo("local.gold.rating_distribution").append()

# ============================================================
# VERIFICACION FINAL: TIME TRAVEL en Bronze
# ============================================================
print("\n>>> Demostrando Time Travel en Bronze...")
snapshots = spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM local.bronze.ratings.snapshots
    ORDER BY committed_at
""")
snapshots.show()

primer_snapshot = snapshots.collect()[0]["snapshot_id"]
count_antes = spark.sql(f"""
    SELECT COUNT(*) as total
    FROM local.bronze.ratings
    VERSION AS OF {primer_snapshot}
""").collect()[0]["total"]
print(f"    Registros en snapshot 1 (lote 1 solamente): {count_antes:,}")

count_ahora = spark.table("local.bronze.ratings").count()
print(f"    Registros actuales (lote 1 + lote 2): {count_ahora:,}")
print("    Time Travel OK: se puede ver el estado anterior")

print("\n=== GOLD COMPLETADO EXITOSAMENTE ===")
spark.stop()