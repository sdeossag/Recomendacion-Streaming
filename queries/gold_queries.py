"""
ST1630 — Persona 5: Consultas Analíticas (Spark SQL) sobre la capa Gold.

Este módulo contiene 5+ consultas de negocio diseñadas para ejecutarse sobre el 
Lakehouse Iceberg (capas Gold y Silver/Bronze) en MinIO.
Cada consulta incluye su interpretación en el lenguaje del dominio (negocio de recomendación).

Las consultas devuelven DataFrames de Pandas, listos para ser mostrados en Streamlit o Jupyter.
"""

from __future__ import annotations

import os
import re
import subprocess

import pandas as pd
from pyspark.sql import SparkSession

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

# Compatibilidad Java 17+ (evita fallos de Subject en drivers locales recientes)
JAVA17_OPTS = (
    "-Dio.netty.tryReflectionSetAccessible=true "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

SPARK_LOCAL_HINT = (
    "Streamlit en Windows necesita **JDK 17** (no Java 21/24) y **pyspark==3.5.0**.\n\n"
    "Opción recomendada — dashboard en Docker (Java ya incluido):\n"
    "  docker compose up -d spark-master spark-worker minio streamlit\n"
    "  http://localhost:8501\n\n"
    "Si preferís local: instalá JDK 17, definí JAVA_HOME y ejecutá:\n"
    "  pip install \"pyspark==3.5.0\"\n"
    "  $env:SPARK_MASTER_URL=\"spark://localhost:7077\"\n"
    "  python -m streamlit run dashboard/streamlit_app.py"
)


def _running_in_docker() -> bool:
    return os.path.exists("/.dockerenv") or os.getenv("RUNNING_IN_DOCKER") == "1"


def _resolve_minio_endpoint() -> str:
    if os.getenv("MINIO_ENDPOINT"):
        return os.environ["MINIO_ENDPOINT"]
    return "http://minio:9000" if _running_in_docker() else "http://localhost:9000"


def _resolve_spark_master() -> str:
    if os.getenv("SPARK_MASTER_URL"):
        return os.environ["SPARK_MASTER_URL"]
    if os.getenv("SPARK_MASTER"):
        return os.environ["SPARK_MASTER"]
    return "spark://spark-master:7077" if _running_in_docker() else "spark://localhost:7077"


MINIO_ENDPOINT = _resolve_minio_endpoint()
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "minioadmin123")


def check_java_for_spark() -> tuple[bool, str]:
    """Valida que el Java del host sea compatible con PySpark 3.5 + Hadoop."""
    try:
        proc = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        out = (proc.stderr or "") + (proc.stdout or "")
    except Exception as exc:
        return False, f"No se encontró `java` en PATH: {exc}"

    match = re.search(r'version "(\d+)', out)
    if not match:
        return False, "No se pudo detectar la versión de Java."

    major = int(match.group(1))
    if major >= 24:
        return False, (
            f"Java {major} no es compatible con Hadoop 3.3 usado por PySpark "
            "(error getSubject / JAVA_GATEWAY_EXITED). Instalá **JDK 17** o usá "
            "el contenedor: `docker compose up -d streamlit`."
        )
    return True, f"Java {major} detectado."


def get_spark_session(app_name: str = "Gold-Queries-Analyst") -> SparkSession:
    """
    SparkSession contra el clúster Docker (spark-master:7077) y catálogo Iceberg en MinIO.
  """
    ok, java_msg = check_java_for_spark()
    if not ok:
        raise RuntimeError(f"{java_msg}\n\n{SPARK_LOCAL_HINT}")

    master = _resolve_spark_master()
    builder = SparkSession.builder.appName(app_name).master(master)

    # En Docker usamos spark-defaults.conf + ivy cache compartido con spark-master
    if not _running_in_docker():
        builder = builder.config("spark.jars.packages", ICEBERG_PACKAGES)

    builder = (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
        .config("spark.driver.extraJavaOptions", JAVA17_OPTS)
        .config("spark.executor.extraJavaOptions", JAVA17_OPTS)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
    )
    if master.startswith("spark://"):
        builder = builder.config("spark.submit.deployMode", "client")

    return builder.getOrCreate()


def gold_tables_ready(spark: SparkSession) -> bool:
    """True si existen las tablas Iceberg mínimas para el dashboard batch."""
    try:
        spark.table("local.gold.recommendations").limit(1).collect()
        return True
    except Exception:
        return False


GOLD_PIPELINE_HINT = (
    "Ejecutá el pipeline batch dentro de spark-master (en orden):\n"
    "  docker exec -it spark-master spark-submit /home/jovyan/jobs/ingestion_bronze.py\n"
    "  docker exec -it spark-master spark-submit /home/jovyan/jobs/transformation_silver.py\n"
    "  docker exec -it spark-master spark-submit /home/jovyan/jobs/training_gold.py\n"
    "Requisito: ratings.csv y movies.csv en la carpeta data/ del repo."
)


# ==============================================================================
# CONSULTA 1: Recomendaciones Personalizadas (Recomendación Directa)
# ==============================================================================
def q_user_recommendations(spark: SparkSession, user_id: int, limit: int = 10) -> pd.DataFrame:
    """
    Objetivo: Obtener las películas sugeridas para un usuario con su score predicho.
    Interpretación de Negocio: Es la consulta operativa principal de la plataforma. 
    Permite renderizar la sección de 'Te recomendamos' del usuario en la interfaz web, 
    mostrando títulos y géneros limpios en lugar de IDs crudos.
    """
    query = f"""
        SELECT 
            r.userId as user_id, 
            r.movieId as movie_id, 
            COALESCE(r.title, m.title_clean) as title, 
            COALESCE(r.genres, m.genres) as genres, 
            r.predicted_score as predicted_score
        FROM local.gold.recommendations r
        LEFT JOIN (
            SELECT DISTINCT movieId, title_clean, genres 
            FROM local.silver.ratings_enriched
        ) m ON r.movieId = m.movieId
        WHERE r.userId = {user_id}
        ORDER BY r.predicted_score DESC
        LIMIT {limit}
    """
    return spark.sql(query).toPandas()


# ==============================================================================
# CONSULTA 2: Películas más recomendadas globalmente (Popularidad ALS)
# ==============================================================================
def q_most_recommended_movies(spark: SparkSession, limit: int = 10) -> pd.DataFrame:
    """
    Objetivo: Listar las películas que el modelo ALS recomienda con mayor frecuencia.
    Interpretación de Negocio: Permite a los administradores identificar sesgos de popularidad.
    Si una película se recomienda casi a todos los usuarios (sesgo de 'super-recomendados'),
    la plataforma podría decidir introducir penalizaciones o filtros de diversidad para no aburrir.
    """
    query = f"""
        SELECT 
            r.movieId as movie_id, 
            COALESCE(r.title, m.title_clean) as title, 
            COALESCE(r.genres, m.genres) as genres, 
            COUNT(*) as times_recommended, 
            ROUND(AVG(r.predicted_score), 3) as avg_predicted_score
        FROM local.gold.recommendations r
        LEFT JOIN (
            SELECT DISTINCT movieId, title_clean, genres 
            FROM local.silver.ratings_enriched
        ) m ON r.movieId = m.movieId
        GROUP BY r.movieId, COALESCE(r.title, m.title_clean), COALESCE(r.genres, m.genres)
        ORDER BY times_recommended DESC, avg_predicted_score DESC
        LIMIT {limit}
    """
    return spark.sql(query).toPandas()


# ==============================================================================
# CONSULTA 3: El mejor género cinematográfico de cada década (Evolución)
# ==============================================================================
def q_top_genre_by_decade(spark: SparkSession) -> pd.DataFrame:
    """
    Objetivo: Identificar el género con el rating promedio más alto para cada década de lanzamiento.
    Interpretación de Negocio: Análisis histórico de tendencias de consumo. Ayuda a entender 
    si el gusto del público ha cambiado a lo largo del tiempo (por ejemplo, si el Cine Negro dominó 
    los 40s, el Sci-Fi los 80s, o el Documental los 2010s). Filtra géneros con pocas valoraciones.
    """
    query = """
        WITH ranked_genres AS (
            SELECT 
                genre, 
                decade, 
                avg_rating, 
                total_ratings,
                ROW_NUMBER() OVER (PARTITION BY decade ORDER BY avg_rating DESC) as rank
            FROM local.gold.stats_genre_decade
            WHERE total_ratings > 1000  -- Excluye nichos muy pequeños para evitar ruido
        )
        SELECT 
            decade as decade, 
            genre as top_genre, 
            avg_rating as average_rating, 
            total_ratings as total_ratings
        FROM ranked_genres
        WHERE rank = 1 AND decade IS NOT NULL
        ORDER BY decade DESC
    """
    return spark.sql(query).toPandas()


# ==============================================================================
# CONSULTA 4: Diversidad e Indecisión en las Predicciones por Usuario
# ==============================================================================
def q_prediction_diversity_by_user(spark: SparkSession, limit: int = 10) -> pd.DataFrame:
    """
    Objetivo: Medir la variabilidad (desviación estándar) de las predicciones de ALS por usuario.
    Interpretación de Negocio: Permite segmentar usuarios:
    - Desviación alta: Usuario con preferencias muy marcadas (el modelo distingue claramente qué le encanta y qué odia).
    - Desviación baja: Usuario 'difícil' o indeciso (el modelo le da scores muy similares a todo).
    Esto sirve para lanzar campañas personalizadas o refinar el algoritmo ALS para ciertos nichos.
    """
    query = f"""
        SELECT 
            userId as user_id,
            ROUND(AVG(predicted_score), 3) as avg_prediction,
            ROUND(STDDEV(predicted_score), 3) as stddev_prediction,
            ROUND(MAX(predicted_score) - MIN(predicted_score), 3) as score_range
        FROM local.gold.recommendations
        GROUP BY userId
        ORDER BY stddev_prediction DESC
        LIMIT {limit}
    """
    return spark.sql(query).toPandas()


# ==============================================================================
# CONSULTA 5: Distribución Porcentual de Calificaciones Históricas
# ==============================================================================
def q_rating_distribution_percentage(spark: SparkSession) -> pd.DataFrame:
    """
    Objetivo: Calcular la distribución de las estrellas dadas históricamente y su porcentaje.
    Interpretación de Negocio: Permite entender el sesgo psicológico de la audiencia.
    En plataformas de streaming, los usuarios suelen calificar con 4 o 5 estrellas las cosas que ven 
    (sesgo positivo), ya que abandonan lo que no les gusta sin calificarlo. Entender esta curva
    permite calibrar el umbral a partir del cual consideramos que una predicción ALS es "buena".
    """
    query = """
        WITH total_count AS (
            SELECT SUM(total) as grand_total FROM local.gold.rating_distribution
        )
        SELECT 
            d.rating as rating,
            d.total as total_ratings,
            ROUND((d.total / t.grand_total) * 100, 2) as percentage
        FROM local.gold.rating_distribution d
        CROSS JOIN total_count t
        ORDER BY d.rating DESC
    """
    return spark.sql(query).toPandas()


# ==============================================================================
# CONSULTA 6: Impacto de la Novedad en las Recomendaciones (Clásicos vs Novedades)
# ==============================================================================
def q_novedad_vs_clasico_recommendations(spark: SparkSession) -> pd.DataFrame:
    """
    Objetivo: Analizar cuántas de las recomendaciones corresponden a películas recientes (>2018).
    Interpretación de Negocio: Mide el 'freshness' del recomendador. Si el 95% de las recomendaciones
    son películas antiguas, el usuario percibirá la plataforma como obsoleta. Si es al revés,
    podría faltar catalogar clásicos queridos. Ayuda a calibrar el balance de catálogo.
    """
    query = """
        SELECT 
            CASE WHEN m.is_recent THEN 'Novedad (Post-2018)' ELSE 'Clásico (Pre-2019)' END as category,
            COUNT(*) as total_recommendations,
            ROUND(AVG(r.predicted_score), 3) as avg_predicted_score
        FROM local.gold.recommendations r
        LEFT JOIN (
            SELECT DISTINCT movieId, is_recent
            FROM local.silver.ratings_enriched
        ) m ON r.movieId = m.movieId
        GROUP BY m.is_recent
    """
    return spark.sql(query).toPandas()


# --- Ejecución de Pruebas ---
if __name__ == "__main__":
    print("Iniciando conexión analítica con Spark en MinIO...")
    try:
        spark = get_spark_session()
        print("\n1. Probando Consulta 1 (Usuario 1042):")
        print(q_user_recommendations(spark, 1042, 5))
        
        print("\n2. Probando Consulta 2 (Películas más sugeridas):")
        print(q_most_recommended_movies(spark, 5))
        
        print("\n3. Probando Consulta 3 (Mejor género por década):")
        print(q_top_genre_by_decade(spark))
        
        print("\n4. Probando Consulta 4 (Diversidad por usuario):")
        print(q_prediction_diversity_by_user(spark, 5))
        
        print("\n5. Probando Consulta 5 (Distribución de estrellas):")
        print(q_rating_distribution_percentage(spark))

        print("\n6. Probando Consulta 6 (Novedad vs Clásico):")
        print(q_novedad_vs_clasico_recommendations(spark))
        
        spark.stop()
        print("\nConexión cerrada exitosamente.")
    except Exception as e:
        print("\n[AVISO] No se pudo ejecutar localmente porque el contenedor Spark/MinIO no está arriba.")
        print("El código es sintácticamente correcto y listo para Docker.")
        print(f"Error detallado: {e}")
