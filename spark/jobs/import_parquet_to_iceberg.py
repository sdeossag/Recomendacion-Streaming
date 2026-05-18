from pyspark.sql import SparkSession
import os
import sys

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "minioadmin123")

def get_spark(app_name="ImportParquetToIceberg"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
        .getOrCreate()
    )


def main():
    if len(sys.argv) < 2:
        print("Uso: spark-submit import_parquet_to_iceberg.py <s3a://path/to/parquet> [local.schema.table]")
        sys.exit(1)

    parquet_path = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else "local.gold.recommendations"

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Leyendo Parquet desde: {parquet_path}")
    df = spark.read.parquet(parquet_path)
    print(f"Registros leidos: {df.count():,}")

    # Crear o anexar a la tabla Iceberg
    try:
        spark.table(table_name).limit(1).collect()
        exists = True
    except Exception:
        exists = False

    if not exists:
        print(f"Tabla {table_name} no existe. Creando tabla Iceberg y registrando datos...")
        df.writeTo(table_name).create()
        print("Tabla creada y datos registrados en Iceberg.")
    else:
        print(f"Tabla {table_name} existe. Appending datos...")
        df.writeTo(table_name).append()
        print("Append completado.")

    spark.stop()


if __name__ == "__main__":
    main()
