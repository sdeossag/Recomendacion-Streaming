# ST1630 — Sistema híbrido de recomendación y monitoreo en tiempo real

**Proyecto Final | Sistemas Intensivos en Datos | Sexto Semestre 2026**

**Equipo:**
- Samuel Henao Castrillón
- Samuel Deossa Gómez ← Persona 1 (Infraestructura y Kafka)
- Juan José Gómez Ramírez
- Samuel Herrera Hoyos
- Abraham Elías Navarro Martínez

---

## Estructura del repositorio

```
Recomendacion-Streaming/
├── docker-compose.yml          ← Persona 1: levanta toda la infraestructura
├── event_simulator.py          ← Persona 1: produce eventos a Kafka
├── README.md                   ← este archivo
├── .gitignore
│
├── spark/
│   └── jobs/                   ← Persona 3: ingestion_bronze.py, transformation_silver.py, training_gold.py
│
├── flink/
│   └── jobs/                   ← Persona 2: flink_streaming_job.py
│
├── notebooks/                  ← comparativa_sql_nosql.ipynb (Mongo vs PostgreSQL)
├── queries/                    ← mongo_queries.py, gold_queries.py
├── Evidencias_mongodb_comparativa.txt  ← benchmarks Mongo vs SQL
├── dashboard/                  ← Persona 5: streamlit_app.py
└── data/                       ← Dataset MovieLens (NO sube a GitHub, cada uno lo descarga)
```

---

## Prerequisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop) instalado y corriendo (motor en verde)
- [Git](https://git-scm.com) instalado
- Python 3.8 o superior
- 8GB RAM mínimo disponibles para Docker
- 20GB espacio libre en disco
- Si usas Windows + WSL2, recomienda asignar al menos 12GB en `.wslconfig` para Spark/ALS. Con menos memoria Docker puede rechazar recursos aunque el worker parezca levantado.

---

## Cómo levantar la infraestructura (TODOS los compañeros)

### Paso 1 — Clonar el repositorio (solo la primera vez)

```bash
git clone https://github.com/sdeossag/Recomendacion-Streaming.git
cd Recomendacion-Streaming
```

### Paso 2 — Levantar todos los servicios

```bash
docker-compose up -d
```

La primera vez descarga imágenes (~5GB). Tarda entre 5 y 15 minutos según tu internet.

> Nota: Kafka UI quedó publicado en `http://localhost:8085` para evitar conflicto con otros procesos en Windows.

### Paso 3 — Verificar que todo está corriendo

```bash
docker-compose ps
```

Deben aparecer estos contenedores en estado `running` o `healthy`:

| Contenedor | Imagen |
|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 |
| kafka | confluentinc/cp-kafka:7.5.0 |
| kafka-ui | provectuslabs/kafka-ui:latest |
| minio | minio/minio:latest |
| spark-master | jupyter/pyspark-notebook:spark-3.5.0 |
| flink-jobmanager | flink-python:1.18-scala_2.12 (compilado automático) |
| flink-taskmanager | flink-python:1.18-scala_2.12 (compilado automático) |
| mongodb | mongo:7.0 |
| postgresql | postgres:16 |
| streamlit | build local desde dashboard/Dockerfile |

Los contenedores `kafka-init`, `minio-init` y `mongodb-init` aparecen, hacen su trabajo (crear topics, buckets y colecciones) y se cierran solos. Eso es normal.

Para verificar el estado real:

```bash
docker-compose ps
```

### Paso 4 — Detener los servicios al terminar el día

```bash
docker-compose stop
```

Los datos se conservan. Para volver a levantar: `docker-compose up -d`

---

## Interfaces web disponibles

| URL | Servicio | Credenciales |
|---|---|---|
| http://localhost:8085 | Kafka UI — ver mensajes en tiempo real | Sin login |
| http://localhost:8082 | Flink UI — ver jobs de streaming | Sin login |
| http://localhost:8888 | Jupyter Lab + PySpark | Sin contraseña |
| http://localhost:4040 | Spark UI — aparece solo cuando hay un job activo | Sin login |
| http://localhost:9001 | MinIO consola — ver buckets y archivos | minioadmin / minioadmin123 |
| http://localhost:8501 | Streamlit Dashboard — app interactiva | Sin login |

---

## Credenciales de servicios

| Servicio | Host (externo) | Host (interno contenedor) | Usuario | Contraseña | Base de datos |
|---|---|---|---|---|---|
| Kafka | localhost:29092 | kafka:9092 | — | — | — |
| MinIO API | localhost:9000 | minio:9000 | minioadmin | minioadmin123 | — |
| MongoDB | localhost:27017 | mongodb:27017 | — | — | streaming_results |
| PostgreSQL | localhost:5432 | postgresql:5432 | postgres | postgres123 | movielens |

---

## Cómo correr el simulador de eventos (Persona 1)

El simulador genera eventos de usuarios interactuando con la plataforma y los envía al topic `platform-events` de Kafka. Se puede correr de dos formas:

### Opción A — Ejecución limpia en Docker (Recomendada - Sin instalar nada local)
Esta opción es excelente porque no requiere tener Python o pip instalados en tu sistema local:

```bash
# 1. Copiar el simulador a la carpeta compartida con Spark
cp event_simulator.py spark/jobs/

# 2. Ejecutar el simulador dentro del contenedor de Spark pointing al broker de Docker
docker exec -it spark-master bash -c "pip install kafka-python && KAFKA_BOOTSTRAP_SERVERS=kafka:9092 python3 /home/jovyan/jobs/event_simulator.py"
```

### Opción B — Ejecución local (Requiere Python y pip en tu PC)
```bash
# Instalar dependencia (solo la primera vez)
pip install kafka-python

# Correr el simulador apuntando a localhost:29092
python event_simulator.py
```

Para verificar que los mensajes llegan: abrir http://localhost:8085 → Topics → platform-events → Messages.

Para detener: `Ctrl+C`

---

## Cómo correr el Job de Streaming en Flink (Persona 2 y 5)

El procesamiento de streaming en tiempo real se ejecuta en Apache Flink. Con las últimas automatizaciones, la imagen de Flink compila con soporte de PyFlink y Kafka de forma transparente.

Para iniciar el Job de streaming:

```bash
# Enviar el job al clúster Flink (script en flink/jobs/)
docker exec -it flink-jobmanager flink run -d -py /opt/flink/jobs/flink_streaming_job.py
```

Para monitorear el estado, reintentos y ver el flujo gráfico, abre **http://localhost:8082** en tu navegador.

---

## Schema del evento JSON (acordado con todo el equipo — no cambiar)

```json
{
  "event_id":    "uuid-string",
  "event_type":  "rating | play_start | play_pause | play_stop | add_to_favorites | search",
  "user_id":     1042,
  "movie_id":    318,
  "movie_title": "The Shawshank Redemption",
  "genres":      ["Drama"],
  "timestamp":   "2026-05-12T21:33:25.738Z",
  "session_id":  "sess-1042-2026051221",

  "rating": 4.5,
  "watch_duration_seconds": 3600
}
```

`rating` solo aparece si `event_type == "rating"`.
`watch_duration_seconds` solo aparece si `event_type == "play_stop"` o `"play_pause"`.

---

## Cómo descargar el dataset MovieLens 32M 

```bash
cd data

# Descargar (~850MB)
wget https://files.grouplens.org/datasets/movielens/ml-32m.zip

# En Windows (PowerShell):
Invoke-WebRequest -Uri https://files.grouplens.org/datasets/movielens/ml-32m.zip -OutFile ml-32m.zip

# Descomprimir
# Linux/Mac:
unzip ml-32m.zip
# Windows: clic derecho → Extraer aquí

# Archivos resultantes:
# data/ml-32m/ratings.csv   → 32,000,204 calificaciones
# data/ml-32m/movies.csv    → 87,585 peliculas con generos
# data/ml-32m/tags.csv      → 2,000,072 tags
# data/ml-32m/links.csv     → links a IMDB y TMDB
```

Los CSV **no se suben a GitHub** (estan en .gitignore). Cada persona los descarga localmente.

---

## Conexiones por persona

### Persona 2 — Flink

```python
# Kafka desde DENTRO del contenedor de Flink
bootstrap_servers = "kafka:9092"
topic = "platform-events"

# MongoDB desde DENTRO del contenedor de Flink
mongo_uri = "mongodb://mongodb:27017"
database  = "streaming_results"
```

### Persona 3 — Spark (Jupyter)

Abrir http://localhost:8888, crear un notebook o usar `spark-submit` dentro del contenedor.

**Spark ya queda configurado con Iceberg y MinIO** mediante `spark/conf/spark-defaults.conf`.

Para ejecutar un script de Persona 3:

```bash
docker exec -it spark-master spark-submit /home/jovyan/jobs/ingestion_bronze.py
docker exec -it spark-master spark-submit /home/jovyan/jobs/transformation_silver.py
docker exec -it spark-master spark-submit /home/jovyan/jobs/training_gold.py
```

En Jupyter:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ST1630") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .getOrCreate()

# Rutas de las capas
bronze_path = "s3a://bronze/ratings"
silver_path = "s3a://silver/ratings_clean"
gold_path   = "s3a://gold/recommendations"
```

Los scripts van en `spark/jobs/` y aparecen en Jupyter bajo la carpeta `jobs/`.

### MongoDB y comparativa SQL vs NoSQL

Consultas operacionales, índices y benchmarks en [`queries/mongo_queries.py`](queries/mongo_queries.py). Comparativa empírica con PostgreSQL en [`notebooks/comparativa_sql_nosql.ipynb`](notebooks/comparativa_sql_nosql.ipynb). Resumen de mediciones: [`Evidencias_mongodb_comparativa.txt`](Evidencias_mongodb_comparativa.txt).

| Capa | Uso de MongoDB |
|------|----------------|
| **Streaming (Flink)** | Resultados en tiempo real: trending, género, anomalías |
| **Batch (réplica Gold)** | Materialización de recomendaciones por usuario para pruebas de lectura PK |
| **Dashboard** | Pestaña **Streaming en Vivo** en Streamlit vía `mongo_queries.py` |
| **Comparativa** | Misma carga y mismas preguntas en MongoDB y PostgreSQL |

Base de datos: **`streaming_results`** en `mongodb://localhost:27017`.

#### Colecciones y origen de datos

| Colección | Escritor | Uso |
|-----------|--------|-----|
| `trending_movies` | Flink (ventana 5 min) | Top películas por `rating_count` en la última ventana |
| `genre_activity` | Flink (ventana deslizante) | Eventos agregados por `genre` |
| `anomaly_alerts` | Flink (sesión) | Usuarios con >20 ratings en 2 min (`RATING_BURST`) |
| `gold_user_recommendations` | Notebook comparativa | 1 documento por `user_id` con lista `items[]` (réplica Gold para PK) |

#### Consultas operacionales implementadas (≥3)

Definidas en `mongo_queries.py` y usadas en el notebook / dashboard:

1. **`q_trending_top_k_latest_window(db, k)`** — Top-K en la ventana con mayor `window_end`.
2. **`q_genre_most_active_in_recent_windows(db, last_n_windows)`** — Pipeline `$group` + `$sum` por género.
3. **`q_anomaly_alerts_for_user(db, user_id)`** — Historial de alertas por usuario.
4. **`q_recommendations_for_user(db, user_id)`** — Lectura por clave lógica en `gold_user_recommendations`.

Índices alineados a esas consultas: `ensure_operational_indexes(db)` (trending, genre, anomaly, gold PK única).

#### Resultados concretos — benchmark SQL vs NoSQL

Ejecutado en `notebooks/comparativa_sql_nosql.ipynb` (protocolo: **10 repeticiones**, **2 warm-ups**, latencias en **ms**, throughput en **filas/s**). Salida guardada en el propio notebook.

**Latencias (media ± desviación estándar):**

| Experimento | MongoDB (ms) | PostgreSQL (ms) |
|-------------|--------------|-----------------|
| Lectura por `user_id` (clave lógica / PK) | 1.46 ± 0.31 | **0.36 ± 0.07** |
| Filtro + `SUM` por género | 1.66 ± 0.21 | **1.33 ± 0.15** |
| JOIN recomendaciones ↔ dimensión película | 1.76 ± 0.27 | **0.70 ± 0.14** |

**Throughput — inserción masiva de 20 000 filas (10 corridas):**

| Motor | Filas/segundo (media) | σ |
|-------|------------------------|---|
| MongoDB (`insert_many` por lotes) | **102 993** | 13 226 |
| PostgreSQL (`execute_batch`) | 43 598 | 4 577 |

**Conclusiones documentadas (muestra local Docker):**

- **Lectura puntual por usuario:** PostgreSQL fue más rápido con índice B-tree; Mongo compensa con un solo round-trip cuando el documento ya está materializado por usuario.
- **Agregación por género:** Rendimiento similar; ambos se benefician de filtro por ventana temporal.
- **JOIN:** PostgreSQL fue ~2.5× más rápido; en Mongo el equivalente es `$lookup` (más costoso que JOIN relacional).
- **Carga masiva:** Mongo alcanzó ~2.4× el throughput de PostgreSQL en esta prueba con lotes de 1 500 filas.

#### Cómo reproducir

```bash
# 1. Infraestructura
docker compose up -d mongodb postgresql

# 2. Dependencias locales (notebook)
pip install pymongo psycopg2-binary pandas matplotlib

# 3. Notebook completo (consultas + benchmarks + gráficos)
jupyter notebook notebooks/comparativa_sql_nosql.ipynb
```

Consultas rápidas desde Python (streaming ya con datos de Flink):

```python
from queries.mongo_queries import (
    get_db, ensure_operational_indexes,
    q_trending_top_k_latest_window,
    q_genre_most_active_in_recent_windows,
    q_anomaly_alerts_for_user,
)

db = get_db("mongodb://localhost:27017", "streaming_results")
ensure_operational_indexes(db)
print(q_trending_top_k_latest_window(db, k=5))
print(q_genre_most_active_in_recent_windows(db, last_n_windows=10))
print(q_anomaly_alerts_for_user(db, user_id=179, limit=5))
```

PostgreSQL (réplica relacional para la comparativa):

```python
import psycopg2
conn = psycopg2.connect(
    host="localhost", port=5432,
    database="movielens", user="postgres", password="postgres123",
)
```


### Persona 5 — Streamlit

El dashboard ya está dockerizado y arranca automáticamente con `docker-compose up`.
Puedes ver los cambios en tiempo real editando el archivo `dashboard/streamlit_app.py`, ya que la carpeta está montada como volumen.

Para abrir la aplicación, ingresa en tu navegador a:
**http://localhost:8501**

---

## Flujo Git — cómo trabajar en equipo

### Al empezar el día (SIEMPRE primero):
```bash
git pull origin main
```

### Al terminar de trabajar:
```bash
git add .
git commit -m "feat: descripción de lo que hiciste"
git push origin main
```

### Convención de mensajes:
- `feat:` nueva funcionalidad
- `fix:` corrección de error
- `docs:` cambio en documentación

Cada persona trabaja en su propia carpeta → no hay conflictos.

---

## Demo en vivo — orden de ejecución

1. `docker-compose up -d` — levantar infraestructura
2. Abrir http://localhost:8085 — Kafka UI vacío
3. `python event_simulator.py` — mensajes llegando a Kafka en tiempo real
4. Abrir http://localhost:8085 → Topics → platform-events → Messages — ver eventos
5. Abrir http://localhost:8082 — Flink UI con job de streaming corriendo
6. Abrir http://localhost:8888 — correr pipeline batch de Spark (Bronze → Silver → Gold)
7. Abrir http://localhost:9001 — ver archivos en MinIO (capas bronze, silver, gold)
8. Mostrar dashboard Streamlit con recomendaciones + trending en tiempo real
9. Streamlit *Streaming en Vivo* (Mongo) + notebook `comparativa_sql_nosql.ipynb` (latencias, throughput; ver `Evidencias_mongodb_comparativa.txt`)

---

## Notas técnicas de Persona 3

- `spark-defaults.conf` monta Iceberg + S3A + catálogo `local` para que Spark reconozca las tablas.
- Los jobs de Spark viven en `spark/jobs/` y se ven en Jupyter como `/home/jovyan/jobs`.
- El dataset MovieLens debe estar en `data/`; el contenedor lo monta como `/home/jovyan/data`.
- Si usas Git Bash en Windows, prefiere `docker exec -it spark-master spark-submit ...` desde PowerShell para evitar problemas de rutas.
- En Windows/WSL2, si Gold falla con mensajes como `App requires more resource than any of Workers could have` o el worker se desconecta, sube la memoria de WSL a 12GB en `.wslconfig`. Esa fue la corrección que estabilizó el ALS y permitió completar Gold.

---

## Tabla de puntos del proyecto

| Sección | Descripción | Puntos |
|---|---|---|
| 4.1 | Descripción del problema | 18 |
| 4.2 | Arquitectura e infraestructura (Docker Compose) | 12 |
| 4.3 | Capa de mensajería Kafka | 12 |
| 4.4 | Procesamiento streaming con Flink | 15 |
| 4.5 | Base de datos NoSQL | 12 |
| 4.6 | Pipeline batch Spark + Lakehouse Iceberg | 15 |
| 4.7 | Integración y consultas analíticas | 10 |
| 4.8 | Calidad del código y documentación | 5 |
| 4.9 | Presentación y demo en vivo | 8 |
| **TOTAL** | | **100** |

**Bonificaciones disponibles:**
- Trino como motor SQL adicional sobre Gold: +2 puntos
- Makefile que ejecute todo en un comando: +1 punto
- Spark Structured Streaming como complemento a Flink: +1 punto
- Jupyter Notebook de análisis exploratorio: +1 punto