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
├── notebooks/                  ← Persona 4: comparativa_sql_nosql.ipynb
├── queries/                    ← Personas 4 y 5: mongo_queries.py, gold_queries.py
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
| flink-jobmanager | flink:1.18-scala_2.12 |
| flink-taskmanager | flink:1.18-scala_2.12 |
| mongodb | mongo:7.0 |
| postgresql | postgres:16 |

Los contenedores `kafka-init`, `minio-init` y `mongodb-init` aparecen, hacen su trabajo (crear topics, buckets y colecciones) y se cierran solos. Eso es normal.

### Paso 4 — Detener los servicios al terminar el día

```bash
docker-compose stop
```

Los datos se conservan. Para volver a levantar: `docker-compose up -d`

---

## Interfaces web disponibles

| URL | Servicio | Credenciales |
|---|---|---|
| http://localhost:8080 | Kafka UI — ver mensajes en tiempo real | Sin login |
| http://localhost:8082 | Flink UI — ver jobs de streaming | Sin login |
| http://localhost:8888 | Jupyter Lab + PySpark | Sin contraseña |
| http://localhost:4040 | Spark UI — aparece solo cuando hay un job activo | Sin login |
| http://localhost:9001 | MinIO consola — ver buckets y archivos | minioadmin / minioadmin123 |

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

El simulador genera eventos de usuarios interactuando con la plataforma y los envía al topic `platform-events` de Kafka.

```bash
# Instalar dependencia (solo la primera vez)
pip install kafka-python

# Correr el simulador
python event_simulator.py
```

Para verificar que los mensajes llegan: abrir http://localhost:8080 → Topics → platform-events → Messages.

Para detener: `Ctrl+C`

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

## Cómo descargar el dataset MovieLens 25M (Persona 3)

```bash
cd data

# Descargar (~250MB)
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip

# En Windows (PowerShell):
Invoke-WebRequest -Uri https://files.grouplens.org/datasets/movielens/ml-25m.zip -OutFile ml-25m.zip

# Descomprimir
# Linux/Mac:
unzip ml-25m.zip
# Windows: clic derecho → Extraer aquí

# Archivos resultantes:
# data/ml-25m/ratings.csv   → 25 millones de calificaciones
# data/ml-25m/movies.csv    → 62,000 películas con géneros
# data/ml-25m/tags.csv      → tags de usuarios
# data/ml-25m/links.csv     → links a IMDB y TMDB
```

Los CSV **no se suben a GitHub** (están en .gitignore). Cada persona los descarga localmente.

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

Abrir http://localhost:8888, crear un notebook y usar:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ST1630") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Rutas de las capas
bronze_path = "s3a://bronze/ratings"
silver_path = "s3a://silver/ratings_clean"
gold_path   = "s3a://gold/recommendations"
```

Los scripts van en `spark/jobs/` y aparecen en Jupyter bajo la carpeta `jobs/`.

### Persona 4 — MongoDB y PostgreSQL

```python
# MongoDB desde tu PC
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")
db = client["streaming_results"]

# Colecciones disponibles:
# db.trending_movies   — ranking cada 5 minutos
# db.genre_activity    — actividad por género (ventana 10min)
# db.anomaly_alerts    — alertas de bots

# PostgreSQL desde tu PC
import psycopg2
conn = psycopg2.connect(
    host="localhost", port=5432,
    database="movielens",
    user="postgres", password="postgres123"
)
```

### Persona 5 — Streamlit

```bash
pip install streamlit pymongo
streamlit run dashboard/streamlit_app.py
```

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
2. Abrir http://localhost:8080 — Kafka UI vacío
3. `python event_simulator.py` — mensajes llegando a Kafka en tiempo real
4. Abrir http://localhost:8080 → Topics → platform-events → Messages — ver eventos
5. Abrir http://localhost:8082 — Flink UI con job de streaming corriendo
6. Abrir http://localhost:8888 — correr pipeline batch de Spark (Bronze → Silver → Gold)
7. Abrir http://localhost:9001 — ver archivos en MinIO (capas bronze, silver, gold)
8. Mostrar dashboard Streamlit con recomendaciones + trending en tiempo real
9. Mostrar comparativa SQL vs NoSQL en Jupyter

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