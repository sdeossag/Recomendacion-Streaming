# ST1630 — Guía de Infraestructura y Colaboración en Equipo

## Estructura del repositorio

```
st1630-proyecto/
├── docker-compose.yml          ← Persona 1 (tú)
├── event_simulator.py          ← Persona 1 (tú)
│
├── spark/
│   ├── jobs/
│   │   ├── ingestion_bronze.py      ← Persona 3
│   │   ├── transformation_silver.py ← Persona 3
│   │   └── training_gold.py         ← Persona 3
│   └── jars/                        ← JARs de Iceberg (ver abajo)
│
├── flink/
│   └── jobs/
│       └── flink_streaming_job.py   ← Persona 2
│
├── notebooks/
│   └── comparativa_sql_nosql.ipynb  ← Persona 4
│
├── queries/
│   ├── mongo_queries.py             ← Persona 4
│   └── gold_queries.py              ← Persona 5
│
├── dashboard/
│   └── streamlit_app.py             ← Persona 5
│
├── data/
│   └── README_data.md               ← instrucciones para descargar MovieLens
│
└── README.md
```

---

## Puertos de cada servicio (para todos los compañeros)

| Servicio       | URL                       | Notas                           |
|----------------|---------------------------|---------------------------------|
| Kafka UI       | http://localhost:8080     | Ver mensajes en tiempo real     |
| Spark UI       | http://localhost:8081     | Ver jobs de Spark               |
| Flink UI       | http://localhost:8082     | Ver jobs de Flink               |
| MinIO consola  | http://localhost:9001     | user: minioadmin / minioadmin123|
| MinIO API      | http://localhost:9000     | Para Spark (S3 compatible)      |
| MongoDB        | localhost:27017           | Sin autenticación               |
| PostgreSQL     | localhost:5432            | user: postgres / postgres123    |
| Kafka externo  | localhost:29092           | Para event_simulator.py         |

---

## Credenciales de servicios

| Servicio   | Usuario       | Contraseña    | Base de datos |
|------------|---------------|---------------|---------------|
| MinIO      | minioadmin    | minioadmin123 | —             |
| MongoDB    | (sin auth)    | (sin auth)    | streaming_results |
| PostgreSQL | postgres      | postgres123   | movielens     |

---

## CÓMO COMPARTIR EL TRABAJO CON EL EQUIPO (Git paso a paso)

### Paso 1 — Crear el repositorio en GitHub (lo hace UNO solo, el líder)

1. Ir a https://github.com → botón verde **New**
2. Nombre: `st1630-proyecto-final`
3. Privado (Private) — solo el equipo lo ve
4. NO marcar "Add a README" (lo creamos nosotros)
5. Clic en **Create repository**

GitHub te muestra comandos. Copia la URL que termina en `.git`

---

### Paso 2 — Configurar Git en tu computador (cada persona, una sola vez)

Abrir terminal y ejecutar:

```bash
# Configurar tu nombre y correo (aparece en los commits)
git config --global user.name "Tu Nombre Aquí"
git config --global user.email "tu@email.com"
```

---

### Paso 3 — El líder sube la estructura inicial

```bash
# Crear carpeta del proyecto
mkdir st1630-proyecto-final
cd st1630-proyecto-final

# Crear la estructura de carpetas vacías
mkdir -p spark/jobs spark/jars flink/jobs notebooks queries dashboard data

# Copiar docker-compose.yml y event_simulator.py aquí

# Crear archivo .gitignore para no subir archivos grandes
cat > .gitignore << 'EOF'
# Dataset MovieLens (son archivos enormes, NO van en Git)
data/*.csv
data/*.zip

# Archivos de Python compilado
__pycache__/
*.pyc
*.pyo
*.pyd

# Entornos virtuales
venv/
env/
.venv/

# Archivos de sistema
.DS_Store
Thumbs.db

# Variables de entorno locales
.env
*.env.local

# Checkpoints de Flink (generados en ejecución)
/tmp/flink-checkpoints/

# Jupyter notebooks con outputs ejecutados (opcional)
# .ipynb_checkpoints/
EOF

# Inicializar Git
git init

# Agregar todos los archivos
git add .

# Primer commit
git commit -m "feat: estructura inicial del proyecto y docker-compose"

# Conectar con GitHub (pegar la URL de tu repositorio)
git remote add origin https://github.com/TU_USUARIO/st1630-proyecto-final.git

# Subir
git branch -M main
git push -u origin main
```

---

### Paso 4 — Cada compañero clona el repositorio

Cada persona (una sola vez):

```bash
# Clonar el repo (pegar la URL del repositorio)
git clone https://github.com/TU_USUARIO/st1630-proyecto-final.git

# Entrar a la carpeta
cd st1630-proyecto-final
```

---

### Paso 5 — Flujo diario de trabajo (CADA VEZ que trabajes)

#### Al EMPEZAR el día — traer los cambios de los compañeros:
```bash
git pull origin main
```

#### Durante el trabajo — guardar tus avances:
```bash
# Ver qué archivos cambiaste
git status

# Agregar tus archivos modificados
git add spark/jobs/ingestion_bronze.py   # ejemplo Persona 3
# O agregar todos tus cambios:
git add .

# Crear un commit con mensaje descriptivo
git commit -m "feat: ingestion_bronze con dos lotes y metadata de auditoria"

# Subir al repositorio compartido
git push origin main
```

---

### Convención de mensajes de commit (para mantener orden)

```
feat: nueva funcionalidad    → git commit -m "feat: job de Flink con ventana tumbling"
fix: corrección de bug       → git commit -m "fix: error en schema JSON del evento"
docs: documentación          → git commit -m "docs: README con instrucciones de MongoDB"
test: pruebas                → git commit -m "test: queries de comparativa SQL vs NoSQL"
```

---

### ¿Qué pasa si dos personas editan el mismo archivo? (conflicto)

Git te avisa con un mensaje como `CONFLICT`. Para resolverlo:

```bash
# 1. Abrir el archivo conflictivo en tu editor
# Verás algo así:
# <<<<<<< HEAD
# tu versión del código
# =======
# la versión de tu compañero
# >>>>>>> origin/main

# 2. Editar el archivo dejando la versión correcta (puede ser una mezcla de ambas)
# 3. Guardar el archivo
git add nombre_del_archivo.py
git commit -m "fix: resolver conflicto en ..."
git push origin main
```

**Para evitar conflictos:** cada persona trabaja en sus propios archivos (ver estructura arriba).

---

## Cómo descargar el dataset MovieLens 25M

```bash
# En la carpeta data/ del proyecto:
cd data

# Descargar (archivo de ~250MB)
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip

# Descomprimir
unzip ml-25m.zip

# Archivos que vas a usar:
# ml-25m/ratings.csv   → 25 millones de calificaciones
# ml-25m/movies.csv    → 62,000 películas con géneros
# ml-25m/tags.csv      → tags de usuarios
# ml-25m/links.csv     → links a IMDB y TMDB
```

⚠️ **IMPORTANTE:** los archivos CSV NO se suben a Git (están en .gitignore). Cada persona los descarga localmente.

---

## Cómo levantar toda la infraestructura

### Requisitos previos
- Docker Desktop instalado y corriendo
- 8GB RAM mínimo disponibles para Docker
- 20GB espacio libre en disco

### Comandos

```bash
# Levantar todo (primera vez tarda ~5 minutos descargando imágenes)
docker-compose up -d

# Ver si todos los servicios están healthy
docker-compose ps

# Ver logs de un servicio específico
docker-compose logs -f kafka
docker-compose logs -f flink-jobmanager

# Detener todo (los datos se conservan en los volúmenes)
docker-compose stop

# Detener y borrar TODO incluyendo datos
docker-compose down -v
```

### ¿Cómo sé que todo está bien?

Todos los servicios deben mostrar `running` o `healthy` en `docker-compose ps`. Además:
- Kafka UI en http://localhost:8080 debe mostrar el cluster `local-cluster`
- MinIO en http://localhost:9001 debe mostrar los buckets `bronze`, `silver`, `gold`, `warehouse`
- Flink UI en http://localhost:8082 debe mostrar 1 Task Manager con 4 slots
- Spark UI en http://localhost:8081 debe mostrar 1 worker activo

---

## Correr el simulador de eventos

```bash
# Instalar la librería de Kafka para Python
pip install kafka-python

# Correr el simulador (kafka debe estar corriendo)
python event_simulator.py

# En otra terminal, verificar en Kafka UI:
# http://localhost:8080 → Topics → platform-events → Messages
```

---

## Schema del evento JSON (ACORDADO CON TODO EL EQUIPO — no cambiar)

```json
{
  "event_id":    "uuid-string",
  "event_type":  "rating | play_start | play_pause | play_stop | add_to_favorites | search",
  "user_id":     1042,
  "movie_id":    318,
  "movie_title": "The Shawshank Redemption",
  "genres":      ["Drama"],
  "timestamp":   "2026-05-12T14:35:22.123Z",
  "session_id":  "sess-1042-2026051214",

  // Solo si event_type == "rating":
  "rating":      4.5,

  // Solo si event_type == "play_stop" o "play_pause":
  "watch_duration_seconds": 3600
}
```

---

## Conexiones para cada persona

### Persona 2 (Flink)
```python
# Kafka → Flink (desde DENTRO del contenedor)
bootstrap_servers = "kafka:9092"   # Interno
topic = "platform-events"

# MongoDB desde Flink (dentro del contenedor)
mongo_uri = "mongodb://mongodb:27017"
database = "streaming_results"
```

### Persona 3 (Spark)
```python
# MinIO desde Spark
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin123")

# Rutas de las capas
bronze_path = "s3a://bronze/ratings"
silver_path = "s3a://silver/ratings_clean"
gold_path   = "s3a://gold/recommendations"
```

### Persona 4 (MongoDB y PostgreSQL)
```python
# MongoDB desde tu PC (fuera del contenedor)
mongo_uri = "mongodb://localhost:27017"
database  = "streaming_results"

# PostgreSQL desde tu PC
conn_string = "postgresql://postgres:postgres123@localhost:5432/movielens"
```

### Persona 5 (Streamlit)
```python
# Streamlit lee Gold (Iceberg/MinIO) y MongoDB
# Correr streamlit desde tu PC:
streamlit run dashboard/streamlit_app.py
```
