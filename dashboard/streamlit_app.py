import streamlit as st
import pandas as pd
import time
from datetime import datetime, timezone
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient

# Importar las consultas predefinidas
from queries.gold_queries import (
    GOLD_PIPELINE_HINT,
    SPARK_LOCAL_HINT,
    get_spark_session,
    gold_tables_ready,
    q_user_recommendations,
    q_most_recommended_movies,
    q_top_genre_by_decade,
    q_prediction_diversity_by_user,
    q_rating_distribution_percentage,
    q_novedad_vs_clasico_recommendations,
)
from queries.mongo_queries import (
    get_db,
    q_trending_top_k_latest_window,
    q_genre_most_active_in_recent_windows,
    q_anomaly_alerts_for_user
)

# --- Configuración de la Página ---
st.set_page_config(
    page_title="CineMetrics - Sistema de Recomendación Híbrido",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Estilos de Diseño Premium (Vanilla CSS) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap');
    
    /* Fuente general y fondo */
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
    }
    
    /* Header estilizado */
    .main-header {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
        padding: 2.5rem;
        border-radius: 16px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 10px 25px rgba(42, 82, 152, 0.15);
    }
    .main-header h1 {
        font-weight: 700;
        font-size: 2.8rem;
        margin-bottom: 0.5rem;
        letter-spacing: -0.5px;
    }
    .main-header p {
        font-weight: 300;
        font-size: 1.1rem;
        opacity: 0.9;
    }
    
    /* Tarjetas de Métricas */
    .metric-card {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 1.5rem;
        border-left: 5px solid #2a5298;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.05);
        margin-bottom: 1rem;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.08);
    }
    .metric-card h4 {
        margin: 0;
        font-size: 0.9rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .metric-card h2 {
        margin: 0.5rem 0 0 0;
        font-size: 2rem;
        font-weight: 700;
        color: #0f172a;
    }
    
    /* Alerta Anomalias */
    .alert-card {
        background-color: #fff1f2;
        border-radius: 10px;
        padding: 1rem 1.5rem;
        border-left: 5px solid #f43f5e;
        box-shadow: 0 4px 10px rgba(244, 63, 94, 0.05);
        margin-bottom: 0.8rem;
    }
    .alert-card-title {
        font-weight: 700;
        color: #9f1239;
        font-size: 1rem;
        margin-bottom: 0.2rem;
    }
    .alert-card-desc {
        color: #e11d48;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)


# --- Caché de Recursos Pesados ---
@st.cache_resource
def get_cached_spark():
    """Mantiene la SparkSession viva para evitar latencias de inicialización de 10s en cada click"""
    try:
        return get_spark_session("CineMetrics-Streamlit")
    except Exception as e:
        st.sidebar.error(f"Error conectando a Spark: {e}")
        if "JAVA_GATEWAY" in str(e) or "getSubject" in str(e) or "Java" in str(e):
            st.sidebar.warning(SPARK_LOCAL_HINT)
        return None

@st.cache_resource
def get_cached_mongo():
    """Mantiene la conexión a MongoDB activa"""
    try:
        # Host interno 'mongodb' para cuando corre en Docker, 'localhost' para fallback
        try:
            db = get_db(uri="mongodb://mongodb:27017")
            # Forzar ping rápido
            db.client.admin.command('ping')
            return db
        except Exception:
            db = get_db(uri="mongodb://localhost:27017")
            db.client.admin.command('ping')
            return db
    except Exception as e:
        st.sidebar.error(f"Error conectando a MongoDB: {e}")
        return None


def resolve_live_mongo_db(primary_db):
    """
    Usa streaming_results; si está vacío pero movie_platform tiene datos (Flink legacy),
    lee desde ahí para no dejar el dashboard en blanco en demos.
    """
    if primary_db is None:
        return None, None
    if primary_db["trending_movies"].estimated_document_count() > 0:
        return primary_db, "streaming_results"
    legacy = primary_db.client["movie_platform"]
    if legacy["trending_movies"].estimated_document_count() > 0:
        return legacy, "movie_platform"
    return primary_db, "streaming_results"


def trending_to_dataframe(trending_docs):
    """Convierte documentos de Flink (uno por película) a DataFrame para la UI."""
    if not trending_docs:
        return pd.DataFrame()
    rows = [
        {
            "ID Película": doc.get("movie_id"),
            "Título": doc.get("movie_title") or f"Movie-{doc.get('movie_id')}",
            "Cantidad de Ratings": doc.get("rating_count", 0),
        }
        for doc in trending_docs
    ]
    return pd.DataFrame(rows)


def genre_activity_to_dataframe(genre_docs):
    if not genre_docs:
        return pd.DataFrame()
    df = pd.DataFrame(genre_docs)
    if "_id" in df.columns:
        df = df.rename(columns={"_id": "Género", "total_events": "Total Eventos"})
    return df


# --- Inicializar conexiones ---
db = get_cached_mongo()

# --- Sidebar Estilizada ---
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/movie-projector.png", width=70)
    st.markdown("## **CineMetrics**\n*Sistemas Intensivos en Datos*")
    st.markdown("---")
    
    # Navegación
    vista = st.radio(
        "Navegación",
        ["🏠 Inicio & Arquitectura", "🎬 Recomendaciones (Batch)", "🔥 Streaming en Vivo", "📊 Análisis del Catálogo"]
    )
    
    st.markdown("---")
    st.markdown("### **Estado de Servicios**")
    
    # Check MongoDB
    if db is not None:
        st.markdown("🟢 **MongoDB:** Conectado")
    else:
        st.markdown("🔴 **MongoDB:** Desconectado")
        
    # Check Spark (solo se activa cuando se requiere)
    spark_status = st.empty()
    spark_status.markdown("⚪ **Spark:** Inactivo")

# --- VISTA 1: INICIO & ARQUITECTURA ---
if vista == "🏠 Inicio & Arquitectura":
    st.markdown("""
    <div class="main-header">
        <h1>CineMetrics Dashboard</h1>
        <p>Plataforma híbrida de recomendación batch (Spark SQL + Iceberg) y analítica de streaming en tiempo real (Flink + Kafka + MongoDB).</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### **El Enfoque Híbrido Lambda/Kappa**")
        st.write("""
        Esta plataforma demuestra cómo diseñar e implementar una arquitectura de datos moderna y robusta capaz de resolver dos desafíos fundamentales de manera simultánea:
        """)
        
        # Expandible para la Arquitectura Lakehouse (Batch)
        with st.expander("📚 Capa Batch (El Pasado) — Arquitectura Medallón en Lakehouse", expanded=True):
            st.markdown("""
            El pipeline batch procesa grandes volúmenes de datos históricos (el dataset de **MovieLens con 32 millones de calificaciones**) utilizando **Apache Spark** sobre un **Lakehouse con formato Apache Iceberg** y almacenamiento compatible con S3 (**MinIO**):
            
            - **Capa Bronze (Datos Crudos):** Ingesta limpia de los archivos CSV históricos y bases relacionales relativas a usuarios y películas a un bucket de MinIO en formato Iceberg.
            - **Capa Silver (Limpieza y Enriquecimiento):** Filtrado de duplicados, normalización de ratings (escala 0-1) y parseo/limpieza de géneros y títulos de películas en una tabla unificada y particionada por año.
            - **Capa Gold (Analítica & ML):**
              1. **Machine Learning:** Entrenamiento de un modelo de filtrado colaborativo **ALS (Alternating Least Squares)** con Spark MLlib para calcular afinidades de usuarios y predecir su Top 10 de recomendaciones.
              2. **Consultas de Negocio:** Agregación e índices analíticos de tendencias de géneros por décadas y distribución histórica de calificaciones.
            """)
            
        # Expandible para la Capa de Streaming (El Presente)
        with st.expander("⚡ Capa Streaming (El Presente) — Procesamiento en Tiempo Real", expanded=True):
            st.markdown("""
            Captura y analiza el comportamiento interactivo de los usuarios en tiempo real a medida que hacen clics, reproducen o buscan contenido. El flujo opera continuo y con baja latencia:
            
            - **Simulación y Mensajería:** Un simulador genera eventos JSON realistas y los publica en el bus de mensajería **Apache Kafka** en el topic `platform-events`.
            - **Procesamiento de Flujos con Apache Flink (PyFlink):** Flink consume directamente de Kafka y realiza analíticas complejas mediante ventanas temporales avanzadas:
              - **Tendencias de Películas:** Ventanas deslizantes (*Sliding Windows*) de **5 minutos** con deslizamiento de 10 segundos para identificar en tiempo real las películas más vistas.
              - **Actividad por Género:** Ventanas de rotación (*Tumbling Windows*) de **10 minutos** para calcular la distribución de géneros más consumidos en la plataforma.
              - **Detección de Anomalías (Seguridad):** Algoritmo de filtrado en tiempo real que alerta si un usuario realiza más de 15 interacciones en 10 segundos, detectando bots de spam.
            - **Persistencia en NoSQL:** Los resultados agregados y alertas de anomalías se insertan en **MongoDB** para que el dashboard los renderice de forma instantánea.
            """)
            
        # Tabla de Stack Tecnológico
        st.markdown("### **Stack Tecnológico Implementado**")
        st.markdown("""
        | Capa del Pipeline | Tecnología Principal | Función y Propósito |
        | :--- | :--- | :--- |
        | **Ingesta Histórica** | PostgreSQL | Base de datos de origen para metadatos |
        | **Capa de Mensajería** | Apache Kafka | Bus de eventos y cola de mensajería distribuida |
        | **Procesamiento Batch** | Apache Spark | Ingesta, curación de datos y entrenamiento ALS |
        | **Procesamiento Stream** | Apache Flink | Análisis en tiempo real sobre ventanas y alertas |
        | **Almacenamiento Batch** | Apache Iceberg + MinIO | Lakehouse transaccional y almacenamiento compatible con S3 |
        | **Almacenamiento Stream** | MongoDB | Base de datos NoSQL de lectura rápida para analíticas vivas |
        | **Visualización** | Streamlit | Dashboard unificado para analítica batch e interactiva |
        """)
        
    with col2:
        st.markdown("### **Integrantes del Equipo**")
        st.info("""
        - **Samuel Henao Castrillón**
        - **Samuel Deossa Gómez**
        - **Juan José Gómez Ramírez**
        - **Samuel Herrera Hoyos**
        - **Abraham Elías Navarro**
        """)
        
        st.markdown("### **Métricas del Dashboard**")
        st.markdown("""
        <div class="metric-card">
            <h4>Volumen Histórico</h4>
            <h2>32M Ratings</h2>
        </div>
        <div class="metric-card">
            <h4>Frecuencia Streaming</h4>
            <h2>~50 msg/seg</h2>
        </div>
        """, unsafe_allow_html=True)

# --- VISTA 2: RECOMENDACIONES BATCH (GOLD - SPARK SQL) ---
elif vista == "🎬 Recomendaciones (Batch)":
    st.markdown("""
    <div class="main-header">
        <h1>Recomendaciones Personalizadas</h1>
        <p>Visualización de las predicciones del modelo de Machine Learning ALS almacenadas en la capa Gold de Apache Iceberg.</p>
    </div>
    """, unsafe_allow_html=True)
    
    spark_status.markdown("🟡 **Spark:** Conectando...")
    spark = get_cached_spark()
    
    if spark is None:
        st.error(
            "No se pudo iniciar la sesión de Spark. Verifica que `spark-master` y MinIO "
            "estén activos (`docker compose ps`)."
        )
        with st.expander("Solución habitual en Windows (JAVA_GATEWAY_EXITED)"):
            st.markdown(SPARK_LOCAL_HINT)
        spark_status.markdown("🔴 **Spark:** Error de Conexión")
    else:
        spark_status.markdown("🟢 **Spark:** Conectado")
        
        st.markdown("### **Consulta de Recomendaciones por Usuario**")
        user_id = st.number_input("Ingresa el ID del Usuario a consultar:", min_value=1, value=1, step=1)
        
        if st.button("Consultar Recomendaciones en Gold"):
            with st.spinner("Ejecutando consulta Spark SQL sobre Iceberg..."):
                try:
                    recos_df = q_user_recommendations(spark, user_id, limit=10)
                    
                    if recos_df.empty:
                        st.warning(f"No se encontraron recomendaciones para el usuario {user_id}. Es posible que no esté en la base de datos o que el modelo ALS no lo haya procesado.")
                    else:
                        st.success(f"¡Recomendaciones cargadas exitosamente para el Usuario {user_id}!")
                        
                        # Mostrar métricas del usuario
                        avg_pred = recos_df['predicted_score'].mean()
                        max_pred = recos_df['predicted_score'].max()
                        
                        col_m1, col_m2 = st.columns(2)
                        with col_m1:
                            st.markdown(f"""
                            <div class="metric-card" style="border-left-color: #10b981;">
                                <h4>Score Promedio Predicho</h4>
                                <h2>{avg_pred:.2f} ★</h2>
                            </div>
                            """, unsafe_allow_html=True)
                        with col_m2:
                            st.markdown(f"""
                            <div class="metric-card" style="border-left-color: #f59e0b;">
                                <h4>Mejor Recomendación</h4>
                                <h2>{max_pred:.2f} ★</h2>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        st.markdown("#### **Top 10 Películas Sugeridas**")
                        
                        # Formatear el dataframe para mostrarlo con estilo
                        recos_df_styled = recos_df.copy()
                        recos_df_styled['predicted_score'] = recos_df_styled['predicted_score'].apply(lambda x: f"{x:.2f} ★")
                        recos_df_styled.columns = ["ID Usuario", "ID Película", "Título de Película", "Géneros", "Afinidad Predicha"]
                        
                        st.dataframe(recos_df_styled, use_container_width=True)
                        
                        # Gráfico de barras de afinidad
                        fig = px.bar(
                            recos_df, 
                            x="predicted_score", 
                            y="title", 
                            orientation='h',
                            title=f"Afinidad Predicha por Película para el Usuario {user_id}",
                            labels={"predicted_score": "Afinidad (Predicted Score)", "title": "Película"},
                            color="predicted_score",
                            color_continuous_scale=px.colors.sequential.Viridis
                        )
                        fig.update_layout(yaxis={'categoryorder':'total ascending'})
                        st.plotly_chart(fig, use_container_width=True)
                        
                except Exception as e:
                    st.error(f"Error al ejecutar la consulta: {e}")
                    st.info("Asegúrate de haber ejecutado primero el pipeline batch de Spark (`training_gold.py`) para popular las tablas de la capa Gold.")

# --- VISTA 3: STREAMING EN VIVO (MONGODB) ---
elif vista == "🔥 Streaming en Vivo":
    st.markdown("""
    <div class="main-header" style="background: linear-gradient(135deg, #f43f5e 0%, #f43f5e 100%);">
        <h1>Monitoreo de Streaming en Vivo</h1>
        <p>Datos analíticos en tiempo real consumidos directamente de MongoDB. Actualizado dinámicamente cada pocos segundos.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col_ctrl1, col_ctrl2 = st.columns([3, 1])
    with col_ctrl1:
        refresh_interval = st.slider("Intervalo de auto-refresco (segundos)", 2, 15, 3)
    with col_ctrl2:
        auto_refresh = st.checkbox("🔄 Auto-refresco", value=True)

    if db is None:
        st.error("No se puede mostrar el tiempo real porque MongoDB está desconectado.")
    else:
        live_db, live_db_name = resolve_live_mongo_db(db)
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        st.caption(f"Última actualización: **{now_utc}** · Base Mongo: `{live_db_name}`")

        n_trend = live_db["trending_movies"].estimated_document_count()
        n_genre = live_db["genre_activity"].estimated_document_count()
        n_alerts = live_db["anomaly_alerts"].estimated_document_count()

        m1, m2, m3 = st.columns(3)
        m1.metric("Docs trending_movies", n_trend)
        m2.metric("Docs genre_activity", n_genre)
        m3.metric("Docs anomaly_alerts", n_alerts)

        if n_trend == 0 and n_genre == 0:
            st.warning(
                "MongoDB conectado pero sin datos de streaming. Para la demo: "
                "1) docker compose up -d  2) python event_simulator.py  "
                "3) flink run del job en flink/jobs  4) esperar ~5 min y auto-refresco."
            )
            if live_db_name == "movie_platform":
                st.info("Leyendo base legacy movie_platform. Reiniciá Flink con MONGO_DB=streaming_results.")

        trending = q_trending_top_k_latest_window(live_db, k=10)
        genre_act = q_genre_most_active_in_recent_windows(live_db, last_n_windows=10)
        alerts = list(live_db["anomaly_alerts"].find().sort([("detected_at", -1)]).limit(5))

        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            st.markdown("### 📈 **Trending Movies (Última ventana de 5 min)**")
            if not trending:
                st.info(
                    "Sin ventana cerrada aún. Flink escribe un documento por película cada 5 min "
                    "tras recibir eventos del simulador."
                )
            else:
                window_start = trending[0].get("window_start")
                window_end = trending[0].get("window_end")
                st.caption(f"Ventana activa: **{window_start}** → **{window_end}**")

                df_trend = trending_to_dataframe(trending)
                st.dataframe(df_trend, use_container_width=True)

                fig_trend = px.bar(
                    df_trend,
                    x="Cantidad de Ratings",
                    y="Título",
                    orientation="h",
                    title="Películas más activas en la última ventana",
                    color="Cantidad de Ratings",
                    color_continuous_scale=px.colors.sequential.Reds,
                )
                fig_trend.update_layout(yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig_trend, use_container_width=True)
            
            st.markdown("### 📊 **Actividad por Géneros en Tiempo Real**")
            df_genres = genre_activity_to_dataframe(genre_act)
            if df_genres.empty:
                st.info("Sin agregación por género aún (ventana deslizante 10 min en Flink).")
            else:
                st.dataframe(df_genres, use_container_width=True)
                fig_gen = px.pie(
                    df_genres,
                    values="Total Eventos",
                    names="Género",
                    title="Eventos por género (últimas ventanas)",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                st.plotly_chart(fig_gen, use_container_width=True)
                
        with col_right:
            st.markdown("### 🚨 **Detección de Anomalías (Flink)**")
            st.write("Flink analiza si un usuario genera más de 20 ratings en menos de 2 minutos, lo cual indica comportamiento automatizado (bots).")
            
            if not alerts:
                st.success("✅ Todo en orden. No hay alertas de bots activas en este momento.")
            else:
                for alert in alerts:
                    st.markdown(f"""
                    <div class="alert-card">
                        <div class="alert-card-title">⚠️ BOT ALERT - ID USUARIO: {alert.get('user_id')}</div>
                        <div class="alert-card-desc">
                            <strong>Eventos detectados:</strong> {alert.get('event_count')}<br>
                            <strong>Hora de detección:</strong> {alert.get('detected_at')}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
        if auto_refresh:
            time.sleep(refresh_interval)
            st.rerun()

# --- VISTA 4: ANALISIS DEL CATALOGO (SPARK SQL GOLD) ---
elif vista == "📊 Análisis del Catálogo":
    st.markdown("""
    <div class="main-header">
        <h1>Análisis Analítico del Catálogo</h1>
        <p>Reportes de alto nivel ejecutados con Spark SQL combinando las dimensiones de la capa Gold y Silver en Apache Iceberg.</p>
    </div>
    """, unsafe_allow_html=True)
    
    spark_status.markdown("🟡 **Spark:** Conectando...")
    spark = get_cached_spark()
    
    if spark is None:
        st.error(
            "No se pudo iniciar la sesión de Spark. Verifica que `spark-master` y MinIO "
            "estén activos (`docker compose ps`)."
        )
        with st.expander("Solución habitual en Windows (JAVA_GATEWAY_EXITED)"):
            st.markdown(SPARK_LOCAL_HINT)
        spark_status.markdown("🔴 **Spark:** Error de Conexión")
    else:
        spark_status.markdown("🟢 **Spark:** Conectado")

        if not gold_tables_ready(spark):
            st.error(
                "No existe la tabla `local.gold.recommendations` en Iceberg/MinIO. "
                "Hay que correr el pipeline Bronze → Silver → Gold antes de usar esta sección."
            )
            st.code(GOLD_PIPELINE_HINT, language="text")
        
        st.markdown("### **Consultas de Negocio de Alto Nivel**")
        
        tab1, tab2, tab3, tab4 = st.tabs([
            "🏆 Películas Más Recomendadas",
            "📅 Evolución Histórica de Géneros",
            "⭐ Distribución de Estrellas",
            "🆕 Clásicos vs Novedades"
        ])
        
        with tab1:
            st.markdown("#### **Sesgo de Popularidad en el Modelo ALS**")
            st.write("Esta consulta muestra las películas que más frecuentemente sugiere el recomendador automático en las 10 sugerencias de todos los usuarios.")
            
            try:
                pop_df = q_most_recommended_movies(spark, limit=10)
                if pop_df.empty:
                    st.info("Asegúrate de haber corrido el pipeline batch para popular las tablas.")
                else:
                    st.dataframe(pop_df, use_container_width=True)
                    
                    fig = px.bar(
                        pop_df,
                        x="times_recommended",
                        y="title",
                        orientation='h',
                        title="Top Películas recomendadas por el recomendador",
                        labels={"times_recommended": "Cantidad de usuarios a quienes se les recomendó", "title": "Película"},
                        color="times_recommended",
                        color_continuous_scale=px.colors.sequential.Plasma
                    )
                    fig.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
                
        with tab2:
            st.markdown("#### **El Mejor Género por Década**")
            st.write("A partir del rating histórico, calcula el género que ha recibido el mayor promedio de calificaciones por cada década de lanzamiento.")
            
            try:
                decade_df = q_top_genre_by_decade(spark)
                if decade_df.empty:
                    st.info("Asegúrate de haber corrido el pipeline batch para popular las tablas.")
                else:
                    st.dataframe(decade_df, use_container_width=True)
                    
                    fig = px.scatter(
                        decade_df,
                        x="decade",
                        y="average_rating",
                        size="total_ratings",
                        color="top_genre",
                        hover_name="top_genre",
                        title="Rating promedio y popularidad del mejor género por década",
                        labels={"decade": "Década de Lanzamiento", "average_rating": "Rating Promedio Historico", "total_ratings": "Total Calificaciones"},
                        size_max=60
                    )
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
                
        with tab3:
            st.markdown("#### **Curva Psicológica de Calificación**")
            st.write("Permite entender cómo califican los usuarios en general. Se observa el típico sesgo de positividad.")
            
            try:
                dist_df = q_rating_distribution_percentage(spark)
                if dist_df.empty:
                    st.info("Asegúrate de haber corrido el pipeline batch para popular las tablas.")
                else:
                    col_d1, col_d2 = st.columns([1, 2])
                    
                    with col_d1:
                        st.dataframe(dist_df, use_container_width=True)
                        
                    with col_d2:
                        fig = px.bar(
                            dist_df,
                            x="rating",
                            y="percentage",
                            title="Porcentaje de Calificaciones Otorgadas (Estrellas)",
                            labels={"rating": "Calificación (Estrellas)", "percentage": "Porcentaje (%)"},
                            color="percentage",
                            color_continuous_scale=px.colors.sequential.Mint
                        )
                        st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
                
        with tab4:
            st.markdown("#### **Balance de Catálogo: Clásicos vs Estrenos Recientes**")
            st.write("Mide el grado de 'frescura' de las recomendaciones. Compara la cantidad y afinidad de películas recientes (post-2018) frente a clásicos (pre-2019).")
            
            try:
                nov_df = q_novedad_vs_clasico_recommendations(spark)
                if nov_df.empty:
                    st.info("Asegúrate de haber corrido el pipeline batch para popular las tablas.")
                else:
                    col_n1, col_n2 = st.columns([1, 2])
                    with col_n1:
                        st.dataframe(nov_df, use_container_width=True)
                    with col_n2:
                        fig = px.pie(
                            nov_df,
                            values="total_recommendations",
                            names="category",
                            title="Proporción de Recomendaciones: Novedades vs Clásicos",
                            hole=0.4,
                            color_discrete_sequence=["#3b82f6", "#10b981"]
                        )
                        st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")
