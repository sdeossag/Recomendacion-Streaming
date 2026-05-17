"""
ST1630 - Sistemas Intensivos en Datos
Persona 1: Infraestructura y Kafka
event_simulator.py

Simula usuarios reales interactuando con una plataforma de streaming.
Produce eventos al topic 'platform-events' en Kafka.

Instalar dependencias:
    pip install kafka-python

Correr:
    python event_simulator.py
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# ============================================================
# CONFIGURACIÓN
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_NAME = "platform-events"

# Cuántos eventos por segundo genera el simulador
EVENTS_PER_SECOND = 5

# ============================================================
# DATOS DE DOMINIO — películas y géneros reales de MovieLens
# ============================================================

# Muestra representativa de películas populares de MovieLens
# Formato: (movie_id, titulo, lista_de_generos)
MOVIES = [
    (318,  "The Shawshank Redemption",  ["Drama"]),
    (296,  "Pulp Fiction",              ["Crime", "Drama"]),
    (593,  "The Silence of the Lambs",  ["Crime", "Horror", "Thriller"]),
    (2571, "The Matrix",                ["Action", "Sci-Fi", "Thriller"]),
    (356,  "Forrest Gump",              ["Comedy", "Drama", "Romance"]),
    (480,  "Jurassic Park",             ["Action", "Adventure", "Sci-Fi"]),
    (260,  "Star Wars: Episode IV",     ["Action", "Adventure", "Sci-Fi"]),
    (527,  "Schindler's List",          ["Drama", "War"]),
    (1,    "Toy Story",                 ["Adventure", "Animation", "Comedy"]),
    (1196, "Star Wars: Episode V",      ["Action", "Adventure", "Sci-Fi"]),
    (4993, "The Lord of the Rings",     ["Action", "Adventure", "Drama"]),
    (7153, "The Dark Knight",           ["Action", "Crime", "Drama"]),
    (858,  "Goodfellas",               ["Crime", "Drama"]),
    (1210, "Star Wars: Episode VI",    ["Action", "Adventure", "Sci-Fi"]),
    (2959, "Fight Club",               ["Drama", "Thriller"]),
    (2762, "The Sixth Sense",          ["Drama", "Horror", "Mystery"]),
    (1198, "Raiders of the Lost Ark",  ["Action", "Adventure"]),
    (745,  "Toy Story 2",              ["Adventure", "Animation", "Comedy"]),
    (364,  "The Lion King",            ["Adventure", "Animation", "Drama"]),
    (3578, "Gladiator",                ["Action", "Adventure", "Drama"]),
]

# Tipos de eventos que puede generar un usuario
EVENT_TYPES = [
    "rating",           # Calificó una película
    "play_start",       # Empezó a reproducir
    "play_pause",       # Pausó la reproducción
    "play_stop",        # Abandonó la sesión
    "add_to_favorites", # Agregó a favoritos
    "search",           # Buscó una película
]

# Pesos de probabilidad para cada tipo de evento
# rating y play_start son los más comunes
EVENT_WEIGHTS = [0.30, 0.35, 0.10, 0.10, 0.05, 0.10]

# ============================================================
# PERFILES DE USUARIO — simulan gustos distintos
# ============================================================

USER_PROFILES = {
    "action_fan": {
        "preferred_genres": ["Action", "Adventure", "Sci-Fi"],
        "rating_bias": 0.3,      # Califica más alto películas de acción
        "activity_level": "high", # Genera muchos eventos
    },
    "drama_lover": {
        "preferred_genres": ["Drama", "Crime", "War"],
        "rating_bias": 0.2,
        "activity_level": "medium",
    },
    "casual_watcher": {
        "preferred_genres": ["Comedy", "Animation", "Romance"],
        "rating_bias": 0.0,
        "activity_level": "low",
    },
    "horror_fan": {
        "preferred_genres": ["Horror", "Thriller", "Mystery"],
        "rating_bias": 0.1,
        "activity_level": "medium",
    },
    "cinephile": {
        "preferred_genres": ["Drama", "Crime", "Thriller", "War"],
        "rating_bias": -0.2,   # Más crítico, califica más bajo
        "activity_level": "high",
    },
}

# IDs de usuarios (tomados del rango real de MovieLens)
USER_IDS = list(range(1, 501))  # 500 usuarios simulados

# Asignar perfil a cada usuario
USER_PROFILE_MAP = {
    uid: random.choice(list(USER_PROFILES.keys()))
    for uid in USER_IDS
}

# ============================================================
# ESTADO DE SESIÓN — para detectar anomalías
# Rastrea cuántos eventos recientes hizo cada usuario
# ============================================================
user_event_count = {uid: 0 for uid in USER_IDS}
user_last_reset = {uid: time.time() for uid in USER_IDS}

# ============================================================
# FUNCIONES DE GENERACIÓN DE EVENTOS
# ============================================================

def get_rating_for_user(user_id: int, movie_genres: list) -> float:
    """
    Calcula una calificación realista según el perfil del usuario.
    Si los géneros coinciden con sus preferidos, califica más alto.
    """
    profile_name = USER_PROFILE_MAP.get(user_id, "casual_watcher")
    profile = USER_PROFILES[profile_name]

    base_rating = random.uniform(1.0, 5.0)
    bias = profile["rating_bias"]

    # Si la película es del género preferido, sube la calificación
    preferred = profile["preferred_genres"]
    genre_match = any(g in preferred for g in movie_genres)
    if genre_match:
        base_rating += bias + random.uniform(0.0, 0.5)

    # Redondear a 0.5 más cercano (como MovieLens real)
    rounded = round(base_rating * 2) / 2
    return max(0.5, min(5.0, rounded))


def generate_event(user_id: int) -> dict:
    """
    Genera un evento JSON para un usuario.
    Estructura estándar acordada con el equipo.
    """
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    movie = random.choice(MOVIES)
    movie_id, movie_title, movie_genres = movie

    # Timestamp en formato ISO 8601 con timezone UTC
    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    event = {
        "event_id":    str(uuid.uuid4()),        # ID único del evento
        "event_type":  event_type,               # Tipo de evento
        "user_id":     user_id,                  # ID del usuario
        "movie_id":    movie_id,                 # ID de la película
        "movie_title": movie_title,              # Nombre (para legibilidad)
        "genres":      movie_genres,             # Lista de géneros
        "timestamp":   timestamp_str,            # Cuándo ocurrió
        "session_id":  f"sess-{user_id}-{now.strftime('%Y%m%d%H')}",  # Sesión por hora
    }

    # Si el evento es una calificación, agregar el valor
    if event_type == "rating":
        event["rating"] = get_rating_for_user(user_id, movie_genres)

    # Si el evento es reproducción, agregar duración aleatoria
    if event_type in ("play_stop", "play_pause"):
        event["watch_duration_seconds"] = random.randint(30, 7200)

    return event


def is_anomalous_user(user_id: int) -> bool:
    """
    Detecta si un usuario está enviando eventos a velocidad anormal.
    Criterio: más de 20 eventos tipo 'rating' en 2 minutos.
    Retorna True si el usuario es sospechoso.
    """
    now = time.time()
    elapsed = now - user_last_reset[user_id]

    # Resetear contador cada 2 minutos
    if elapsed > 120:
        user_event_count[user_id] = 0
        user_last_reset[user_id] = now

    return user_event_count[user_id] > 20


def simulate_bot_burst(producer: KafkaProducer, bot_user_id: int):
    """
    Simula un usuario bot que envía muchas calificaciones rápido.
    Esto debe disparar la alerta de anomalía en Flink.
    Se activa aleatoriamente ~1% del tiempo.
    """
    print(f"\n⚠️  SIMULANDO BOT: usuario {bot_user_id} — ráfaga de eventos")
    for _ in range(25):  # 25 eventos rápidos → supera umbral de 20
        event = generate_event(bot_user_id)
        event["event_type"] = "rating"  # Solo ratings cuenta para anomalía
        send_event(producer, event)
        user_event_count[bot_user_id] += 1
        time.sleep(0.05)  # 50ms entre eventos = muy rápido


# ============================================================
# PRODUCCIÓN A KAFKA
# ============================================================

def send_event(producer: KafkaProducer, event: dict):
    """
    Envía un evento a Kafka.
    La clave de partición es el user_id: todos los eventos del mismo
    usuario van a la misma partición (garantiza orden por usuario).
    """
    key = str(event["user_id"]).encode("utf-8")
    value = json.dumps(event).encode("utf-8")

    producer.send(
        topic=TOPIC_NAME,
        key=key,
        value=value,
    )


def create_producer() -> KafkaProducer:
    """
    Crea el productor Kafka con configuración robusta.
    """
    print("Conectando a Kafka en localhost:29092...")

    # Reintentar conexión si Kafka no está listo aún
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                # Garantiza que el mensaje llegó al broker
                acks="all",
                # Reintentos si hay error de red
                retries=3,
                # Envía en lotes de 16KB para eficiencia
                batch_size=16384,
                # Espera hasta 10ms para llenar el lote
                linger_ms=10,
                # Compresión para reducir tamaño de mensajes
                compression_type="gzip",
            )
            print("✅ Conectado a Kafka exitosamente")
            return producer
        except Exception as e:
            print(f"Intento {attempt+1}/10 fallido: {e}")
            time.sleep(3)

    raise RuntimeError("No se pudo conectar a Kafka después de 10 intentos")


# ============================================================
# BUCLE PRINCIPAL
# ============================================================

def main():
    print("=" * 60)
    print("ST1630 - Event Simulator")
    print("Topic: platform-events | Particiones: 3")
    print(f"Velocidad: {EVENTS_PER_SECOND} eventos/segundo")
    print("Presiona Ctrl+C para detener")
    print("=" * 60)

    producer = create_producer()

    total_events = 0
    start_time = time.time()

    try:
        while True:
            # Elegir usuario aleatorio
            user_id = random.choice(USER_IDS)

            # 1% de probabilidad de simular un bot
            if random.random() < 0.01:
                simulate_bot_burst(producer, user_id)
                total_events += 25
                continue

            # Generar y enviar evento normal
            event = generate_event(user_id)

            # Actualizar contador de eventos para detección de anomalías
            if event["event_type"] == "rating":
                user_event_count[user_id] += 1

            send_event(producer, event)
            total_events += 1

            # Log cada 50 eventos
            if total_events % 50 == 0:
                elapsed = time.time() - start_time
                rate = total_events / elapsed
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"Total eventos: {total_events:,} | "
                    f"Velocidad real: {rate:.1f} ev/s | "
                    f"Último: user={event['user_id']} "
                    f"type={event['event_type']} "
                    f"movie='{event['movie_title']}'"
                )

            # Asegurar que los mensajes se envíen al broker
            producer.flush()

            # Controlar la velocidad de producción
            time.sleep(1.0 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        print(f"\n\nSimulación detenida.")
        print(f"Total eventos enviados: {total_events:,}")
        elapsed = time.time() - start_time
        print(f"Duración: {elapsed:.1f}s | Promedio: {total_events/elapsed:.1f} ev/s")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
