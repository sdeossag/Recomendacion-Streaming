"""
ST1630 — Consultas operacionales sobre MongoDB y utilidades de benchmark.

Requisitos:
    pip install pymongo

Uso típico:
    from pymongo import MongoClient
    from mongo_queries import ensure_operational_indexes, benchmark_latencies

    client = MongoClient("mongodb://localhost:27017")
    db = client["streaming_results"]
    ensure_operational_indexes(db)
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database

# --- Conexión por defecto (desde el host, fuera de Docker) ---
DEFAULT_MONGO_URI = "mongodb://localhost:27017"
DEFAULT_DB_NAME = "streaming_results"

# Colecciones escritas por Flink
COLL_TRENDING = "trending_movies"
COLL_GENRE = "genre_activity"
COLL_ANOMALY = "anomaly_alerts"

# Colección materializada para comparativa PK vs PostgreSQL
COLL_GOLD_RECOS = "gold_user_recommendations"


def get_db(
    uri: str = DEFAULT_MONGO_URI,
    db_name: str = DEFAULT_DB_NAME,
    *,
    server_selection_timeout_ms: int = 5000,
) -> Database:
    """
    `server_selection_timeout_ms` bajo evita que el notebook quede minutos
    colgado si Mongo no está levantado (valor por defecto de PyMongo es ~30s).
    """
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=min(server_selection_timeout_ms, 10_000),
    )
    return client[db_name]


def ensure_operational_indexes(db: Database) -> None:
    """
    Índices alineados con consultas operacionales (trending, género, anomalías).
    createIndex es idempotente si el índice ya existe con la misma clave.
    """
    db[COLL_TRENDING].create_index(
        [("window_end", DESCENDING), ("rating_count", DESCENDING)],
        name="idx_trending_window_end_rating_count",
    )
    db[COLL_TRENDING].create_index(
        [("movie_id", ASCENDING), ("window_end", DESCENDING)],
        name="idx_trending_movie_window",
    )

    db[COLL_GENRE].create_index(
        [("genre", ASCENDING), ("window_end", DESCENDING)],
        name="idx_genre_genre_window_end",
    )
    db[COLL_GENRE].create_index(
        [("window_end", DESCENDING)],
        name="idx_genre_window_end",
    )

    db[COLL_ANOMALY].create_index(
        [("user_id", ASCENDING), ("detected_at", DESCENDING)],
        name="idx_anomaly_user_detected",
    )

    db[COLL_GOLD_RECOS].create_index(
        [("user_id", ASCENDING)],
        unique=True,
        name="uniq_gold_reco_user",
    )


# ========= Consultas operacionales (preguntas típicas del tablero en vivo) =========


def q_trending_top_k_latest_window(db: Database, k: int = 10) -> List[Dict[str, Any]]:
    """
    Top-K películas con más eventos en la ventana de tiempo más reciente
    (la de mayor window_end en la colección).
    """
    latest = db[COLL_TRENDING].find_one(sort=[("window_end", DESCENDING)])
    if not latest:
        return []
    end = latest["window_end"]
    cur = (
        db[COLL_TRENDING]
        .find({"window_end": end})
        .sort([("rating_count", DESCENDING), ("movie_id", ASCENDING)])
        .limit(k)
    )
    return list(cur)


def q_genre_most_active_in_recent_windows(
    db: Database,
    last_n_windows: int = 30,
) -> List[Dict[str, Any]]:
    """
    Agregación operacional: suma de eventos por género en las últimas N ventanas
    (por window_end distinto, de mayor a menor).
    """
    recent_ends = (
        db[COLL_GENRE]
        .aggregate(
            [
                {"$group": {"_id": "$window_end"}},
                {"$sort": {"_id": -1}},
                {"$limit": last_n_windows},
            ]
        )
    )
    ends = [d["_id"] for d in recent_ends]
    if not ends:
        return []

    pipeline = [
        {"$match": {"window_end": {"$in": ends}}},
        {"$group": {"_id": "$genre", "total_events": {"$sum": "$event_count"}}},
        {"$sort": {"total_events": -1}},
    ]
    return list(db[COLL_GENRE].aggregate(pipeline))


def q_recent_anomaly_alerts(db: Database, limit: int = 10) -> List[Dict[str, Any]]:
    """Alertas globales más recientes (panel de monitoreo en vivo)."""
    cur = (
        db[COLL_ANOMALY]
        .find({})
        .sort([("detected_at", DESCENDING), ("_id", DESCENDING)])
        .limit(limit)
    )
    return list(cur)


def q_latest_anomaly_alert(db: Database) -> Optional[Dict[str, Any]]:
    """Documento de alerta más reciente en la colección."""
    return db[COLL_ANOMALY].find_one(
        sort=[("detected_at", DESCENDING), ("_id", DESCENDING)]
    )


def q_anomaly_alerts_since_minutes(
    db: Database,
    minutes: int = 30,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """
    Alertas detectadas en los últimos N minutos (ISO UTC en detected_at).
    Útil para el panel en vivo sin mostrar solo histórico antiguo.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    cur = (
        db[COLL_ANOMALY]
        .find({"detected_at": {"$gte": cutoff}})
        .sort([("detected_at", DESCENDING), ("_id", DESCENDING)])
        .limit(limit)
    )
    return list(cur)


def q_anomaly_alerts_for_user(
    db: Database,
    user_id: int,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Historial operacional de alertas para un usuario (orden reciente)."""
    cur = (
        db[COLL_ANOMALY]
        .find({"user_id": user_id})
        .sort([("detected_at", DESCENDING)])
        .limit(limit)
    )
    return list(cur)


def q_recommendations_for_user(
    db: Database,
    user_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Lectura por clave lógica (user_id) sobre la colección materializada Gold.
    Un documento por usuario acelera el patrón app -> "dashboard de recomendaciones".
    """
    return db[COLL_GOLD_RECOS].find_one({"user_id": user_id})


# ========= Benchmark genérico =========


def benchmark_latencies(
    fn: Callable[[], Any],
    repetitions: int = 10,
    warmup: int = 2,
) -> Dict[str, Any]:
    """
    Mide latencias de `fn` (sin argumentos; usar lambda o closure).

    Devuelve dict con muestras en ms, media y desviación estándar de `repetitions`
    ejecuciones tras `warmup` calentamientos.
    """
    if repetitions < 10:
        raise ValueError("Se requieren al menos 10 repeticiones para el informe.")

    for _ in range(warmup):
        fn()

    samples_ms: List[float] = []
    for _ in range(repetitions):
        t0 = time.perf_counter()
        fn()
        samples_ms.append((time.perf_counter() - t0) * 1000.0)

    return {
        "samples_ms": samples_ms,
        "mean_ms": mean(samples_ms),
        "stdev_ms": stdev(samples_ms) if len(samples_ms) > 1 else 0.0,
        "repetitions": repetitions,
        "warmup": warmup,
    }


def summarize_batches(results: Sequence[Dict[str, Any]]) -> Dict[str, float]:
    """Agrega medias de varias corridas (p. ej. un bloque por usuario distinto)."""
    all_means = [r["mean_ms"] for r in results]
    return {
        "mean_of_means_ms": mean(all_means),
        "stdev_across_blocks_ms": stdev(all_means) if len(all_means) > 1 else 0.0,
    }


# ========= Materializar Gold en Mongo (modelo documento por usuario) =========


def build_user_reco_docs(
    rows: Sequence[Tuple[int, int, float]],
) -> Dict[int, Dict[str, Any]]:
    """
    Agrupa filas (user_id, movie_id, score) en documentos anidados por usuario.
    """
    by_user: Dict[int, List[Dict[str, Any]]] = {}
    for uid, mid, score in rows:
        by_user.setdefault(uid, []).append(
            {"movie_id": mid, "predicted_score": float(score)}
        )
    out: Dict[int, Dict[str, Any]] = {}
    for uid, items in by_user.items():
        out[uid] = {"user_id": uid, "items": items}
    return out


def replace_gold_recommendations(
    db: Database,
    rows: Sequence[Tuple[int, int, float]],
    batch_size: int = 500,
) -> None:
    """
    Reemplazo completo de la colección materializada (idempotente para una corrida).
    """
    db[COLL_GOLD_RECOS].drop()
    ensure_operational_indexes(db)

    docs = list(build_user_reco_docs(rows).values())
    if not docs:
        return

    for i in range(0, len(docs), batch_size):
        db[COLL_GOLD_RECOS].insert_many(docs[i : i + batch_size])


def bulk_insert_recommendations_normalized(
    db: Database,
    rows: Sequence[Tuple[int, int, float]],
    batch_size: int = 2000,
) -> float:
    """
    Escritura masiva en modelo normalizado (un documento por fila).
    Útil para medir throughput de insert many vs PostgreSQL.
    Retorna segundos totales.
    """
    coll = db["gold_recommendations_normalized"]
    coll.drop()
    coll.create_index([("user_id", ASCENDING), ("movie_id", ASCENDING)], unique=True)

    docs = [
        {"user_id": u, "movie_id": m, "predicted_score": float(s)} for u, m, s in rows
    ]
    t0 = time.perf_counter()
    for i in range(0, len(docs), batch_size):
        coll.insert_many(docs[i : i + batch_size])
    return time.perf_counter() - t0


def mirror_genre_activity_to_list(
    db: Database,
    limit_docs: int = 20_000,
) -> List[Dict[str, Any]]:
    """
    Lee hasta `limit_docs` de genre_activity para replicar en PostgreSQL y comparar
    la misma agregación en ambos motores.
    """
    cur = db[COLL_GENRE].find().sort([("window_end", DESCENDING)]).limit(limit_docs)
    return list(cur)


# ========= Explicaciones (para documentación en notebook) =========


def explain_trending_query(db: Database) -> Any:
    """Última ventana + sort — usar en notebook para justificar índices."""
    latest = db[COLL_TRENDING].find_one(sort=[("window_end", DESCENDING)])
    if not latest:
        return None
    end = latest["window_end"]
    return db[COLL_TRENDING].find({"window_end": end}).sort(
        [("rating_count", DESCENDING)]
    ).explain()


if __name__ == "__main__":
    db = get_db()
    ensure_operational_indexes(db)
    print("Índices operacionales asegurados.")
    print("Trending top 3:", q_trending_top_k_latest_window(db, 3)[:1])
    print("Género (agg):", q_genre_most_active_in_recent_windows(db)[:3])
