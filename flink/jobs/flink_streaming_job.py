import json
import logging
import os
from datetime import datetime, timezone

import pymongo
from pyflink.common import Time, Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import FlatMapFunction, ProcessWindowFunction
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    SlidingEventTimeWindows,
    EventTimeSessionWindows,
)

# ---------------------------------------------------------------------------
# CONFIGURACION POR VARIABLES DE ENTORNO (valores default para Docker Compose)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "platform-events")
KAFKA_GROUP = os.getenv("KAFKA_GROUP_ID", "flink-movie-platform-consumer")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = os.getenv("MONGO_DB", "streaming_results")

COLL_TRENDING = "trending_movies"
COLL_GENRE = "genre_activity"
COLL_ANOMALY = "anomaly_alerts"

CHECKPOINT_INTERVAL_MS = 30_000      # 30 segundos
MAX_OUT_OF_ORDERNESS_MS = 30_000     # watermark tolerance
IDLENESS_MS = 60_000                 # idle source timeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("FlinkStreamingJob")


# ---------------------------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------------------------

def _fmt_ts(epoch_ms: int) -> str:
    """Convierte epoch millis a ISO-8601 UTC string."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def _parse_json(raw: str):
    """Parsea string JSON a dict Python. Devuelve None si falla."""
    try:
        evt = json.loads(raw)
        # Normalizar timestamp del evento a epoch millis para EventTime
        ts_str = evt.get("timestamp")
        if ts_str:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            evt["event_ts"] = int(dt.timestamp() * 1000)
        else:
            evt["event_ts"] = 0
        return evt
    except Exception as exc:
        logger.warning(f"Evento descartado por parseo invalido: {exc}")
        return None


# ---------------------------------------------------------------------------
# FLATMAP: EXPANDIR EVENTO POR CADA GENERO
# ---------------------------------------------------------------------------

class ExpandByGenre(FlatMapFunction):
    """
    Un evento puede tener N generos. Esta funcion emite N sub-eventos,
    uno por genero, para permitir agregacion por genero posteriormente.
    """

    def flat_map(self, event):
        genres = event.get("genres", [])
        if not genres:
            genres = ["Unknown"]
        for g in genres:
            yield {
                "user_id": event["user_id"],
                "movie_id": event["movie_id"],
                "movie_title": event.get("movie_title", ""),
                "rating": event.get("rating"),
                "event_type": event["event_type"],
                "event_ts": event["event_ts"],
                "genre": g,
            }


# ---------------------------------------------------------------------------
# PROCESS WINDOW FUNCTIONS (logica de ventana + sink a MongoDB integrado)
# ---------------------------------------------------------------------------
# Nota: En un entorno productivo se usaria un SinkFunction oficial.
# Para este proyecto academico, ProcessWindowFunction con side-effect a MongoDB
# es pragmatico, simple de depurar y cumple el requisito de entregable.

class TrendingMoviesWindowProcess(ProcessWindowFunction):
    """
    Ventana Tumbling de 5 minutos keyed by movie_id.
    Cuenta cuantos ratings recibio cada pelicula en la ventana
    e inserta un documento en MongoDB.
    """

    def open(self, runtime_context):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.coll = self.client[MONGO_DB][COLL_TRENDING]
        logger.info("[Trending] Sink MongoDB conectado")

    def close(self):
        self.client.close()
        logger.info("[Trending] Sink MongoDB cerrado")

    def process(self, movie_id, context, elements, collector):
        count = 0
        title = None
        for e in elements:
            count += 1
            if title is None and e.get("movie_title"):
                title = e["movie_title"]

        doc = {
            "window_start": _fmt_ts(context.window().start),
            "window_end": _fmt_ts(context.window().end),
            "movie_id": movie_id,
            "movie_title": title or f"Movie-{movie_id}",
            "rating_count": count,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.coll.insert_one(doc)
            logger.info(f"[Trending] movie_id={movie_id} count={count}")
            collector.collect(doc)
        except Exception as exc:
            logger.error(f"[Trending] Error MongoDB: {exc}")


class GenreActivityWindowProcess(ProcessWindowFunction):
    """
    Ventana Sliding de 10 minutos con paso de 1 minuto keyed by genre.
    Cuenta eventos por genero e inserta en MongoDB.
    """

    def open(self, runtime_context):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.coll = self.client[MONGO_DB][COLL_GENRE]
        logger.info("[Genre] Sink MongoDB conectado")

    def close(self):
        self.client.close()
        logger.info("[Genre] Sink MongoDB cerrado")

    def process(self, genre, context, elements, collector):
        count = sum(1 for _ in elements)
        doc = {
            "window_start": _fmt_ts(context.window().start),
            "window_end": _fmt_ts(context.window().end),
            "genre": genre,
            "event_count": count,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.coll.insert_one(doc)
            logger.info(f"[Genre] genre={genre} count={count}")
            collector.collect(doc)
        except Exception as exc:
            logger.error(f"[Genre] Error MongoDB: {exc}")


class AnomalyDetectionWindowProcess(ProcessWindowFunction):
    """
    Ventana de Sesion con gap de 2 minutos keyed by user_id.
    Solo analiza eventos de tipo 'rating'.
    Si una sesion acumula >20 eventos, genera alerta en MongoDB.
    """

    def open(self, runtime_context):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.coll = self.client[MONGO_DB][COLL_ANOMALY]
        logger.info("[Anomaly] Sink MongoDB conectado")

    def close(self):
        self.client.close()
        logger.info("[Anomaly] Sink MongoDB cerrado")

    def process(self, user_id, context, elements, collector):
        count = sum(1 for _ in elements)
        if count > 20:
            doc = {
                "user_id": user_id,
                "session_start": _fmt_ts(context.window().start),
                "session_end": _fmt_ts(context.window().end),
                "event_count": count,
                "alert_type": "RATING_BURST",
                "threshold": 20,
                "message": (
                    f"Usuario {user_id} emitio {count} ratings en sesion "
                    f"({_fmt_ts(context.window().start)} -> {_fmt_ts(context.window().end)})"
                ),
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                self.coll.insert_one(doc)
                logger.warning(f"[Anomaly] {doc['message']}")
                collector.collect(doc)
            except Exception as exc:
                logger.error(f"[Anomaly] Error MongoDB: {exc}")
        # Si count <= 20 no se genera documento (no es anomalia)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Parallelismo igual al numero de particiones Kafka (3)
    env.set_parallelism(3)

    # EventTime es obligatorio para ventanas basadas en timestamp del evento
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    # Checkpointing cada 30 segundos.
    # At-least-once es el modo por defecto en Flink; para exactly-once
    # se usaria CheckpointingMode.EXACTLY_ONCE (requiere idempotencia en sink).
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)

    # -----------------------------------------------------------------------
    # 1. SOURCE: Kafka Consumer
    # -----------------------------------------------------------------------
    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": "false",          # Flink maneja offsets via checkpoint
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props,
    )

    raw_stream = env.add_source(kafka_consumer)

    # -----------------------------------------------------------------------
    # 2. PARSEO + WATERMARKS (Event Time)
    # -----------------------------------------------------------------------
    parsed_stream = raw_stream.map(
        _parse_json,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    ).filter(lambda x: x is not None and x.get("event_ts", 0) > 0)

    # Timestamp assigner: extrae event_ts del dict
    class EventTimestampAssigner:
        def extract_timestamp(self, value, record_timestamp):
            return value["event_ts"]

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_millis(MAX_OUT_OF_ORDERNESS_MS))
        .with_idleness(Duration.of_millis(IDLENESS_MS))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    event_stream = parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # -----------------------------------------------------------------------
    # 3. BRANCH 1 — Trending Movies (Tumbling 5 min)
    # -----------------------------------------------------------------------
    trending_stream = (
        event_stream
        .key_by(lambda e: e["movie_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .process(
            TrendingMoviesWindowProcess(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )

    # -----------------------------------------------------------------------
    # 4. BRANCH 2 — Genre Activity (Sliding 10 min / 1 min)
    # -----------------------------------------------------------------------
    # Expandimos por genero antes de la ventana
    genre_expanded = event_stream.flat_map(
        ExpandByGenre(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    genre_stream = (
        genre_expanded
        .key_by(lambda e: e["genre"])
        .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
        .process(
            GenreActivityWindowProcess(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )

    # -----------------------------------------------------------------------
    # 5. BRANCH 3 — Anomaly Detection (Session 2 min gap)
    # -----------------------------------------------------------------------
    ratings_only = event_stream.filter(lambda e: e.get("event_type") == "rating")

    anomaly_stream = (
        ratings_only
        .key_by(lambda e: e["user_id"])
        .window(EventTimeSessionWindows.with_gap(Time.minutes(2)))
        .process(
            AnomalyDetectionWindowProcess(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )

    # -----------------------------------------------------------------------
    # 6. EJECUCION
    # -----------------------------------------------------------------------
    logger.info("========================================")
    logger.info("Flink Streaming Job iniciado")
    logger.info(f"Kafka : {KAFKA_BOOTSTRAP} / topic={KAFKA_TOPIC}")
    logger.info(f"MongoDB: {MONGO_URI} / db={MONGO_DB}")
    logger.info("Checkpoint interval: 30s")
    logger.info("Watermark delay: 30s")
    logger.info("========================================")

    env.execute("Movie Platform Streaming Job")


if __name__ == "__main__":
    main()
