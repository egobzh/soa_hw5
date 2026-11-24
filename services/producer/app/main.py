"""Movie Service (Producer): HTTP API приёма событий + генератор синтетического трафика."""
import logging
import threading

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import settings
from .event_schema import EventSchema, InvalidEventError, SchemaRegistryError
from .generator import EventGenerator, run_generator_loop
from .kafka_producer import MovieEventProducer, wait_kafka_available

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("movie-service")

app = FastAPI(title="Movie Service (Producer)", version="1.0.0")

schema: EventSchema | None = None
producer: MovieEventProducer | None = None


class EventIn(BaseModel):
    event_id: str | None = None
    user_id: str
    movie_id: str = ""
    event_type: str
    timestamp: str | int | float
    device_type: str
    session_id: str
    progress_seconds: int = 0


class GenerateRequest(BaseModel):
    count: int = 100
    days: int | None = None  # если указано — генерируем историю, а не realtime


@app.on_event("startup")
def startup() -> None:
    global schema, producer
    logger.info("старт Movie Service; ожидание Kafka...")
    wait_kafka_available()

    schema = EventSchema(settings.SCHEMA_REGISTRY_URL, f"{settings.KAFKA_TOPIC}-value")
    schema.load()
    logger.info(
        "схема загружена из Schema Registry: id=%s version=%s",
        schema.schema_id, schema.version,
    )

    producer = MovieEventProducer(schema)
    logger.info("продюсер готов: acks=%s topic=%s", settings.PRODUCER_ACKS, settings.KAFKA_TOPIC)

    if settings.GENERATOR_ENABLED:
        gen = EventGenerator()
        thread = threading.Thread(target=run_generator_loop, args=(producer, gen), daemon=True)
        thread.start()
        logger.info("генератор событий запущен")


@app.get("/health")
def health() -> dict:
    status = {"status": "ok", "topic": settings.KAFKA_TOPIC}
    if schema is not None:
        status["schema_id"] = schema.schema_id
        status["schema_version"] = schema.version
    return status


@app.post("/events")
def publish_event(event: EventIn) -> dict:
    """Принимает JSON-событие, валидирует по схеме и публикует в Kafka."""
    if producer is None or schema is None:
        raise HTTPException(status_code=503, detail="сервис не инициализирован")

    payload = event.model_dump()
    if not payload.get("event_id"):
        import uuid

        payload["event_id"] = str(uuid.uuid4())

    try:
        normalized = schema.normalize_and_validate(payload)
    except SchemaRegistryError as exc:
        logger.error("ошибка Schema Registry: %s", exc)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except InvalidEventError as exc:
        logger.warning("отклонено событие: %s", exc)
        raise HTTPException(status_code=422, detail={"code": exc.code, "message": str(exc)}) from exc

    try:
        producer.send(normalized)
    except Exception as exc:  # noqa: BLE001
        logger.error("ошибка публикации event_id=%s: %s", payload["event_id"], exc)
        raise HTTPException(status_code=502, detail=f"ошибка публикации в Kafka: {exc}") from exc

    return {"status": "published", "event_id": normalized["event_id"]}


@app.post("/generate")
def generate(req: GenerateRequest) -> dict:
    """Ручной запуск генератора: realtime-пакет или исторический бэкфилл."""
    if producer is None or schema is None:
        raise HTTPException(status_code=503, detail="сервис не инициализирован")

    gen = EventGenerator()
    if req.days:
        events = gen.generate_history(req.days, req.count)
    else:
        events = gen.generate_batch(req.count)

    # Валидация каждого события перед отправкой (тот же кодовый путь, что и в /events).
    published = 0
    for ev in events:
        try:
            producer.send(schema.normalize_and_validate(ev))
            published += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("generate: событие пропущено: %s", exc)
    producer.flush()
    return {"status": "ok", "generated": len(events), "published": published}


if __name__ == "__main__":
    uvicorn.run(app, host=settings.API_HOST, port=settings.API_PORT)
