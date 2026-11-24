import os


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


class Settings:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092")
    SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "movie-events")

    # Producer: подтверждение записи и повторные попытки
    PRODUCER_ACKS = os.environ.get("PRODUCER_ACKS", "all")          # ждём всех ISR
    PRODUCER_RETRIES = int(os.environ.get("PRODUCER_RETRIES", "10"))
    PRODUCER_MAX_RETRIES = int(os.environ.get("PRODUCER_MAX_RETRIES", "5"))  # повторы в коде (backoff)
    PRODUCER_BACKOFF_BASE_MS = int(os.environ.get("PRODUCER_BACKOFF_BASE_MS", "200"))

    # HTTP API
    API_HOST = os.environ.get("API_HOST", "0.0.0.0")
    API_PORT = int(os.environ.get("API_PORT", "8000"))

    # Генератор синтетических событий
    GENERATOR_ENABLED = _bool("GENERATOR_ENABLED", True)
    GENERATOR_USERS = int(os.environ.get("GENERATOR_USERS", "2000"))
    GENERATOR_HISTORY_DAYS = int(os.environ.get("GENERATOR_HISTORY_DAYS", "14"))
    GENERATOR_BATCH_SIZE = int(os.environ.get("GENERATOR_BATCH_SIZE", "300"))
    GENERATOR_INTERVAL_SECONDS = float(os.environ.get("GENERATOR_INTERVAL_SECONDS", "3"))
    GENERATOR_BACKFILL = _bool("GENERATOR_BACKFILL", True)


settings = Settings()
