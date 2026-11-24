import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


class Settings:
    # ClickHouse (native protocol)
    CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    CLICKHOUSE_PORT = _int("CLICKHOUSE_PORT", 9000)
    CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "cinema")
    CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse")

    # PostgreSQL
    POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = _int("POSTGRES_PORT", 5432)
    POSTGRES_DB = os.environ.get("POSTGRES_DB", "cinema")
    POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
    POSTGRES_MAX_RETRIES = _int("POSTGRES_MAX_RETRIES", 5)
    POSTGRES_BACKOFF_BASE_MS = _int("POSTGRES_BACKOFF_BASE_MS", 300)

    # Расписание агрегации (секунды) и глубина окна пересчёта
    AGGREGATION_INTERVAL_SECONDS = _int("AGGREGATION_INTERVAL_SECONDS", 60)
    AGGREGATION_WINDOW_DAYS = _int("AGGREGATION_WINDOW_DAYS", 14)

    # Расписание экспорта в S3 (секунды)
    EXPORT_INTERVAL_SECONDS = _int("EXPORT_INTERVAL_SECONDS", 120)
    EXPORT_DAYS_BACK = _int("EXPORT_DAYS_BACK", 1)  # за какой день экспортируем (1 = вчера)

    # S3 (MinIO)
    S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minio")
    S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minio123")
    S3_BUCKET = os.environ.get("S3_BUCKET", "movie-analytics")
    S3_REGION = os.environ.get("S3_REGION", "us-east-1")
    S3_MAX_RETRIES = _int("S3_MAX_RETRIES", 4)

    API_HOST = os.environ.get("API_HOST", "0.0.0.0")
    API_PORT = _int("API_PORT", 8001)


settings = Settings()
