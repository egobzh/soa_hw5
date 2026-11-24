"""Загрузка схемы события из Schema Registry, нормализация и валидация событий."""
import json
import re
import urllib.request
from datetime import datetime, timezone
from uuid import UUID

from fastavro import parse_schema
from fastavro.validation import validate

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

EVENT_TYPES = ("VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED")
DEVICE_TYPES = ("MOBILE", "DESKTOP", "TV", "TABLET")

ERROR_SCHEMA_REGISTRY = "schema_registry_unavailable"
ERROR_INVALID_EVENT = "invalid_event"


class SchemaRegistryError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.code = ERROR_SCHEMA_REGISTRY


class InvalidEventError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.code = ERROR_INVALID_EVENT


class EventSchema:
    """Клиент Schema Registry + валидатор по Avro-схеме (включая enum-поля)."""

    def __init__(self, registry_url: str, subject: str):
        self.registry_url = registry_url.rstrip("/")
        self.subject = subject
        self.parsed_schema = None
        self.schema_id = None
        self.version = None

    def load(self) -> None:
        url = f"{self.registry_url}/subjects/{self.subject}/versions/latest"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 — приводим к доменной ошибке
            raise SchemaRegistryError(f"не удалось загрузить схему {self.subject}: {exc}") from exc

        self.schema_id = payload.get("id")
        self.version = payload.get("version")
        self.parsed_schema = parse_schema(json.loads(payload["schema"]))

    def normalize_and_validate(self, raw: dict) -> dict:
        """Проверяет событие по схеме Avro и приводит его к каноническому виду.

        Канонический вид (для JSON в Kafka):
        event_id, user_id, movie_id, event_type (имя enum), timestamp (long, ms),
        device_type (имя enum), session_id, progress_seconds (int).
        """
        if not isinstance(raw, dict):
            raise InvalidEventError("событие должно быть JSON-объектом")

        # --- Нормализация timestamp -> epoch millis (UTC) ---
        ts = raw.get("timestamp")
        if ts is None:
            raise InvalidEventError("поле 'timestamp' обязательно")
        if isinstance(ts, str):
            text = ts.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(text)
            except ValueError as exc:
                raise InvalidEventError(f"некорректный 'timestamp': {ts!r}") from exc
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts_ms = int(dt.timestamp() * 1000)
        elif isinstance(ts, (int, float)):
            # Считаем, что переданы секунды, если число слишком маленькое для millis.
            ts_ms = int(ts * 1000) if ts < 10**12 else int(ts)
        else:
            raise InvalidEventError(f"некорректный 'timestamp': {ts!r}")

        # --- Нормализация UUID ---
        event_id = raw.get("event_id")
        if event_id is None or event_id == "":
            raise InvalidEventError("поле 'event_id' обязательно")
        if not _UUID_RE.match(str(event_id)):
            raise InvalidEventError(f"'event_id' должен быть UUID: {event_id!r}")

        event = {
            "event_id": str(event_id),
            "user_id": str(raw.get("user_id") or ""),
            "movie_id": str(raw.get("movie_id") or ""),
            "event_type": str(raw.get("event_type") or ""),
            "timestamp": ts_ms,
            "device_type": str(raw.get("device_type") or ""),
            "session_id": str(raw.get("session_id") or ""),
            "progress_seconds": int(raw.get("progress_seconds") or 0),
        }

        if not event["user_id"]:
            raise InvalidEventError("поле 'user_id' обязательно")
        if not event["session_id"]:
            raise InvalidEventError("поле 'session_id' обязательно")
        if event["event_type"] not in EVENT_TYPES:
            raise InvalidEventError(
                f"недопустимый 'event_type': {event['event_type']!r}, допустимы {EVENT_TYPES}"
            )
        if event["device_type"] not in DEVICE_TYPES:
            raise InvalidEventError(
                f"недопустимый 'device_type': {event['device_type']!r}, допустимы {DEVICE_TYPES}"
            )
        if event["progress_seconds"] < 0:
            raise InvalidEventError("'progress_seconds' не может быть отрицательным")

        # Валидация по зарегистрированной Avro-схеме (enum'ы, типы, logicalType uuid).
        try:
            validate(event, self.parsed_schema)
        except Exception as exc:  # noqa: BLE001
            raise InvalidEventError(f"событие не прошло валидацию по схеме: {exc}") from exc
        return event

    @staticmethod
    def to_kafka_json(event: dict) -> bytes:
        """Сериализация в JSON для топика (ClickHouse читает JSONEachRow)."""
        return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
