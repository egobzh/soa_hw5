import json
import os
import time
import uuid
from datetime import datetime, timezone

import pytest
import requests

PRODUCER_URL = os.environ.get("PRODUCER_URL", "http://localhost:8000")
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "cinema")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse")

WAIT_TIMEOUT = 90  # секунд на прохождение события Kafka -> ClickHouse


def wait_until(predicate, timeout: float = WAIT_TIMEOUT, interval: float = 2.0, message: str = ""):
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        time.sleep(interval)
    raise AssertionError(f"условие не выполнено за {timeout}s: {message}; последняя ошибка: {last_exc}")


def ch_query(sql: str) -> list[dict]:
    resp = requests.post(
        CLICKHOUSE_URL,
        params={
            "database": CLICKHOUSE_DB,
            "default_format": "JSONEachRow",
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        },
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
        timeout=15,
    )
    resp.raise_for_status()
    if not resp.text.strip():
        return []
    return [json.loads(line) for line in resp.text.strip().splitlines()]


@pytest.fixture(scope="session", autouse=True)
def wait_services():
    """Ожидание готовности сервисов — тесты запускаются одной командой."""
    wait_until(
        lambda: requests.get(f"{PRODUCER_URL}/health", timeout=5).status_code == 200,
        timeout=300, interval=5, message="producer /health",
    )
    wait_until(
        lambda: ch_query("SELECT 1"),
        timeout=120, interval=5, message="clickhouse ping",
    )


@pytest.fixture()
def unique() -> str:
    return uuid.uuid4().hex[:12]


@pytest.fixture()
def make_event(unique):
    def _make(**overrides):
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": f"test-user-{unique}",
            "movie_id": f"test-movie-{unique}",
            "event_type": "VIEW_STARTED",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "device_type": "DESKTOP",
            "session_id": f"test-session-{unique}",
            "progress_seconds": 0,
        }
        event.update(overrides)
        return event

    return _make
