"""Интеграционные тесты pipeline: публикация в Kafka -> появление в ClickHouse.

Запуск одной командой (см. README):
    docker compose --profile test run --rm test-runner
Тесты изолированы уникальными event_id/user_id, поэтому воспроизводимы
и не мешают друг другу при повторных запусках.
"""
import uuid

import requests

from conftest import PRODUCER_URL, ch_query, wait_until


def publish(event: dict) -> requests.Response:
    return requests.post(f"{PRODUCER_URL}/events", json=event, timeout=15)


def test_event_published_via_api_appears_in_clickhouse(make_event):
    """Полный путь: HTTP API продюсера -> Kafka -> ClickHouse MV -> raw-таблица."""
    event = make_event(progress_seconds=120)
    resp = publish(event)
    assert resp.status_code == 200, resp.text
    assert resp.json()["event_id"] == event["event_id"]

    def appeared() -> bool:
        rows = ch_query(f"SELECT count() AS c FROM movie_events WHERE event_id = '{event['event_id']}'")
        return bool(rows) and int(rows[0]["c"]) == 1

    wait_until(appeared, message=f"событие {event['event_id']} не появилось в ClickHouse")

    rows = ch_query(
        "SELECT event_id, user_id, movie_id, event_type, device_type, session_id, "
        "progress_seconds, toDate(timestamp) AS d "
        f"FROM movie_events WHERE event_id = '{event['event_id']}'"
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["user_id"] == event["user_id"]
    assert row["movie_id"] == event["movie_id"]
    assert row["event_type"] == "VIEW_STARTED"
    assert row["device_type"] == "DESKTOP"
    assert row["session_id"] == event["session_id"]
    assert row["progress_seconds"] == 120


def test_viewing_progression_keeps_growing_progress(make_event):
    """VIEW_STARTED всегда предшествует VIEW_FINISHED; progress растёт."""
    session_id = f"sess-{uuid.uuid4().hex[:12]}"
    started = make_event(
        event_type="VIEW_STARTED", session_id=session_id,
        movie_id="test-movie-progress", progress_seconds=10,
    )
    finished = make_event(
        event_type="VIEW_FINISHED", session_id=session_id,
        movie_id="test-movie-progress", progress_seconds=600,
    )
    for ev in (started, finished):
        resp = publish(ev)
        assert resp.status_code == 200, resp.text

    def both_present() -> bool:
        rows = ch_query(
            "SELECT event_type, progress_seconds FROM movie_events "
            f"WHERE session_id = '{session_id}'"
        )
        got = {r["event_type"]: r["progress_seconds"] for r in rows}
        return got.get("VIEW_STARTED") == 10 and got.get("VIEW_FINISHED") == 600

    wait_until(both_present, message="сессия просмотра не восстановлена в ClickHouse")


def test_invalid_event_rejected_and_not_stored(make_event):
    """Событие с недопустимым enum-значением отклоняется валидацией по схеме."""
    bad = make_event(event_type="CLICKED_SOMETHING")  # нет в enum EventType
    resp = publish(bad)
    assert resp.status_code == 422, resp.text

    def absent() -> bool:
        rows = ch_query(f"SELECT count() AS c FROM movie_events WHERE event_id = '{bad['event_id']}'")
        return bool(rows) and int(rows[0]["c"]) == 0

    wait_until(absent, timeout=15, message="невалидное событие не должно попадать в ClickHouse")

    missing_field = make_event()
    del missing_field["user_id"]
    resp = requests.post(f"{PRODUCER_URL}/events", json=missing_field, timeout=15)
    assert resp.status_code == 422, resp.text
