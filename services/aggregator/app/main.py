"""Aggregation Service: вычисление бизнес-метрик из ClickHouse и выгрузка в PostgreSQL.

Цикл работы (по расписанию или по HTTP):
    ClickHouse (raw events) --SELECT--> метрики --INSERT--> ClickHouse (агрегаты)
                                      `--UPSERT--> PostgreSQL (готовые метрики)
Плюс ежедневный экспорт агрегатов из PostgreSQL в S3 (MinIO).
"""
import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .clickhouse_repo import ClickHouseRepository
from .config import settings
from .pg_store import PostgresStore
from .s3_exporter import S3Exporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("aggregation-service")

app = FastAPI(title="Aggregation Service", version="1.0.0")
scheduler = BackgroundScheduler()

ch_repo = ClickHouseRepository()
pg_store = PostgresStore()
s3_exporter = S3Exporter()


# ------------------------------------------------------------- aggregation
def aggregate_day(day: date) -> dict:
    """Пересчёт всех метрик за дату: raw -> метрики -> CH-агрегаты -> PostgreSQL."""
    start = time.monotonic()

    overview = ch_repo.compute_daily_overview(day)
    top_movies = ch_repo.compute_top_movies(day)
    retention = ch_repo.compute_retention(day)
    raw_count = ch_repo.count_raw_events(day)

    ch_repo.materialize_day(day, overview, top_movies, retention)

    calculated_at = datetime.now(timezone.utc)
    rows = [
        (day, "dau", {}, overview["dau"], calculated_at),
        (day, "avg_view_seconds", {}, overview["avg_view_seconds"], calculated_at),
        (day, "views_started", {}, overview["views_started"], calculated_at),
        (day, "views_finished", {}, overview["views_finished"], calculated_at),
        (day, "view_conversion", {}, overview["view_conversion"], calculated_at),
        (day, "retention_d1", {}, retention["retention_d1"], calculated_at),
        (day, "retention_d7", {}, retention["retention_d7"], calculated_at),
    ]
    rows += [
        (day, "top_movie", {"movie_id": m["movie_id"]}, m["views_started"], calculated_at)
        for m in top_movies
    ]
    pg_store.upsert_metrics(rows)

    elapsed = time.monotonic() - start
    logger.info(
        "пересчёт за %s: DAU=%d conversion=%.4f retention_d1=%.4f retention_d7=%.4f, %.2fs",
        day, overview["dau"], overview["view_conversion"],
        retention["retention_d1"], retention["retention_d7"], elapsed,
    )
    return {
        "date": day.isoformat(),
        "raw_events": raw_count,
        "overview": overview,
        "retention": retention,
        "top_movies": top_movies,
        "elapsed_seconds": round(elapsed, 3),
    }


def aggregate_window() -> dict:
    """Пересчёт окна последних N дней + когортная матрица retention."""
    started = time.monotonic()
    today = date.today()
    window = settings.AGGREGATION_WINDOW_DAYS
    days = [today - timedelta(days=i) for i in range(window - 1, -1, -1)]

    total_raw = 0
    results = []
    for day in days:
        try:
            result = aggregate_day(day)
            total_raw += result["raw_events"]
            results.append(result)
        except Exception:  # noqa: BLE001
            logger.exception("ошибка пересчёта за %s", day)

    # Когортная матрица для Grafana heatmap (маериализуем целиком за окно).
    try:
        matrix = ch_repo.compute_cohort_matrix(window)
        ch_repo.materialize_cohort_matrix(matrix)
        logger.info("когортная матрица retention пересчитана: %d строк", len(matrix))
    except Exception:  # noqa: BLE001
        logger.exception("ошибка пересчёта когортной матрицы")

    elapsed = time.monotonic() - started
    logger.info(
        "Цикл завершён, обработано %d записей за %.2f сек (дат: %d)",
        total_raw, elapsed, len(results),
    )
    return {
        "days_processed": len(results),
        "raw_events": total_raw,
        "elapsed_seconds": round(elapsed, 3),
    }


def scheduled_aggregation() -> None:
    logger.info("=== старт цикла агрегации по расписанию ===")
    try:
        aggregate_window()
    except Exception:  # noqa: BLE001
        logger.exception("цикл агрегации завершился с ошибкой")


def scheduled_export() -> None:
    day = date.today() - timedelta(days=settings.EXPORT_DAYS_BACK)
    logger.info("=== экспорт в S3 за %s по расписанию ===", day)
    try:
        export_day(day)
    except Exception:  # noqa: BLE001
        # Ошибка залогирована; повтор произойдёт при следующем запуске по расписанию.
        logger.error("экспорт за %s не выполнен, будет повторён при следующем запуске", day)


# ----------------------------------------------------------------- export
def export_day(day: date) -> dict:
    metrics = pg_store.get_metrics(day)
    if not metrics:
        logger.warning("экспорт за %s: метрик нет, файл не создан", day)
        return {"date": day.isoformat(), "exported": 0, "key": None}
    key = s3_exporter.export_day(day, metrics)
    return {"date": day.isoformat(), "exported": len(metrics), "key": key}


# -------------------------------------------------------------------- API
class DateRequest(BaseModel):
    date: date


@app.on_event("startup")
def startup() -> None:
    # Ждём, пока БД поднимутся (зависимости в compose + повторные попытки здесь).
    for name, fn in (
        ("ClickHouse", ch_repo.connect),
        ("PostgreSQL", pg_store.connect),
        ("S3", s3_exporter.ensure_bucket),
    ):
        for attempt in range(30):
            try:
                fn()
                break
            except Exception as exc:  # noqa: BLE001
                logger.warning("%s недоступен (%s), попытка %d/30", name, exc, attempt + 1)
                time.sleep(3)
        else:
            raise RuntimeError(f"{name} не поднялся")

    scheduler.add_job(
        scheduled_aggregation,
        "interval",
        seconds=settings.AGGREGATION_INTERVAL_SECONDS,
        id="aggregation",
        next_run_time=datetime.now() + timedelta(seconds=10),  # первый запуск скоро после старта
    )
    scheduler.add_job(
        scheduled_export,
        "interval",
        seconds=settings.EXPORT_INTERVAL_SECONDS,
        id="export",
        next_run_time=datetime.now() + timedelta(seconds=45),
    )
    scheduler.start()
    logger.info(
        "расписание: агрегация каждые %ds, экспорт каждые %ds",
        settings.AGGREGATION_INTERVAL_SECONDS, settings.EXPORT_INTERVAL_SECONDS,
    )


@app.on_event("shutdown")
def shutdown() -> None:
    scheduler.shutdown(wait=False)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/aggregate")
def aggregate(req: DateRequest) -> dict:
    """Ручной пересчёт за конкретную дату."""
    try:
        return aggregate_day(req.date)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ошибка ручного пересчёта за %s", req.date)
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/aggregate/run")
def aggregate_window_endpoint() -> dict:
    """Ручной запуск полного цикла по окну (как в расписании)."""
    try:
        return aggregate_window()
    except Exception as exc:  # noqa: BLE001
        logger.exception("ошибка ручного цикла агрегации")
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/export")
def export(req: DateRequest) -> dict:
    """Ручной экспорт агрегатов за дату в S3."""
    try:
        return export_day(req.date)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ошибка экспорта за %s", req.date)
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/metrics")
def get_metrics(date: date) -> dict:  # noqa: A002 — имя параметра из API
    metrics = pg_store.get_metrics(date)
    for m in metrics:
        m["metric_date"] = m["metric_date"].isoformat()
        m["calculated_at"] = m["calculated_at"].isoformat()
    return {"date": date.isoformat(), "metrics": metrics}


if __name__ == "__main__":
    uvicorn.run(app, host=settings.API_HOST, port=settings.API_PORT)
