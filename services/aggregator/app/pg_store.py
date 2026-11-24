"""PostgreSQL-хранилище готовых метрик с идемпотентной записью (UPSERT)."""
import logging
import time
from contextlib import contextmanager
from datetime import date, datetime

import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError
from psycopg2.extras import Json

from .config import settings

logger = logging.getLogger("aggregation-service.postgres")

UPSERT_SQL = """
INSERT INTO metrics (metric_date, metric_name, dimensions, value, calculated_at)
VALUES %s
ON CONFLICT (metric_date, metric_name, dimensions) DO UPDATE
SET value = EXCLUDED.value,
    calculated_at = EXCLUDED.calculated_at
"""

EXPORT_SQL = """
SELECT metric_date, metric_name, dimensions::text AS dimensions, value, calculated_at
FROM metrics
WHERE metric_date = %s
ORDER BY metric_name, dimensions
"""


class PostgresStore:
    def __init__(self) -> None:
        self._conn = None

    def _connect(self):
        return psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            connect_timeout=10,
        )

    def connect(self) -> None:
        self._conn = self._connect()
        self._conn.autocommit = False
        logger.info(
            "PostgreSQL подключён: %s:%s/%s",
            settings.POSTGRES_HOST, settings.POSTGRES_PORT, settings.POSTGRES_DB,
        )

    @contextmanager
    def _reconnectable(self):
        for attempt in range(settings.POSTGRES_MAX_RETRIES + 1):
            try:
                if self._conn is None or self._conn.closed:
                    logger.warning("PostgreSQL: переподключение...")
                    self._conn = self._connect()
                yield self._conn
                return
            except OperationalError as exc:
                wait = settings.POSTGRES_BACKOFF_BASE_MS * (2 ** attempt) / 1000.0
                logger.error(
                    "PostgreSQL недоступен (%s), повтор через %.1fs (попытка %d/%d)",
                    exc, wait, attempt + 1, settings.POSTGRES_MAX_RETRIES,
                )
                try:
                    if self._conn:
                        self._conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self._conn = None
                if attempt == settings.POSTGRES_MAX_RETRIES:
                    raise
                time.sleep(wait)

    def upsert_metrics(self, rows: list[tuple]) -> int:
        """rows: (metric_date, metric_name, dimensions_dict, value, calculated_at)."""
        if not rows:
            return 0
        values = [
            (d, name, Json(dims), val, calc)
            for (d, name, dims, val, calc) in rows
        ]
        with self._reconnectable() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, UPSERT_SQL, values)
            conn.commit()
        logger.info("PostgreSQL: UPSERT %d метрик", len(values))
        return len(values)

    def get_metrics(self, day: date) -> list[dict]:
        with self._reconnectable() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(EXPORT_SQL, (day,))
                return [dict(r) for r in cur.fetchall()]
