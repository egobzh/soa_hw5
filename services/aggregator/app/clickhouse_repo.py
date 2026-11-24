"""ClickHouse-репозиторий: агрегатные запросы и материализация результатов.

Все метрики считаются за календарный день (UTC) из raw-таблицы cinema.movie_events
с использованием агрегатных (uniqExact, countIf, avgIf) и оконных функций.
"""
import logging
from datetime import date, timedelta

from clickhouse_driver import Client

from .config import settings

logger = logging.getLogger("aggregation-service.clickhouse")

EVENT_TYPE = "event_type"
FINISHED, STARTED, LIKED = "VIEW_FINISHED", "VIEW_STARTED", "LIKED"


class ClickHouseRepository:
    def __init__(self) -> None:
        self._client: Client | None = None

    def connect(self) -> None:
        self._client = Client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            database=settings.CLICKHOUSE_DB,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            settings={"use_numpy": False},
        )
        self._client.execute("SELECT 1")
        logger.info(
            "ClickHouse подключён: %s:%s/%s",
            settings.CLICKHOUSE_HOST, settings.CLICKHOUSE_PORT, settings.CLICKHOUSE_DB,
        )

    def _execute(self, query: str, params: dict | None = None):
        return self._client.execute(query, params or {}, types_check=True)

    # ------------------------------------------------------------------ raw
    def count_raw_events(self, day: date) -> int:
        rows = self._execute(
            "SELECT count() FROM movie_events WHERE toDate(timestamp) = %(day)s",
            {"day": day},
        )
        return int(rows[0][0])

    # -------------------------------------------------------------- metrics
    def compute_daily_overview(self, day: date) -> dict:
        """DAU, среднее время завершённого просмотра, конверсия просмотра."""
        rows = self._execute(
            """
            SELECT
                uniqExact(user_id)                                            AS dau,
                avgIf(progress_seconds, event_type = 'VIEW_FINISHED')         AS avg_view_seconds,
                countIf(event_type = 'VIEW_STARTED')                          AS views_started,
                countIf(event_type = 'VIEW_FINISHED')                         AS views_finished,
                countIf(event_type = 'VIEW_FINISHED')
                    / nullIf(countIf(event_type = 'VIEW_STARTED'), 0)         AS view_conversion
            FROM movie_events
            WHERE toDate(timestamp) = %(day)s
            """,
            {"day": day},
        )
        dau, avg_view, started, finished, conversion = rows[0]
        return {
            "dau": int(dau),
            "avg_view_seconds": float(avg_view or 0.0),
            "views_started": int(started),
            "views_finished": int(finished),
            "view_conversion": float(conversion or 0.0),
        }

    def compute_top_movies(self, day: date, limit: int = 10) -> list[dict]:
        """Топ фильмов по количеству начатых просмотров."""
        rows = self._execute(
            """
            SELECT
                movie_id,
                countIf(event_type = 'VIEW_STARTED')                  AS views_started,
                countIf(event_type = 'VIEW_FINISHED')                 AS views_finished,
                countIf(event_type = 'LIKED')                         AS likes,
                avgIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_progress_seconds
            FROM movie_events
            WHERE toDate(timestamp) = %(day)s AND movie_id != ''
            GROUP BY movie_id
            ORDER BY views_started DESC
            LIMIT %(limit)s
            """,
            {"day": day, "limit": limit},
        )
        return [
            {
                "movie_id": r[0],
                "views_started": int(r[1]),
                "views_finished": int(r[2]),
                "likes": int(r[3]),
                "avg_progress_seconds": float(r[4] or 0.0),
            }
            for r in rows
        ]

    def compute_retention(self, day: date) -> dict:
        """Retention D1/D7 для когорты пользователей с первым просмотром в `day`.

        Оконная функция min(...) OVER (PARTITION BY user_id) определяет дату
        первой активности каждого пользователя.
        """
        rows = self._execute(
            """
            SELECT
                uniqExact(user_id)                                          AS cohort_size,
                uniqExactIf(user_id, active_date = cohort_date + 1)         AS retained_d1,
                uniqExactIf(user_id, active_date = cohort_date + 7)         AS retained_d7
            FROM
            (
                SELECT
                    user_id,
                    toDate(timestamp)                                       AS active_date,
                    min(toDate(timestamp)) OVER (PARTITION BY user_id)      AS cohort_date
                FROM movie_events
                WHERE toDate(timestamp) <= %(day_plus_7)s
            )
            WHERE cohort_date = %(day)s
            """,
            {"day": day, "day_plus_7": day + timedelta(days=7)},
        )
        cohort, d1, d7 = rows[0]
        cohort, d1, d7 = int(cohort), int(d1), int(d7)
        return {
            "cohort_size": cohort,
            "retained_d1_users": d1,
            "retained_d7_users": d7,
            "retention_d1": (d1 / cohort) if cohort else 0.0,
            "retention_d7": (d7 / cohort) if cohort else 0.0,
        }

    def compute_cohort_matrix(self, window_days: int, max_day: int = 7) -> list[dict]:
        """Полная когортная матрица retention (для Grafana heatmap)."""
        rows = self._execute(
            """
            WITH per_user AS
            (
                SELECT
                    user_id,
                    toDate(timestamp)                                       AS active_date,
                    min(toDate(timestamp)) OVER (PARTITION BY user_id)      AS cohort_date
                FROM movie_events
                WHERE toDate(timestamp) >= today() - %(window)s - %(max_day)s
            ),
            activity AS
            (
                SELECT
                    cohort_date,
                    dateDiff('day', cohort_date, active_date) AS day_index,
                    uniqExact(user_id) AS users
                FROM per_user
                GROUP BY cohort_date, day_index
            ),
            cohorts AS
            (
                SELECT cohort_date, users AS cohort_size
                FROM activity WHERE day_index = 0
            )
            SELECT
                a.cohort_date,
                a.day_index,
                a.users,
                round(a.users * 100.0 / c.cohort_size, 1) AS pct
            FROM activity a
            INNER JOIN cohorts c USING (cohort_date)
            WHERE a.day_index <= %(max_day)s
            ORDER BY a.cohort_date, a.day_index
            """,
            {"window": window_days, "max_day": max_day},
        )
        return [
            {
                "cohort_date": r[0],
                "day_index": int(r[1]),
                "users": int(r[2]),
                "pct": float(r[3]),
            }
            for r in rows
        ]

    # ------------------------------------------------------ materialization
    def materialize_day(self, day: date, overview: dict, top_movies: list[dict], retention: dict) -> None:
        """Материализует результаты пересчёта в таблицы ClickHouse."""
        self._execute(
            "INSERT INTO daily_overview "
            "(date, dau, avg_view_seconds, views_started, views_finished, view_conversion) VALUES",
            [[
                day,
                overview["dau"],
                overview["avg_view_seconds"],
                overview["views_started"],
                overview["views_finished"],
                overview["view_conversion"],
            ]],
        )
        if top_movies:
            self._execute(
                "INSERT INTO daily_top_movies "
                "(date, movie_id, views_started, views_finished, likes, avg_progress_seconds) VALUES",
                [[
                    day,
                    m["movie_id"],
                    m["views_started"],
                    m["views_finished"],
                    m["likes"],
                    m["avg_progress_seconds"],
                ] for m in top_movies],
            )
        self._execute(
            "INSERT INTO daily_retention (date, metric, value, cohort_size, retained_users) VALUES",
            [
                [day, "retention_d1", retention["retention_d1"], retention["cohort_size"], retention["retained_d1_users"]],
                [day, "retention_d7", retention["retention_d7"], retention["cohort_size"], retention["retained_d7_users"]],
            ],
        )

    def materialize_cohort_matrix(self, matrix: list[dict]) -> None:
        if not matrix:
            return
        self._execute(
            "INSERT INTO cohort_retention (cohort_date, day_index, users, pct) VALUES",
            [[r["cohort_date"], r["day_index"], r["users"], r["pct"]] for r in matrix],
        )
