"""Генератор синтетических событий, имитирующий поведение зрителей.

Сценарии сессии:
    VIEW_STARTED -> (VIEW_PAUSED -> VIEW_RESUMED)* -> VIEW_FINISHED [-> LIKED]
плюс отдельные события SEARCHED. Прогресс просмотра монотонно растёт
в рамках сессии, timestamp событий идут в порядке сценария.
"""
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from .config import settings

logger = logging.getLogger("movie-service.generator")

# Пул фильмов: несколько «хитов» с большим весом, длинный хвост обычных фильмов.
# Длительность фильма нужна, чтобы progress_seconds был реалистичным.
MOVIES: list[tuple[str, int, int]] = [  # (movie_id, длительность_сек, вес)
    ("movie-avatar-3", 9000, 30),
    ("movie-dune-2", 9900, 25),
    ("movie-oppenheimer", 10800, 20),
    ("movie-barbie", 6900, 15),
    ("movie-john-wick-4", 10200, 15),
    ("movie-interstellar", 10140, 10),
    ("movie-inception", 8880, 10),
    ("movie-matrix", 8160, 8),
    ("movie-gladiator-2", 9300, 8),
    ("movie-furiosa", 8880, 6),
] + [(f"movie-catalog-{i:04d}", random.Random(i).randint(4200, 12000), 1) for i in range(90)]

USER_POOL = [f"user-{i:06d}" for i in range(settings.GENERATOR_USERS)]
DEVICE_POOL = ["MOBILE", "MOBILE", "MOBILE", "DESKTOP", "DESKTOP", "TV", "TABLET"]

MOVIE_IDS = [m[0] for m in MOVIES]
MOVIE_DUR = {m[0]: m[1] for m in MOVIES}
MOVIE_W = [m[2] for m in MOVIES]


class EventGenerator:
    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)
        self._users = USER_POOL
        self._movies = MOVIE_IDS
        self._weights = MOVIE_W

    def _pick_movie(self) -> str:
        return self.rng.choices(self._movies, weights=self._weights, k=1)[0]

    def _session_events(self, user_id: str, device: str, start: datetime) -> list[dict]:
        """Собирает реалистичную последовательность событий одной сессии."""
        movie_id = self._pick_movie()
        movie_len = MOVIE_DUR[movie_id]
        session_id = str(uuid.uuid4())
        # Досматриваем в среднем 70% фильма; часть сессий бросают на середине.
        watched_ratio = min(1.0, self.rng.betavariate(3.0, 1.5))
        watch_span = int(movie_len * watched_ratio)
        if watch_span < 60:
            watch_span = 60

        events: list[dict] = []
        ts = start
        progress = self.rng.randint(0, 120)  # небольшой «прогресс» при старте — досмотр с места

        def push(event_type: str, progress_seconds: int) -> None:
            events.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "movie_id": movie_id,
                    "event_type": event_type,
                    "timestamp": int(ts.timestamp() * 1000),
                    "device_type": device,
                    "session_id": session_id,
                    "progress_seconds": progress_seconds,
                }
            )

        push("VIEW_STARTED", progress)
        session_end = ts + timedelta(seconds=watch_span)
        # Случайные паузы/возобновления внутри сессии.
        cursor = ts
        while cursor < session_end:
            step = self.rng.randint(120, 900)
            cursor = min(cursor + timedelta(seconds=step), session_end)
            progress = min(movie_len, progress + step)
            if self.rng.random() < 0.18:
                push("VIEW_PAUSED", progress)
                pause = self.rng.randint(30, 900)
                cursor = cursor + timedelta(seconds=pause)
                progress = min(movie_len, progress + self.rng.randint(0, 30))
                if cursor < session_end:
                    push("VIEW_RESUMED", progress)
        # ~22% сессий бросают без досмотра -> конверсия VIEW_FINISHED/VIEW_STARTED < 1.
        if self.rng.random() < 0.78:
            push("VIEW_FINISHED", min(movie_len, progress))
            if self.rng.random() < 0.25:
                push("LIKED", min(movie_len, progress))
        return events

    def _search_event(self, user_id: str, device: str, ts: datetime) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "movie_id": "",  # поиск не привязан к конкретному фильму
            "event_type": "SEARCHED",
            "timestamp": int(ts.timestamp() * 1000),
            "device_type": device,
            "session_id": str(uuid.uuid4()),
            "progress_seconds": 0,
        }

    def generate_batch(self, size: int, now: datetime | None = None) -> list[dict]:
        """Генерирует пакет событий, привязанных к времени `now`.

        ~8% сессий — новые пользователи (растущие «сегодняшние» когорты),
        остальные — возвращающиеся пользователи из пула.
        """
        now = now or datetime.now(timezone.utc)
        batch: list[dict] = []
        while len(batch) < size:
            fresh_user = self.rng.random() < 0.08
            user_id = (
                f"user-rt-{self.rng.randint(0, 10**9):010d}" if fresh_user
                else self.rng.choice(self._users)
            )
            device = self.rng.choice(DEVICE_POOL)
            if self.rng.random() < 0.12:
                ts = now - timedelta(seconds=self.rng.randint(0, 1800))
                batch.append(self._search_event(user_id, device, ts))
            else:
                start = now - timedelta(
                    seconds=self.rng.randint(0, 6 * 3600)  # сессии за последние 6 часов
                )
                batch.extend(self._session_events(user_id, device, start))
        return batch[:size]

    def generate_history(self, days: int, events_per_day: int) -> list[dict]:
        """Исторический бэкфилл с моделью жизненного цикла пользователя.

        Каждый пользователь «регистрируется» в некоторый день окна (когорта)
        и далее проявляет активность с вероятностью ~75% в день. Когорты
        растут от старых к новым (прирост аудитории). Первый день жизни
        пользователя гарантированно содержит его первое событие.
        """
        now = datetime.now(timezone.utc)
        all_events: list[dict] = []
        if days < 3:
            days = 3
        # Веса регистрации: линейный рост аудитории (свежие когорты крупнее).
        reg_weights = [max(1, i) for i in range(1, days)]

        for user_id in self._users:
            registered_days_ago = self.rng.choices(range(1, days), weights=reg_weights, k=1)[0]
            device = self.rng.choice(DEVICE_POOL)
            for day_offset in range(registered_days_ago, 0, -1):
                if self.rng.random() > 0.75:
                    continue  # неактивен в этот день
                day = now - timedelta(days=day_offset)
                sessions_today = 1 + (1 if self.rng.random() < 0.35 else 0)
                for _ in range(sessions_today):
                    start = day.replace(
                        hour=self.rng.randint(8, 22),
                        minute=self.rng.randint(0, 59),
                        second=self.rng.randint(0, 59),
                    )
                    all_events.extend(self._session_events(user_id, device, start))
        all_events.sort(key=lambda e: e["timestamp"])
        logger.info("генератор: история за %d дн., всего событий: %d", days, len(all_events))
        return all_events


def run_generator_loop(producer, schema: EventGenerator) -> None:  # noqa: ANN001
    """Фоновый режим: бэкфилл истории (только в пустой топик) + realtime-генерация.

    Бэкфилл выполняется однократно — при первом запуске системы. Если топик уже
    содержит сообщения (перезапуск продюсера), повторный бэкфилл пропускается,
    чтобы не дублировать историю в ClickHouse.
    """
    from .config import settings as cfg  # noqa: PLC0415
    from .kafka_producer import topic_end_offset  # noqa: PLC0415

    if cfg.GENERATOR_BACKFILL:
        if topic_end_offset(cfg.KAFKA_TOPIC) == 0:
            events_per_day = max(1500, cfg.GENERATOR_BATCH_SIZE * 12)
            history = schema.generate_history(cfg.GENERATOR_HISTORY_DAYS, events_per_day)
            for i in range(0, len(history), 2000):
                producer.send_batch(history[i : i + 2000])
            logger.info("генератор: бэкфилл отправлен (%d событий)", len(history))
        else:
            logger.info("генератор: топик не пуст, бэкфилл пропущен (история уже опубликована)")

    while True:
        try:
            batch = schema.generate_batch(settings.GENERATOR_BATCH_SIZE)
            producer.send_batch(batch)
            logger.info("генератор: отправлено %d событий", len(batch))
        except Exception:  # noqa: BLE001
            logger.exception("генератор: ошибка отправки пакета")
        time.sleep(settings.GENERATOR_INTERVAL_SECONDS)
