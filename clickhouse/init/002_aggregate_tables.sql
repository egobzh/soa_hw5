-- 002: таблицы для материализации бизнес-агрегатов (заполняет Aggregation Service)
-- ReplacingMergeTree: повторный пересчёт за ту же дату заменяет строку
-- (фактическое удаление дублей происходит при фоновых мерджах; для чтения
-- используем SELECT ... FINAL либо GROUP BY + any()/max()).

CREATE TABLE IF NOT EXISTS cinema.daily_overview
(
    date Date,
    dau UInt64,
    avg_view_seconds Float64,
    views_started UInt64,
    views_finished UInt64,
    view_conversion Float64,   -- views_finished / views_started
    raw_events UInt64,
    calculated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY date;

CREATE TABLE IF NOT EXISTS cinema.daily_top_movies
(
    date Date,
    movie_id String,
    views_started UInt64,
    views_finished UInt64,
    likes UInt64,
    avg_progress_seconds Float64,
    calculated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (date, movie_id);

CREATE TABLE IF NOT EXISTS cinema.daily_retention
(
    date Date,                -- дата когорты (день первого просмотра)
    metric LowCardinality(String),  -- 'retention_d1' | 'retention_d7'
    value Float64,            -- доля 0..1
    cohort_size UInt64,
    retained_users UInt64,
    calculated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (date, metric);

-- Когортная матрица retention: по вертикали когорта, по горизонтали day_index (0..7).
CREATE TABLE IF NOT EXISTS cinema.cohort_retention
(
    cohort_date Date,   -- день первой активности пользователей
    day_index UInt8,    -- Day 0, Day 1, ..., Day 7
    users UInt64,       -- вернувшиеся пользователи
    pct Float64,        -- % от размера когорты (Day 0 = 100)
    calculated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (cohort_date, day_index);
