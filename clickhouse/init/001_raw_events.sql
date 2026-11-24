-- 001: приём событий из Kafka и постоянное хранение (raw-слой)
CREATE DATABASE IF NOT EXISTS cinema;

-- Таблица-движок Kafka Engine: читает топик movie-events в формате JSONEachRow.
-- События публикуются продюсером в JSON (валидация по Avro-схеме с enum-полями),
-- enum'ы передаются строками и при перекладке в MergeTree превращаются в Enum8.
CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp String,  -- 'YYYY-MM-DD HH:MM:SS' UTC
    device_type String,
    session_id String,
    progress_seconds UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_thread_per_consumer = 1;

-- Постоянное хранение событий.
-- Партиционирование по месяцу — типовой компромисс для аналитики;
-- сортировка (event_date, user_id, timestamp):
--   - фильтр по дате отсекает куски по minmax-индексу event_date;
--   - uniqExact(user_id) за день читает плотные диапазоны (user_id в ORDER BY);
--   - порядок timestamp внутри пользователя сохраняет хронологию сессий.
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id String,
    user_id String,
    movie_id String,
    event_type Enum8(
        'VIEW_STARTED' = 1,
        'VIEW_FINISHED' = 2,
        'VIEW_PAUSED' = 3,
        'VIEW_RESUMED' = 4,
        'LIKED' = 5,
        'SEARCHED' = 6
    ),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds UInt32,
    event_date Date MATERIALIZED toDate(timestamp)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, timestamp);

-- Автоматическая перекладка Kafka -> MergeTree.
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv TO cinema.movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    toDateTime64(timestamp, 3, 'UTC') AS timestamp,
    device_type,
    session_id,
    progress_seconds
FROM cinema.movie_events_kafka;
