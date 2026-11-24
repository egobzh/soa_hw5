-- PostgreSQL: хранилище готовых метрик (читают дашборды, экспорт в S3 и др. сервисы).
-- Идемпотентность: повторный пересчёт обновляет строку благодаря
-- ON CONFLICT (metric_date, metric_name, dimensions) DO UPDATE.

CREATE TABLE IF NOT EXISTS metrics (
    metric_date   DATE NOT NULL,
    metric_name   TEXT NOT NULL,
    dimensions    JSONB NOT NULL DEFAULT '{}'::jsonb,
    value         DOUBLE PRECISION NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT metrics_pk PRIMARY KEY (metric_date, metric_name, dimensions)
);

COMMENT ON TABLE metrics IS 'Готовые бизнес-метрики онлайн-кинотеатра (DAU, retention, конверсия, топ фильмов)';

-- Удобные индексы для чтения метрик за период и для экспорта.
CREATE INDEX IF NOT EXISTS metrics_date_idx ON metrics (metric_date);
CREATE INDEX IF NOT EXISTS metrics_name_idx ON metrics (metric_name);
