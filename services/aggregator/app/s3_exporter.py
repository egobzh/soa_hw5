"""Ежедневный экспорт агрегатов из PostgreSQL в S3-совместимое хранилище (MinIO).

Формат файла — CSV, путь в бакете:
    s3://movie-analytics/daily/YYYY-MM-DD/aggregates.csv
Повторный экспорт за ту же дату перезаписывает файл (put_object по тому же ключу).
"""
import csv
import io
import logging
import time
from datetime import date, datetime

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .config import settings

logger = logging.getLogger("aggregation-service.s3")


def _strip_expect_header(request, **kwargs):  # noqa: ANN001, ANN003, ANN202
    """before-send хук: убирает 'Expect: 100-continue' из запроса.

    S3-эмуляторы на Tomcat 11 отвечают на 100-continue строкой 'HTTP/1.1 100 '
    с пустой reason-фразой, что ломает разбор ответа в urllib3. Без заголовка
    сервер сразу возвращает финальный статус.
    """
    if "Expect" in request.headers:
        del request.headers["Expect"]
    return None  # None -> отправлять штатным HTTP-клиентом boto3


class S3Exporter:
    def __init__(self) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.S3_ENDPOINT,
            aws_access_key_id=settings.S3_ACCESS_KEY,
            aws_secret_access_key=settings.S3_SECRET_KEY,
            region_name=settings.S3_REGION,
        )
        self._client.meta.events.register("before-send.s3.*", _strip_expect_header)

    def ensure_bucket(self) -> None:
        for attempt in range(settings.S3_MAX_RETRIES):
            try:
                self._client.head_bucket(Bucket=settings.S3_BUCKET)
                logger.info("S3: бакет '%s' существует", settings.S3_BUCKET)
                return
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code in ("404", "NoSuchBucket", "NotFound"):
                    self._client.create_bucket(Bucket=settings.S3_BUCKET)
                    logger.info("S3: создан бакет '%s'", settings.S3_BUCKET)
                    return
                self._sleep_and_log(attempt, exc)
        raise RuntimeError(f"не удалось проверить/создать бакет {settings.S3_BUCKET}")

    def _sleep_and_log(self, attempt: int, exc: Exception) -> None:
        wait = 2 ** attempt
        logger.error(
            "S3 недоступен (%s), повтор через %ds (попытка %d/%d)",
            exc, wait, attempt + 1, settings.S3_MAX_RETRIES,
        )
        if attempt == settings.S3_MAX_RETRIES - 1:
            raise
        time.sleep(wait)

    def export_day(self, day: date, metrics: list[dict]) -> str:
        """Формирует CSV из метрик за день и складывает в бакет. Возвращает ключ."""
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["metric_date", "metric_name", "dimensions", "value", "calculated_at"])
        for m in metrics:
            writer.writerow([
                m["metric_date"].isoformat() if isinstance(m["metric_date"], date) else m["metric_date"],
                m["metric_name"],
                m["dimensions"],
                m["value"],
                m["calculated_at"].isoformat()
                if isinstance(m["calculated_at"], datetime)
                else m["calculated_at"],
            ])
        body = buf.getvalue().encode("utf-8")

        key = f"daily/{day.isoformat()}/aggregates.csv"
        last_exc: Exception | None = None
        for attempt in range(settings.S3_MAX_RETRIES):
            try:
                self._client.put_object(
                    Bucket=settings.S3_BUCKET,
                    Key=key,
                    Body=body,
                    ContentType="text/csv",
                )
                logger.info(
                    "S3: экспортировано %d метрик за %s -> s3://%s/%s (%d байт)",
                    len(metrics), day, settings.S3_BUCKET, key, len(body),
                )
                return key
            except (BotoCoreError, ClientError) as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.error(
                    "S3: ошибка экспорта (%s), повтор через %ds (попытка %d/%d)",
                    exc, wait, attempt + 1, settings.S3_MAX_RETRIES,
                )
                time.sleep(wait)
        raise RuntimeError(f"экспорт в S3 не удался за {settings.S3_MAX_RETRIES} попыток") from last_exc
