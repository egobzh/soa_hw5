"""Kafka-продюсер: ключ партиционирования, acks, retry с exponential backoff, логирование."""
import logging
import time

from confluent_kafka import KafkaError, Producer, TopicPartition

from .config import settings
from .event_schema import EventSchema

logger = logging.getLogger("movie-service.producer")


class MovieEventProducer:
    def __init__(self, schema: EventSchema):
        self.schema = schema
        self._producer = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                # Подтверждение записи: ждём подтверждения от всех in-sync реплик.
                "acks": settings.PRODUCER_ACKS,
                # Повторы на уровне librdkafka (временные ошибки брокера).
                "retries": settings.PRODUCER_RETRIES,
                "retry.backoff.ms": 200,
                "retry.backoff.max.ms": 5000,
                "enable.idempotence": True,
                "compression.type": "lz4",
                "queue.buffering.max.messages": 100000,
                "linger.ms": 20,
            }
        )

    def _delivery_report(self, err: KafkaError | None, msg) -> None:
        if err is not None:
            logger.error(
                "ОШИБКА доставки: topic=%s partition=%s offset=%s error=%s",
                msg.topic(), msg.partition(), msg.offset(), err,
            )
        else:
            logger.info(
                "event delivered: key(user_id)=%s partition=%s offset=%s",
                msg.key(), msg.partition(), msg.offset(),
            )

    def send(self, event: dict) -> None:
        """Публикует одно событие. Ключ — user_id (порядок событий пользователя).

        При ошибках публикации делает повторные попытки с exponential backoff.
        """
        payload = EventSchema.to_kafka_json(event)
        key = event["user_id"]  # ключ партиционирования

        attempt = 0
        backoff_ms = settings.PRODUCER_BACKOFF_BASE_MS
        while True:
            try:
                self._producer.produce(
                    settings.KAFKA_TOPIC,
                    value=payload,
                    key=key.encode("utf-8"),
                    headers=[("schema_id", str(self.schema.schema_id).encode("utf-8"))],
                    on_delivery=self._delivery_report,
                )
                self._producer.poll(0)
                logger.info(
                    "event published: event_id=%s event_type=%s user_id=%s timestamp=%s",
                    event["event_id"], event["event_type"], event["user_id"], event["timestamp"],
                )
                return
            except BufferError:
                # Локальная очередь переполнена — ждём и повторяем (backoff).
                attempt += 1
                if attempt > settings.PRODUCER_MAX_RETRIES:
                    raise
                wait = backoff_ms / 1000.0
                logger.warning(
                    "очередь продюсера переполнена, повтор через %.2fs (попытка %d)", wait, attempt
                )
                self._producer.poll(wait)
                backoff_ms *= 2

    def flush(self, timeout: float = 30.0) -> None:
        self._producer.flush(timeout)

    def send_batch(self, events: list[dict]) -> None:
        for event in events:
            self.send(event)
        self.flush()


def wait_kafka_available(timeout: float = 120.0) -> None:
    """Ждём, пока кластер Kafka станет доступен (для healthcheck-готовности)."""
    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            metadata = producer.list_topics(timeout=5)
            if len(metadata.brokers) >= 1:
                logger.info("Kafka доступна: брокеров=%d", len(metadata.brokers))
                return
        except Exception as exc:  # noqa: BLE001
            logger.warning("ожидание Kafka... %s", exc)
        time.sleep(2)
    raise RuntimeError("Kafka недоступна")


def topic_end_offset(topic: str, timeout: float = 15.0) -> int:
    """Суммарный конечный offset топика (0 = топик пуст)."""
    from confluent_kafka.admin import AdminClient, OffsetSpec  # noqa: PLC0415

    admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(topic, timeout=timeout)
    tps = [TopicPartition(topic, p.id) for p in metadata.topics[topic].partitions.values()]
    if not tps:
        return 0
    futures = admin.list_offsets(
        {tp: OffsetSpec.latest() for tp in tps}, request_timeout=timeout
    )
    total = 0
    for future in futures.values():
        total += max(future.result().offset, 0)
    return total
