"""Инициализация Kafka: создание топика movie-events и регистрация Avro-схемы.

Запускается однократно как init-контейнер перед стартом продюсера.
"""
import json
import os
import time
import warnings

import requests
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, RESOURCE_TOPIC

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = os.environ.get("KAFKA_TOPIC", "movie-events")
PARTITIONS = int(os.environ.get("TOPIC_PARTITIONS", "3"))
REPLICATION_FACTOR = int(os.environ.get("TOPIC_REPLICATION_FACTOR", "2"))
MIN_INSYNC_REPLICAS = os.environ.get("TOPIC_MIN_INSYNC_REPLICAS", "1")
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "/schemas/movie_event.avsc")

WAIT_TIMEOUT = int(os.environ.get("INIT_WAIT_TIMEOUT", "180"))


def wait_for_kafka(admin: AdminClient) -> None:
    deadline = time.time() + WAIT_TIMEOUT
    while time.time() < deadline:
        try:
            metadata = admin.list_topics(timeout=10)
            brokers = len(metadata.brokers)
            print(f"kafka-init: доступно брокеров: {brokers}", flush=True)
            if brokers >= 2:
                return
        except KafkaException as exc:
            print(f"kafka-init: ожидание Kafka... ({exc})", flush=True)
        time.sleep(3)
    raise RuntimeError("Kafka не поднялась за отведённое время")


def wait_for_schema_registry() -> None:
    deadline = time.time() + WAIT_TIMEOUT
    while time.time() < deadline:
        try:
            resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
            if resp.status_code == 200:
                print("kafka-init: Schema Registry доступен", flush=True)
                return
        except requests.RequestException:
            pass
        print("kafka-init: ожидание Schema Registry...", flush=True)
        time.sleep(3)
    raise RuntimeError("Schema Registry не поднялся за отведённое время")


def create_topic(admin: AdminClient) -> None:
    existing = admin.list_topics(timeout=10).topics
    if TOPIC in existing:
        print(f"kafka-init: топик '{TOPIC}' уже существует, пропуск создания", flush=True)
    else:
        topic = NewTopic(
            topic=TOPIC,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            config={
                # Оставляем ключи (события одного пользователя) как можно дольше:
                "cleanup.policy": "delete",
                "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 дней
                "min.insync.replicas": MIN_INSYNC_REPLICAS,
            },
        )
        futures = admin.create_topics([topic], request_timeout=30)
        futures[TOPIC].result()
        print(
            f"kafka-init: топик '{TOPIC}' создан: partitions={PARTITIONS}, "
            f"replication_factor={REPLICATION_FACTOR}",
            flush=True,
        )

    # Проверяем/выставляем min.insync.replicas (при создании топик уже получает
    # нужный конфиг; здесь — защита для случая, когда топик уже существовал).
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    admin.alter_configs([ConfigResource(RESOURCE_TOPIC, TOPIC, set_config={"min.insync.replicas": MIN_INSYNC_REPLICAS})])
    print(f"kafka-init: min.insync.replicas={MIN_INSYNC_REPLICAS} для '{TOPIC}'", flush=True)

    # Метаданные о новом топике распространяются с задержкой — ждём появления.
    for attempt in range(10):
        meta = admin.list_topics(timeout=10).topics.get(TOPIC)
        if meta is not None:
            for p in meta.partitions.values():
                print(f"kafka-init: партиция {p.id}: leader={p.leader}, replicas={p.replicas}", flush=True)
            return
        time.sleep(2)
    raise RuntimeError(f"топик '{TOPIC}' не появился в метаданных")


def register_schema() -> None:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:
        schema = json.load(fh)

    subject = f"{TOPIC}-value"
    # Идемпотентно: если схема уже зарегистрирована и совпадает — регистрация
    # вернёт существующую версию (благодаря проверке совместимости на стороне SR).
    resp = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
        timeout=10,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Не удалось зарегистрировать схему: {resp.status_code} {resp.text}")

    version = resp.json().get("version")
    schema_id = resp.json().get("id")
    print(f"kafka-init: схема '{subject}' зарегистрирована: id={schema_id}, version={version}", flush=True)

    compat = requests.put(
        f"{SCHEMA_REGISTRY_URL}/config/{subject}",
        json={"compatibility": "BACKWARD"},
        timeout=10,
    )
    if compat.status_code == 200:
        print(f"kafka-init: compatibility={compat.json().get('compatibility')} для '{subject}'", flush=True)

    all_versions = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions", timeout=10)
    print(f"kafka-init: версии схемы {subject}: {all_versions.json()}", flush=True)


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    wait_for_kafka(admin)
    wait_for_schema_registry()
    create_topic(admin)
    register_schema()
    print("kafka-init: инициализация завершена успешно", flush=True)


if __name__ == "__main__":
    main()
