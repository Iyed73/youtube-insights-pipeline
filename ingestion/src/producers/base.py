from __future__ import annotations

import logging
import os
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

logger = logging.getLogger(__name__)

_SCHEMAS_DIR = Path(__file__).parents[3] / "schemas"


class AvroKafkaProducer:
    def __init__(self, topic: str, schema_file: str) -> None:
        self._topic = topic

        sr_client = SchemaRegistryClient({"url": os.environ["SCHEMA_REGISTRY_URL"]})
        schema_str = (_SCHEMAS_DIR / schema_file).read_text()
        self._serializer = AvroSerializer(sr_client, schema_str)

        self._producer = Producer(
            {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}
        )

    def produce(self, key: str, value: dict) -> None:
        self._producer.produce(
            topic=self._topic,
            key=key.encode(),
            value=self._serializer(
                value, SerializationContext(self._topic, MessageField.VALUE)
            ),
            on_delivery=self._on_delivery,
        )

    def flush(self) -> None:
        self._producer.flush()

    @staticmethod
    def _on_delivery(err: Exception | None, msg: object) -> None:
        if err:
            logger.error("Delivery failed for %s: %s", getattr(msg, "topic", "?"), err)
        else:
            logger.debug(
                "Delivered to %s [partition %s]", msg.topic(), msg.partition()  # type: ignore[union-attr]
            )
