from __future__ import annotations

import json
import logging
import socket
from datetime import datetime, timezone

from confluent_kafka import Producer

from ananke_hermes.streaming.settings import KafkaSettings

logger = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, settings: KafkaSettings | None = None):
        settings = settings or KafkaSettings()
        config = {
            **settings.producer_config,
            "client.id": socket.gethostname(),
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "compression.type": "lz4",
        }
        self._producer = Producer(config)

    def publish(self, topic: str, event: dict, key: str | None = None) -> None:
        payload = {**event, "timestamp": datetime.now(timezone.utc).isoformat()}

        def delivery_callback(err, msg):
            if err:
                logger.error(f"Delivery failed for {topic}: {err}")
            else:
                logger.info(
                    f"Delivered to {msg.topic()} [{msg.partition()}]@{msg.offset()}"
                )

        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(payload, default=str).encode("utf-8"),
            callback=delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)
