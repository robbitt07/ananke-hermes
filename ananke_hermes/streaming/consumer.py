from __future__ import annotations

import json
import logging
import signal
import time
from collections import defaultdict
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from ananke_hermes.streaming.producer import EventProducer
from ananke_hermes.streaming.settings import KafkaSettings

logger = logging.getLogger(__name__)


class EventConsumer:

    def __init__(
        self,
        group_id: str,
        topics: list[str],
        settings: KafkaSettings | None = None,
        on_message: Callable[[dict, object], None] | None = None,
    ):
        settings = settings or KafkaSettings()
        self._settings = settings
        config = {**settings.consumer_config, "group.id": group_id}
        self._consumer = Consumer(config)
        self._consumer.subscribe(topics)
        self._running = True
        self._on_message = on_message or self._default_handler
        self._batch_size = settings.batch_size
        self._dlq_producer: EventProducer | None = None
        if settings.consumer_dlq_enabled:
            self._dlq_producer = EventProducer(settings)
        self._retry_counts: dict[tuple[str, int, int], int] = defaultdict(int)

    @staticmethod
    def _default_handler(event: dict, msg) -> None:
        integration: str = event.get("integration") or "unknown"
        logger.info(
            f"Processed {integration} from {msg.topic()} "
            f"[{msg.partition()}]@{msg.offset()}"
        )

    def shutdown(self, *_args) -> None:
        logger.info("Shutting down consumer...")
        self._running = False

    def consume_batch(self, batch_size: int | None = None, timeout: float = 1.0) -> list:
        batch_size = batch_size or self._batch_size
        messages = self._consumer.consume(num_messages=batch_size, timeout=timeout)
        if not messages:
            return []

        valid = []
        for msg in messages:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue
            valid.append(msg)
        return valid

    def _commit_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        if not offsets:
            return
        partitions = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in offsets.items()
        ]
        self._consumer.commit(offsets=partitions, asynchronous=False)

    def _send_to_dlq(self, event: dict | None, msg, error: Exception) -> None:
        if not self._dlq_producer:
            return
        dlq_topic = self._settings.dlq_topic_for.get(msg.topic())
        if not dlq_topic:
            logger.error(f"No DLQ topic configured for {msg.topic()}")
            return
        payload = {
            "original_topic": msg.topic(),
            "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "error": str(error),
            "event": event,
        }
        key = f"{msg.topic()}-{msg.partition()}-{msg.offset()}"
        self._dlq_producer.publish(dlq_topic, payload, key=key)
        self._dlq_producer.flush()
        logger.warning(
            f"Sent to DLQ {dlq_topic} from {msg.topic()} [{msg.partition()}]@{msg.offset()}",
        )

    def _process_batch(self, msgs: list) -> bool:
        """Process a batch; return False if processing should stop."""
        commit_offsets: dict[tuple[str, int], int] = {}
        stop_batch = False

        for msg in msgs:
            if not self._running:
                break

            key = (msg.topic(), msg.partition(), msg.offset())
            event: dict | None = None
            try:
                event = json.loads(msg.value().decode("utf-8"))
                self._on_message(event, msg)
                commit_offsets[(msg.topic(), msg.partition())] = msg.offset() + 1
                self._retry_counts.pop(key, None)
            except Exception as exc:
                self._retry_counts[key] += 1
                retries = self._retry_counts[key]
                logger.exception(
                    f"Event processing failed (attempt {retries}/{self._settings.consumer_max_retries})",
                )

                if (
                    self._settings.consumer_dlq_enabled
                    and retries >= self._settings.consumer_max_retries
                ):
                    self._send_to_dlq(event, msg, exc)
                    commit_offsets[(msg.topic(), msg.partition())] = msg.offset() + 1
                    self._retry_counts.pop(key, None)
                    continue

                self._commit_offsets(commit_offsets)
                stop_batch = True
                break

        self._commit_offsets(commit_offsets)
        return not stop_batch

    def run(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        try:
            while self._running:
                try:
                    msgs = self.consume_batch()
                    if not msgs:
                        continue

                    if not self._process_batch(msgs):
                        time.sleep(1)

                except KafkaException:
                    logger.exception("Kafka batch error")
                    time.sleep(5)
                except Exception:
                    logger.exception("Batch error")
                    time.sleep(5)
        finally:
            if self._dlq_producer:
                self._dlq_producer.flush()
            self._consumer.close()
            logger.info("Consumer closed")
