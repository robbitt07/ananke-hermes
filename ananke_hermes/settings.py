from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_username: str = Field(..., min_length=1, description="Username")
    kafka_password: str = Field(..., min_length=1, description="Password")
    kafka_security_protocol: str = "SASL_PLAINTEXT"
    kafka_sasl_mechanism: str = "SCRAM-SHA-256"
    schema_registry_url: str = "http://localhost:18081"

    batch_size: int = 50
    producer_flush_every: int = 10

    topic_pricing_ohlcv_1min: str = "hermes.pricing.ohlcv.1min"
    topic_pricing_ohlcv_1min_dlq: str = "hermes.pricing.ohlcv.1min.dlq"

    consumer_session_timeout_ms: int = 45000
    consumer_heartbeat_interval_ms: int = 15000
    consumer_max_poll_interval_ms: int = 900000
    consumer_fetch_min_bytes: int = 1048576
    consumer_max_partition_fetch_bytes: int = 4194304
    consumer_group_instance_id: str = ""

    consumer_dlq_enabled: bool = True
    consumer_max_retries: int = 3

    @property
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": self.kafka_security_protocol,
            "sasl.mechanism": self.kafka_sasl_mechanism,
            "sasl.username": self.kafka_username,
            "sasl.password": self.kafka_password,
        }

    @property
    def dlq_topic_for(self) -> dict[str, str]:
        return {
            self.topic_pricing_ohlcv_1min: self.topic_pricing_ohlcv_1min_dlq,
        }

    @property
    def consumer_config(self) -> dict:
        config = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": self.kafka_security_protocol,
            "sasl.mechanism": self.kafka_sasl_mechanism,
            "sasl.username": self.kafka_username,
            "sasl.password": self.kafka_password,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": self.consumer_max_poll_interval_ms,
            "session.timeout.ms": self.consumer_session_timeout_ms,
            "heartbeat.interval.ms": self.consumer_heartbeat_interval_ms,
            "fetch.min.bytes": self.consumer_fetch_min_bytes,
            "max.partition.fetch.bytes": self.consumer_max_partition_fetch_bytes,
        }
        if self.consumer_group_instance_id:
            config["group.instance.id"] = self.consumer_group_instance_id
        return config


@lru_cache
def get_settings() -> KafkaSettings:
    return KafkaSettings()
