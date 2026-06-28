import pytest
from pydantic import ValidationError

from ananke_hermes.streaming.settings import KafkaSettings


def test_kafka_settings_requires_username_and_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KAFKA_USERNAME", raising=False)
    monkeypatch.delenv("KAFKA_PASSWORD", raising=False)

    with pytest.raises(ValidationError):
        KafkaSettings(_env_file=None)


def test_kafka_settings_loads_with_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAFKA_USERNAME", "superuser")
    monkeypatch.setenv("KAFKA_PASSWORD", "secret")

    settings = KafkaSettings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "localhost:19092"
    assert settings.kafka_username == "superuser"
    assert settings.kafka_password == "secret"
    assert settings.kafka_security_protocol == "SASL_PLAINTEXT"
    assert settings.producer_config["sasl.password"] == "secret"
