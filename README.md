# ananke-hermes

Hermes Redpanda/Kafka streaming SDK for producers and consumers in the Ananke
Trading platform.

## Install

From GitHub (pinned release):

```bash
pip install "ananke-hermes @ git+https://github.com/robbitt07/ananke-hermes.git@v0.1.0"
```

Local editable install (monorepo development):

```bash
cd hermes/ananke-hermes
pip install -e ".[test]"
```

## Usage

```python
from ananke_hermes import (
    EventConsumer,
    EventProducer,
    KafkaSettings,
    build_pricing_batch,
    get_settings,
    pricing_batch_key,
)
```

## Configuration

Environment variables are loaded via `KafkaSettings` (pydantic-settings).

**Required** (no defaults — must be set via environment or `.env`):

| Variable | Notes |
|----------|-------|
| `KAFKA_USERNAME` | SASL/SCRAM username |
| `KAFKA_PASSWORD` | SASL/SCRAM password |

Common optional settings (defaults shown):

| Variable | Default | Notes |
|----------|---------|-------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Docker services use `redpanda:9092` |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` | |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-256` | |

Other settings (topics, batch size, consumer tuning) also retain SDK defaults.

## Topics

| Topic | Purpose |
|-------|---------|
| `hermes.pricing.ohlcv.1min` | Live 1min pricing bar batches |
| `hermes.pricing.ohlcv.1min.dlq` | Dead letter queue |

## Versioning

Tag releases as `v0.1.x` and pin dependent services to explicit git tags.
