# ananke-hermes

Hermes SDK for the Ananke Trading platform: Redpanda/Kafka streaming helpers
and shared domain models for pricing data.

## Package structure

```
ananke_hermes/
├── streaming/   # Kafka producer, consumer, settings, message envelopes
└── models/      # Pydantic domain models (e.g. HermesBar)
```

Root imports remain supported for backward compatibility; prefer explicit
subpackage imports in new code.

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

### Streaming

```python
from ananke_hermes.streaming import (
    EventConsumer,
    EventProducer,
    KafkaSettings,
    build_pricing_batch,
    get_settings,
    pricing_batch_key,
)
```

### Models

```python
from ananke_hermes.models import HermesBar
# or
from ananke_hermes import HermesBar
```

#### HermesBar fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `integration` | `str` | yes | Data source integration name |
| `ticker` | `str` | yes | Instrument ticker |
| `security_type` | `str` | yes | e.g. `crypto`, `equity` |
| `bar_type` | `str` | yes | e.g. `trade` |
| `data_feed` | `str` | yes | e.g. `stream_bar` |
| `dt` | `datetime` | yes | Bar timestamp (naive values coerced to UTC) |
| `open` | `float` | yes | |
| `high` | `float` | yes | |
| `low` | `float` | yes | |
| `close` | `float` | yes | |
| `volume` | `float \| None` | no | |
| `bid_ask_spread` | `float \| None` | no | |
| `open_int` | `float \| None` | no | Open interest |
| `updated_on` | `datetime` | no | Defaults to UTC now |

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
