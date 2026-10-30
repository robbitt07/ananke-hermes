# ananke-hermes TODO

## Topic configuration refactor

Today, topic names live as individual fields on `KafkaSettings`
(`topic_pricing_ohlcv_1min`, `topic_pricing_ohlcv_1min_dlq`, etc.). That
works for Phase 1 but will not scale as we add more domains.

Planned topic surface area includes:

- OHLCV pricing across three frequencies (`1day`, `5min`, `1min`)
- Pricing quotes
- Security events
- Orders
- Transactions
- Strategy events
- DLQ topics for each where applicable

### Recommended split of responsibilities

| Layer | Responsibility | Location |
|-------|----------------|----------|
| **Topic catalog** | Canonical names, naming convention, DLQ derivation | `ananke_hermes/streaming/topics.py` (SDK) |
| **Infra provisioning** | Partitions, retention, create/alter | `hermes/scripts/create_topics.sh` (or YAML it reads) |
| **Connection settings** | Broker, auth, consumer tuning only | `KafkaSettings` |
| **Service wiring** | Which topic(s) this service uses | Each producer/consumer passes explicit topic(s) |

### Naming convention

```
hermes.{domain}.{entity}[.{granularity}][.dlq]
```

Examples:

- `hermes.pricing.ohlcv.1min` / `hermes.pricing.ohlcv.1min.dlq`
- `hermes.pricing.ohlcv.5min` / `hermes.pricing.ohlcv.5min.dlq`
- `hermes.pricing.ohlcv.1day` / `hermes.pricing.ohlcv.1day.dlq`
- `hermes.pricing.quotes`
- `hermes.security.events`
- `hermes.orders.events`
- `hermes.transactions.events`
- `hermes.strategy.events`

### Proposed topic catalog pattern

Add `ananke_hermes/streaming/topics.py`:

```python
from enum import StrEnum

class HermesTopic(StrEnum):
    PRICING_OHLCV_1MIN = "hermes.pricing.ohlcv.1min"
    PRICING_OHLCV_5MIN = "hermes.pricing.ohlcv.5min"
    PRICING_OHLCV_1DAY = "hermes.pricing.ohlcv.1day"
    PRICING_QUOTES = "hermes.pricing.quotes"
    SECURITY_EVENTS = "hermes.security.events"
    # ...

    @property
    def dlq(self) -> str:
        return f"{self.value}.dlq"
```

Services should import from the catalog instead of settings:

```python
from ananke_hermes.streaming import EventConsumer, get_settings
from ananke_hermes.streaming.topics import HermesTopic

consumer = EventConsumer(
    group_id="hermes-core-pricing-group",
    topics=[HermesTopic.PRICING_OHLCV_1MIN],
    settings=get_settings(),
    on_message=pricing_handler,
)
```

DLQ routing in `EventConsumer` should use convention (`{topic}.dlq`) rather
than a hand-maintained map on `KafkaSettings`.

### What stays in `KafkaSettings`

Connection and client behavior only:

- bootstrap servers, SASL credentials, security protocol
- batch size, flush intervals, consumer session/poll tuning
- DLQ retry policy (`consumer_max_retries`, etc.)

### Environment overrides

Most deployments use the same topic names on different clusters. If a prefix
is needed (e.g. `dev.hermes.pricing...`), add a single optional setting:

```python
kafka_topic_prefix: str = ""  # e.g. "dev."
```

Apply the prefix in the topic catalog helper, not as one field per topic.

### Infra alignment

Keep `hermes/scripts/create_topics.sh` as the provisioning entry point, but
align it with the SDK catalog. Options:

1. Shared YAML (`hermes/topics.yaml`) used by the script and optionally the SDK
2. Generate the shell topic list from Python
   (`python -m ananke_hermes.streaming.topics --list`)

This avoids SDK topic names and Redpanda reality diverging.

### Migration steps

1. Add `streaming/topics.py` with `HermesTopic` and DLQ convention
2. Replace `settings.topic_pricing_ohlcv_1min` call sites with
   `HermesTopic.PRICING_OHLCV_1MIN`
3. Remove topic fields from `KafkaSettings`; keep connection/tuning only
4. Extend the catalog with partition/retention metadata when topic creation
   is automated
