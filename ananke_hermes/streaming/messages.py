from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_pricing_batch(
    integration: str,
    period_frequency: str,
    ingress_mode: str,
    payloads: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a Hermes pricing batch envelope for Kafka publishing."""
    return {
        "period_frequency": period_frequency,
        "ingress_mode": ingress_mode,
        "integration": integration,
        "payloads": payloads,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


def pricing_batch_key(integration: str, bar_minute: datetime) -> str:
    """Partition key for a pricing batch message."""
    return f"{integration}:{bar_minute.isoformat()}"
