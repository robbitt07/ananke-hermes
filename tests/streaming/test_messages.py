from datetime import datetime, timezone

from ananke_hermes.streaming.messages import build_pricing_batch, pricing_batch_key


def test_build_pricing_batch() -> None:
    bar_dt = datetime(2026, 6, 28, 12, 0, tzinfo=timezone.utc)
    payloads = [{"ticker": "btc.usd", "dt": bar_dt.isoformat()}]
    envelope = build_pricing_batch(
        integration="cryptocompare",
        period_frequency="1min",
        ingress_mode="live",
        payloads=payloads,
    )
    assert envelope["integration"] == "cryptocompare"
    assert envelope["period_frequency"] == "1min"
    assert envelope["ingress_mode"] == "live"
    assert envelope["payloads"] == payloads
    assert envelope["published_at"]


def test_pricing_batch_key() -> None:
    bar_dt = datetime(2026, 6, 28, 12, 0, tzinfo=timezone.utc)
    key = pricing_batch_key("cryptocompare", bar_dt)
    assert key == "cryptocompare:2026-06-28T12:00:00+00:00"
