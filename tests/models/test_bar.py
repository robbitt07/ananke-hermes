from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from ananke_hermes.models import HermesBar


def _base_bar_kwargs() -> dict:
    return {
        "integration": "cryptocompare",
        "ticker": "btc.usd",
        "security_type": "crypto",
        "bar_type": "trade",
        "data_feed": "stream_bar",
        "dt": datetime(2026, 6, 28, 12, 0, tzinfo=timezone.utc),
        "open": 60000.0,
        "high": 60100.0,
        "low": 59900.0,
        "close": 60050.0,
    }


def test_hermes_bar_with_optional_fields() -> None:
    bar = HermesBar(
        **_base_bar_kwargs(),
        volume=100.0,
        bid_ask_spread=0.5,
        open_int=1200.0,
    )

    assert bar.volume == 100.0
    assert bar.bid_ask_spread == 0.5
    assert bar.open_int == 1200.0
    assert bar.dt.tzinfo is not None


def test_hermes_bar_naive_dt_normalized_to_utc() -> None:
    kwargs = _base_bar_kwargs()
    kwargs["dt"] = datetime(2026, 6, 28, 12, 0)
    bar = HermesBar(**kwargs)

    assert bar.dt.tzinfo == timezone.utc
    assert bar.dt.hour == 12


def test_hermes_bar_missing_required_field_raises() -> None:
    kwargs = _base_bar_kwargs()
    del kwargs["ticker"]

    with pytest.raises(ValidationError):
        HermesBar(**kwargs)
