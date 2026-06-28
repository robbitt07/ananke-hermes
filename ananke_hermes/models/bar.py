from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class HermesBar(BaseModel):
    """Hermes intraday OHLCV bar contract."""

    integration: str
    ticker: str
    security_type: str
    bar_type: str
    data_feed: str
    dt: datetime

    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    bid_ask_spread: float | None = None
    open_int: float | None = None

    updated_on: datetime = Field(default_factory=_utc_now)

    @field_validator("dt", mode="after")
    @classmethod
    def dt_to_utc(cls, value: datetime) -> datetime:
        return _normalize_to_utc(value)

    @field_validator("updated_on", mode="after")
    @classmethod
    def updated_on_to_utc(cls, value: datetime) -> datetime:
        return _normalize_to_utc(value)
