from __future__ import annotations

import logging
from datetime import date
from typing import Any

import httpx

from jpswing.ingest.normalize import to_date, to_float
from jpswing.utils.retry import retry_with_backoff


class FxClient:
    def __init__(self, base_url: str, api_key: str, timeout_sec: int = 20) -> None:
        self.base_url = base_url
        self.api_key = api_key
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch_usdjpy_daily(self, target_date: date) -> dict[str, Any] | None:
        if not self.api_key:
            return None

        def _run() -> dict[str, Any]:
            params = {
                "function": "FX_DAILY",
                "from_symbol": "USD",
                "to_symbol": "JPY",
                "outputsize": "compact",
                "apikey": self.api_key,
            }
            response = httpx.get(self.base_url, params=params, timeout=self.timeout_sec)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"AlphaVantage temporary error: {response.status_code}")
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return {}

        payload = retry_with_backoff(_run, retries=3, base_delay_sec=2.0, backoff=2.0, logger=self.logger)
        series = payload.get("Time Series FX (Daily)")
        if not isinstance(series, dict):
            return None
        point = series.get(target_date.isoformat())
        if not isinstance(point, dict):
            return None
        return {
            "date": to_date(target_date.isoformat()),
            "open": to_float(point.get("1. open")),
            "high": to_float(point.get("2. high")),
            "low": to_float(point.get("3. low")),
            "close": to_float(point.get("4. close")),
            "source": "alphavantage",
        }

