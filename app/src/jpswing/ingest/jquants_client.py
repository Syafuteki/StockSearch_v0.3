from __future__ import annotations

import logging
from datetime import date
from typing import Any

import httpx

from jpswing.ingest.normalize import to_date
from jpswing.utils.retry import retry_with_backoff


class JQuantsClient:
    def __init__(self, base_url: str, api_key: str, timeout_sec: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger(self.__class__.__name__)
        headers: dict[str, str] = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["X-API-KEY"] = api_key
        self.client = httpx.Client(base_url=self.base_url, timeout=self.timeout_sec, headers=headers)

    def close(self) -> None:
        self.client.close()

    def _request(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        def _run() -> dict[str, Any]:
            response = self.client.get(path, params=params)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"J-Quants temporary error: {response.status_code} {response.text[:200]}")
            if response.status_code == 404:
                return {}
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return {"data": payload}

        return retry_with_backoff(_run, retries=4, base_delay_sec=1.0, backoff=2.0, logger=self.logger)

    @staticmethod
    def _extract_items(payload: dict[str, Any], candidate_keys: list[str]) -> list[dict[str, Any]]:
        for key in candidate_keys:
            data = payload.get(key)
            if isinstance(data, list):
                return [row for row in data if isinstance(row, dict)]
        for value in payload.values():
            if isinstance(value, list) and all(isinstance(x, dict) for x in value):
                return value
        return []

    def _fetch_paginated(
        self,
        path: str,
        *,
        params: dict[str, Any] | None,
        item_keys: list[str],
    ) -> list[dict[str, Any]]:
        all_items: list[dict[str, Any]] = []
        current_params = dict(params or {})
        seen_keys: set[str] = set()
        while True:
            payload = self._request(path, params=current_params)
            items = self._extract_items(payload, item_keys)
            all_items.extend(items)
            pagination_key = payload.get("pagination_key") or payload.get("paginationKey")
            if not pagination_key:
                break
            if pagination_key in seen_keys:
                self.logger.warning("Duplicate pagination key detected for %s. stop paging.", path)
                break
            seen_keys.add(str(pagination_key))
            current_params["pagination_key"] = pagination_key
        return all_items

    def fetch_calendar(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        params = {"from": from_date.isoformat(), "to": to_date.isoformat()}
        return self._fetch_paginated(
            "/v2/markets/calendar",
            params=params,
            item_keys=["calendar", "markets_calendar", "trading_calendar"],
        )

    def fetch_equities_master(self, as_of: date) -> list[dict[str, Any]]:
        params = {"date": as_of.isoformat()}
        return self._fetch_paginated(
            "/v2/equities/master",
            params=params,
            item_keys=["listed_info", "master", "equities", "items"],
        )

    def fetch_daily_bars(self, target_date: date, code: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"date": target_date.isoformat()}
        if code:
            params["code"] = code
        return self._fetch_paginated(
            "/v2/equities/bars/daily",
            params=params,
            item_keys=["daily_quotes", "daily_bars", "bars", "items"],
        )

    def fetch_earnings_calendar(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        params = {"from": from_date.isoformat(), "to": to_date.isoformat()}
        return self._fetch_paginated(
            "/v2/equities/earnings-calendar",
            params=params,
            item_keys=["earnings_calendar", "announcements", "items"],
        )

    def fetch_indices_bars_daily(self, target_date: date, code: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"date": target_date.isoformat()}
        if code:
            params["code"] = code
        return self._fetch_paginated(
            "/v2/indices/bars/daily",
            params=params,
            item_keys=["indices_daily_quotes", "indices_bars", "daily_quotes", "items"],
        )

    def fetch_short_ratio(self, target_date: date) -> list[dict[str, Any]]:
        params = {"date": target_date.isoformat()}
        return self._fetch_paginated(
            "/v2/markets/short-ratio",
            params=params,
            item_keys=["short_ratio", "items"],
        )

    def fetch_short_sale_report(self, target_date: date) -> list[dict[str, Any]]:
        params = {"date": target_date.isoformat()}
        return self._fetch_paginated(
            "/v2/markets/short-sale-report",
            params=params,
            item_keys=["short_sale_report", "items"],
        )

    def fetch_margin_alert(self, target_date: date) -> list[dict[str, Any]]:
        params = {"date": target_date.isoformat()}
        return self._fetch_paginated(
            "/v2/markets/margin-alert",
            params=params,
            item_keys=["margin_alert", "items"],
        )

    def fetch_225_options(self, target_date: date) -> list[dict[str, Any]]:
        params = {"date": target_date.isoformat()}
        return self._fetch_paginated(
            "/v2/derivatives/bars/daily/options/225",
            params=params,
            item_keys=["option_225_daily_quotes", "derivatives_bars", "items"],
        )

    def fetch_financial_summary(self, target_date: date, code: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"date": target_date.isoformat()}
        if code:
            params["code"] = code
        return self._fetch_paginated(
            "/v2/fins/summary",
            params=params,
            item_keys=["fins_summary", "financial_summary", "statements", "items"],
        )

    def has_date_in_rows(self, rows: list[dict[str, Any]], target_date: date) -> bool:
        for row in rows:
            row_date = to_date(
                row.get("Date")
                or row.get("date")
                or row.get("TradeDate")
                or row.get("TargetDate")
                or row.get("DisclosedDate")
            )
            if row_date == target_date:
                return True
        return False
