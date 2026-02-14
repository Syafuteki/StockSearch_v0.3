from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry_with_backoff(
    func: Callable[[], T],
    *,
    retries: int = 4,
    base_delay_sec: float = 1.0,
    backoff: float = 2.0,
    retriable: Callable[[Exception], bool] | None = None,
    logger: logging.Logger | None = None,
) -> T:
    log = logger or logging.getLogger(__name__)
    delay = base_delay_sec
    attempt = 0
    while True:
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            should_retry = attempt <= retries
            if retriable is not None:
                should_retry = should_retry and retriable(exc)
            if not should_retry:
                raise
            log.warning("Retrying after error (attempt=%s/%s): %s", attempt, retries, exc)
            time.sleep(delay)
            delay *= backoff

