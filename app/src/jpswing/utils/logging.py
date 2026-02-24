from __future__ import annotations

import logging
import re


_SECRET_PATTERNS = [
    re.compile(r"(Subscription-Key=)([^&\s]+)", re.IGNORECASE),
]


class _RedactSecretsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001
            return True
        redacted = message
        for pattern in _SECRET_PATTERNS:
            redacted = pattern.sub(r"\1***REDACTED***", redacted)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    filt = _RedactSecretsFilter()
    root = logging.getLogger()
    for handler in root.handlers:
        handler.addFilter(filt)
