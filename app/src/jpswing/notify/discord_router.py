from __future__ import annotations

import logging
import time
from enum import Enum
from typing import Any, Callable, Mapping
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

DEFAULT_MAX_CONTENT_CHARS = 2000
DEFAULT_MAX_EMBEDS_PER_MESSAGE = 10
DEFAULT_MAX_EMBED_TEXT_PER_MESSAGE = 6000


class Topic(str, Enum):
    TECH = "tech"
    FUND_INTEL = "fund_intel"
    FUND_INTEL_FLASH = "fund_intel_flash"
    FUND_INTEL_DETAIL = "fund_intel_detail"
    PROPOSALS = "proposals"


def split_discord_content(content: str, max_chars: int = DEFAULT_MAX_CONTENT_CHARS) -> list[str]:
    text = str(content or "")
    if len(text) <= max_chars:
        return [text]

    lines = text.splitlines()
    chunks: list[str] = []
    current = ""
    in_code_block = False
    opening_fence = "```"

    for raw_line in lines:
        line = str(raw_line)
        close_len = 4 if in_code_block else 0
        separator_len = 0 if not current else 1
        next_len = len(current) + separator_len + len(line) + close_len

        if next_len > max_chars and current:
            chunk = current + ("\n```" if in_code_block else "")
            chunks.append(chunk)
            current = f"{opening_fence}\n{line}" if in_code_block else line
        elif next_len > max_chars:
            remaining = line
            while remaining:
                prefix = f"{opening_fence}\n" if in_code_block else ""
                room = max_chars - len(prefix) - (4 if in_code_block else 0)
                if room <= 0:
                    room = max_chars
                part = remaining[:room]
                current = f"{prefix}{part}"
                remaining = remaining[room:]
                if remaining:
                    chunk = current + ("\n```" if in_code_block else "")
                    chunks.append(chunk)
                    current = ""
        else:
            if current:
                current = f"{current}\n{line}"
            else:
                current = line

        fence_count = line.count("```")
        if fence_count % 2 == 1:
            stripped = line.strip()
            if not in_code_block and stripped.startswith("```"):
                opening_fence = stripped
            in_code_block = not in_code_block

    if current:
        if in_code_block:
            current = f"{current}\n```"
        chunks.append(current)

    return chunks


def _truncate(value: Any, max_len: int) -> str:
    text = str(value or "")
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return f"{text[: max_len - 3]}..."


def _embed_text_length(embed: dict[str, Any]) -> int:
    total = 0
    for key in ("title", "description"):
        value = embed.get(key)
        if isinstance(value, str):
            total += len(value)
    footer = embed.get("footer")
    if isinstance(footer, dict):
        text = footer.get("text")
        if isinstance(text, str):
            total += len(text)
    author = embed.get("author")
    if isinstance(author, dict):
        name = author.get("name")
        if isinstance(name, str):
            total += len(name)
    fields = embed.get("fields")
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            name = field.get("name")
            value = field.get("value")
            if isinstance(name, str):
                total += len(name)
            if isinstance(value, str):
                total += len(value)
    return total


def _sanitize_embed(embed: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(embed)
    if "title" in out:
        out["title"] = _truncate(out.get("title"), 256)
    if "description" in out:
        out["description"] = _truncate(out.get("description"), 4096)

    footer = out.get("footer")
    if isinstance(footer, dict):
        footer_out = dict(footer)
        if "text" in footer_out:
            footer_out["text"] = _truncate(footer_out.get("text"), 2048)
        out["footer"] = footer_out

    author = out.get("author")
    if isinstance(author, dict):
        author_out = dict(author)
        if "name" in author_out:
            author_out["name"] = _truncate(author_out.get("name"), 256)
        out["author"] = author_out

    fields = out.get("fields")
    if isinstance(fields, list):
        safe_fields: list[dict[str, Any]] = []
        for item in fields[:25]:
            if not isinstance(item, dict):
                continue
            safe_fields.append(
                {
                    "name": _truncate(item.get("name"), 256) or " ",
                    "value": _truncate(item.get("value"), 1024) or " ",
                    "inline": bool(item.get("inline", False)),
                }
            )
        out["fields"] = safe_fields
    return out


def chunk_embeds(
    embeds: list[Mapping[str, Any]],
    *,
    max_embeds: int = DEFAULT_MAX_EMBEDS_PER_MESSAGE,
    max_text_chars: int = DEFAULT_MAX_EMBED_TEXT_PER_MESSAGE,
) -> list[list[dict[str, Any]]]:
    if not embeds:
        return []

    batches: list[list[dict[str, Any]]] = []
    current_batch: list[dict[str, Any]] = []
    current_chars = 0

    for embed in embeds:
        clean = _sanitize_embed(embed)
        embed_chars = _embed_text_length(clean)

        if current_batch and (len(current_batch) >= max_embeds or current_chars + embed_chars > max_text_chars):
            batches.append(current_batch)
            current_batch = []
            current_chars = 0

        current_batch.append(clean)
        current_chars += embed_chars

    if current_batch:
        batches.append(current_batch)
    return batches


class DiscordRouter:
    def __init__(
        self,
        *,
        webhooks: Mapping[str, str] | None = None,
        threads: Mapping[str, str | None] | None = None,
        timeout_sec: int = 15,
        retry_attempts: int = 3,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.webhooks = {k: str(v) for k, v in (webhooks or {}).items()}
        self.threads = {k: (str(v) if v is not None else None) for k, v in (threads or {}).items()}
        self.timeout_sec = timeout_sec
        self.retry_attempts = max(1, retry_attempts)
        self.sleep_fn = sleep_fn
        self.logger = logging.getLogger(self.__class__.__name__)
        self._sent_idempotency_keys: set[str] = set()

    @classmethod
    def from_config(cls, discord_cfg: Any, timeout_sec: int = 15) -> "DiscordRouter":
        fund_intel_webhook = getattr(getattr(discord_cfg, "webhooks", object()), "fund_intel", "")
        fund_intel_thread = getattr(getattr(discord_cfg, "threads", object()), "fund_intel", None)
        webhooks = {
            Topic.TECH.value: getattr(getattr(discord_cfg, "webhooks", object()), "tech", "")
            or getattr(discord_cfg, "webhook_url", ""),
            Topic.FUND_INTEL.value: fund_intel_webhook,
            Topic.FUND_INTEL_FLASH.value: getattr(getattr(discord_cfg, "webhooks", object()), "fund_intel_flash", "")
            or fund_intel_webhook,
            Topic.FUND_INTEL_DETAIL.value: getattr(getattr(discord_cfg, "webhooks", object()), "fund_intel_detail", "")
            or fund_intel_webhook,
            Topic.PROPOSALS.value: getattr(getattr(discord_cfg, "webhooks", object()), "proposals", ""),
        }
        threads = {
            Topic.TECH.value: getattr(getattr(discord_cfg, "threads", object()), "tech", None),
            Topic.FUND_INTEL.value: fund_intel_thread,
            Topic.FUND_INTEL_FLASH.value: getattr(getattr(discord_cfg, "threads", object()), "fund_intel_flash", None)
            or fund_intel_thread,
            Topic.FUND_INTEL_DETAIL.value: getattr(getattr(discord_cfg, "threads", object()), "fund_intel_detail", None)
            or fund_intel_thread,
            Topic.PROPOSALS.value: getattr(getattr(discord_cfg, "threads", object()), "proposals", None),
        }
        return cls(webhooks=webhooks, threads=threads, timeout_sec=timeout_sec)

    def send(
        self,
        topic: Topic,
        payload: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> tuple[bool, str | None]:
        webhook_url = str(self.webhooks.get(topic.value, "") or "").strip()
        if not webhook_url:
            self.logger.warning("Discord webhook missing for topic=%s. skip notification.", topic.value)
            return False, "webhook_url_empty"

        opts = dict(options or {})
        idem = opts.get("idempotency_key")
        if isinstance(idem, str) and idem:
            if idem in self._sent_idempotency_keys:
                self.logger.info("Skip duplicate idempotency_key=%s", idem)
                return True, None

        content = payload.get("content")
        embeds = payload.get("embeds")
        username = payload.get("username")
        avatar_url = payload.get("avatar_url")

        content_parts = split_discord_content(str(content), DEFAULT_MAX_CONTENT_CHARS) if content else []
        embed_batches = chunk_embeds(embeds if isinstance(embeds, list) else [])

        message_count = max(len(content_parts), len(embed_batches), 1)
        execute_url = self._build_execute_url(
            webhook_url=webhook_url,
            thread_id=(opts.get("thread_id") if "thread_id" in opts else self.threads.get(topic.value)),
            wait=bool(opts.get("wait", True)),
        )

        overall_ok = True
        last_error: str | None = None
        for index in range(message_count):
            request_payload: dict[str, Any] = {}
            if index < len(content_parts):
                request_payload["content"] = content_parts[index]
            if index < len(embed_batches):
                request_payload["embeds"] = embed_batches[index]
            if isinstance(username, str) and username:
                request_payload["username"] = username
            if isinstance(avatar_url, str) and avatar_url:
                request_payload["avatar_url"] = avatar_url

            if not request_payload:
                continue
            ok, err = self._post_with_retry(execute_url, request_payload)
            if not ok:
                overall_ok = False
                last_error = err

        if overall_ok and isinstance(idem, str) and idem:
            self._sent_idempotency_keys.add(idem)
            if len(self._sent_idempotency_keys) > 10000:
                self._sent_idempotency_keys.clear()

        return overall_ok, last_error

    def _build_execute_url(self, *, webhook_url: str, thread_id: str | None, wait: bool) -> str:
        parsed = urlparse(webhook_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["wait"] = "true" if wait else "false"
        if thread_id:
            query["thread_id"] = str(thread_id)
        rebuilt = parsed._replace(query=urlencode(query))
        return urlunparse(rebuilt)

    def _post_with_retry(self, url: str, payload: dict[str, Any]) -> tuple[bool, str | None]:
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = httpx.post(url, json=payload, timeout=self.timeout_sec)
            except httpx.HTTPError as exc:
                if attempt >= self.retry_attempts:
                    return False, f"http_error:{exc}"
                self.sleep_fn(0.5 * (2 ** (attempt - 1)))
                continue

            status = response.status_code
            if 200 <= status < 300:
                return True, None

            if status == 429 and attempt < self.retry_attempts:
                wait_sec = self._retry_after_seconds(response)
                self.logger.warning("Discord rate limited. wait %.3fs and retry.", wait_sec)
                self.sleep_fn(wait_sec)
                continue

            if 500 <= status < 600 and attempt < self.retry_attempts:
                backoff = 0.5 * (2 ** (attempt - 1))
                self.sleep_fn(backoff)
                continue

            body = _truncate(response.text, 500)
            if 400 <= status < 500 and status != 429:
                self.logger.error("Discord request rejected status=%s body=%s", status, body)
            return False, f"status={status} body={body}"

        return False, "retry_exhausted"

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float:
        header = response.headers.get("Retry-After")
        if header is not None:
            try:
                value = float(header)
                if value > 1000:
                    return max(0.1, value / 1000.0)
                return max(0.1, value)
            except ValueError:
                pass

        try:
            body = response.json()
        except Exception:  # noqa: BLE001
            body = {}
        retry_after = body.get("retry_after")
        if retry_after is None:
            return 1.0
        try:
            value = float(retry_after)
            if value > 1000:
                return max(0.1, value / 1000.0)
            return max(0.1, value)
        except (TypeError, ValueError):
            return 1.0
