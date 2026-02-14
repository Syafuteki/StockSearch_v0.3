from __future__ import annotations

from jpswing.notify.discord_router import DiscordRouter, Topic


class DiscordNotifier:
    """Backward-compatible single-webhook notifier (TECH topic only)."""

    def __init__(self, webhook_url: str, timeout_sec: int = 15) -> None:
        self.router = DiscordRouter(webhooks={Topic.TECH.value: webhook_url}, timeout_sec=timeout_sec)

    def send(self, content: str) -> tuple[bool, str | None]:
        return self.router.send(Topic.TECH, {"content": content})
