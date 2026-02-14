from __future__ import annotations

import copy
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class DatabaseConfig(BaseModel):
    url: str = Field(default="postgresql+psycopg://jpswing:CHANGE_ME@postgres:5432/jpswing")
    echo: bool = False


class JQuantsPollingConfig(BaseModel):
    enabled: bool = True
    interval_sec: int = 300
    max_wait_minutes: int = 180


class JQuantsConfig(BaseModel):
    base_url: str = "https://api.jquants.com"
    timeout_sec: int = 20
    api_key: str = ""
    polling: JQuantsPollingConfig = Field(default_factory=JQuantsPollingConfig)


class DiscordWebhooksConfig(BaseModel):
    tech: str = ""
    fund_intel: str = ""
    proposals: str = ""


class DiscordThreadsConfig(BaseModel):
    tech: str | None = None
    fund_intel: str | None = None
    proposals: str | None = None


class DiscordConfig(BaseModel):
    webhook_url: str = ""
    webhooks: DiscordWebhooksConfig = Field(default_factory=DiscordWebhooksConfig)
    threads: DiscordThreadsConfig = Field(default_factory=DiscordThreadsConfig)
    max_message_chars: int = 1900
    split_max_parts: int = 2


class LlmConfig(BaseModel):
    base_url: str = "http://host.docker.internal:1234/v1"
    api_key: str = ""
    model_name: str = "gpt-oss-20b"
    temperature: float = 0.1
    timeout_sec: int = 90


class ExternalFxConfig(BaseModel):
    use_fallback: bool = True
    alpha_vantage_api_key: str = ""
    alpha_vantage_base_url: str = "https://www.alphavantage.co/query"


class EdinetConfig(BaseModel):
    base_url: str = "https://disclosure2.edinet-fsa.go.jp"
    api_key: str = ""
    timeout_sec: int = 30


class RagConfig(BaseModel):
    embedding_base_url: str = "http://host.docker.internal:1234/v1"
    embedding_api_key: str = ""
    embedding_model: str = "text-embedding-nomic-embed-text-v1.5"
    chunk_size: int = 700
    chunk_overlap: int = 120


class SchedulerConfig(BaseModel):
    morning_cron: str = "0 8 * * 1-5"
    close_cron: str = "30 15 * * 1-5"
    timezone: str = "Asia/Tokyo"


class AppRuntimeConfig(BaseModel):
    timezone: str = "Asia/Tokyo"
    history_days: int = 120
    llm_input_lookback_days: int = 60
    allow_morning_on_holiday: bool = True
    send_holiday_notice: bool = True
    disclaimer: str = (
        "この通知は参考情報です。投資助言ではありません。"
        "最終的な投資判断は自己責任でお願いします。"
    )
    log_level: str = "INFO"


class MarketConfig(BaseModel):
    index_codes: list[str] = Field(default_factory=lambda: ["NIKKEI225"])
    usd_jpy_symbol: str = "USDJPY"


class AppConfig(BaseModel):
    app: AppRuntimeConfig = Field(default_factory=AppRuntimeConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    jquants: JQuantsConfig = Field(default_factory=JQuantsConfig)
    discord: DiscordConfig = Field(default_factory=DiscordConfig)
    llm: LlmConfig = Field(default_factory=LlmConfig)
    external_fx: ExternalFxConfig = Field(default_factory=ExternalFxConfig)
    edinet: EdinetConfig = Field(default_factory=EdinetConfig)
    rag: RagConfig = Field(default_factory=RagConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    market: MarketConfig = Field(default_factory=MarketConfig)


@dataclass(slots=True)
class Settings:
    app_config: AppConfig
    rules: dict[str, Any]
    tag_policy: dict[str, Any]
    fund_config: dict[str, Any]
    intel_config: dict[str, Any]
    theme_config: dict[str, Any]
    notify_config: dict[str, Any]
    config_dir: Path


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    content = yaml.safe_load(path.read_text(encoding="utf-8"))
    if content is None:
        return {}
    if not isinstance(content, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return content


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in extra.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _expand_env_placeholders(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _expand_env_placeholders(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_placeholders(v) for v in value]
    if isinstance(value, str):
        pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

        def repl(match: re.Match[str]) -> str:
            env_name = match.group(1)
            return os.getenv(env_name, "")

        return pattern.sub(repl, value)
    return value


def _apply_env_overrides(app_cfg: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(app_cfg)
    env_map = {
        ("database", "url"): "DATABASE_URL",
        ("jquants", "api_key"): "JQUANTS_API_KEY",
        ("discord", "webhook_url"): "DISCORD_WEBHOOK_URL",
        ("discord", "webhooks", "tech"): "DISCORD_WEBHOOK_TECH",
        ("discord", "webhooks", "fund_intel"): "DISCORD_WEBHOOK_FUND_INTEL",
        ("discord", "webhooks", "proposals"): "DISCORD_WEBHOOK_PROPOSALS",
        ("llm", "base_url"): "LMSTUDIO_BASE_URL",
        ("llm", "api_key"): "LMSTUDIO_API_KEY",
        ("llm", "model_name"): "LLM_MODEL_NAME",
        ("external_fx", "alpha_vantage_api_key"): "ALPHAVANTAGE_API_KEY",
        ("edinet", "api_key"): "EDINET_API_KEY",
        ("rag", "embedding_base_url"): "EMBEDDING_BASE_URL",
        ("rag", "embedding_api_key"): "EMBEDDING_API_KEY",
        ("rag", "embedding_model"): "EMBEDDING_MODEL_NAME",
    }
    for path_keys, env_name in env_map.items():
        env_value = os.getenv(env_name)
        if env_value is None:
            continue
        cursor = out
        for key in path_keys[:-1]:
            cursor = cursor.setdefault(key, {})
        cursor[path_keys[-1]] = env_value

    tech_hook = out.get("discord", {}).get("webhooks", {}).get("tech")
    legacy_hook = out.get("discord", {}).get("webhook_url", "")
    if not tech_hook and legacy_hook:
        out.setdefault("discord", {}).setdefault("webhooks", {})["tech"] = legacy_hook
    return out


def load_settings(config_dir: str | Path = "config") -> Settings:
    load_dotenv(override=False)
    cfg_dir = Path(config_dir).resolve()
    app_yaml = _load_yaml(cfg_dir / "app.yaml")
    rules = _load_yaml(cfg_dir / "rules.yaml")
    tag_policy = _load_yaml(cfg_dir / "tag_policy.yaml")
    fund = _load_yaml(cfg_dir / "fund.yaml")
    intel = _load_yaml(cfg_dir / "intel.yaml")
    theme = _load_yaml(cfg_dir / "theme.yaml")
    notify = _expand_env_placeholders(_load_yaml(cfg_dir / "notify.yaml"))

    mcp_endpoint = os.getenv("INTEL_MCP_ENDPOINT")
    if mcp_endpoint:
        intel.setdefault("search", {})
        if isinstance(intel["search"], dict):
            intel["search"]["mcp_endpoint"] = mcp_endpoint

    merged_app = _deep_merge(AppConfig().model_dump(), app_yaml)
    if isinstance(notify, dict) and isinstance(notify.get("discord"), dict):
        merged_app = _deep_merge(merged_app, {"discord": notify["discord"]})
    merged_app = _apply_env_overrides(merged_app)
    app_config = AppConfig.model_validate(merged_app)

    return Settings(
        app_config=app_config,
        rules=rules,
        tag_policy=tag_policy,
        fund_config=fund,
        intel_config=intel,
        theme_config=theme,
        notify_config=notify,
        config_dir=cfg_dir,
    )
