# jp-stock-swing-agent (MVP)

日本株（東証）の日足データを使って、TECH / FUND / INTEL を実行し、Discordへ通知するシステムです。  
自動発注は行わず、通知のみを行います。

## 1. 構成

```text
app/src/jpswing/
  main.py                     # エントリーポイント（scheduler / one-shot）
  pipeline.py                 # TECH実行本体
  fund_intel_orchestrator.py  # FUND / INTEL / THEME 実行
  config.py                   # YAML + .env 読み込み
  db/                         # SQLAlchemy models / session
  ingest/                     # J-Quants / EDINET / FX 取得
  features/                   # テクニカル指標
  screening/                  # Step1 / Step2
  fund/                       # FUND状態更新
  intel/                      # Intel検索 / 優先度 / LLM schema
  theme/                      # Theme探索 / 強度更新
  rag/                        # kb index / retrieval
  notify/                     # Discord Router
app/tests/
config/
  app.yaml
  rules.yaml
  tag_policy.yaml
  fund.yaml
  intel.yaml
  theme.yaml
  notify.yaml
```

## 2. 前提

- Windows 11
- Docker Desktop + Docker Compose
- LM Studio（OpenAI互換API有効）
- J-Quants APIキー
- EDINET APIキー

## 3. セットアップ

1. `.env.example` を `.env` にコピー
2. `.env` を設定
3. 起動

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

ログ確認:

```powershell
docker compose logs -f app
```

コンテナ状態:

```powershell
docker compose ps
```

## 4. 主要環境変数

必須（主要）:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `DATABASE_URL`
- `JQUANTS_API_KEY`
- `EDINET_API_KEY`
- `LMSTUDIO_BASE_URL`
- `LLM_MODEL_NAME`

Discord通知:

- `DISCORD_WEBHOOK_TECH`
- `DISCORD_WEBHOOK_FUND_INTEL`
- `DISCORD_WEBHOOK_FUND_INTEL_FLASH`
- `DISCORD_WEBHOOK_FUND_INTEL_DETAIL`
- `DISCORD_WEBHOOK_PROPOSALS`

MCP関連（Intel）:

- `INTEL_USE_MCP=true|false`
- `INTEL_MCP_PLUGIN_IDS`（例: `mcp/playwright`）
- `INTEL_MCP_SERVER`
- `INTEL_MCP_ENDPOINT`

補足:

- `DISCORD_WEBHOOK_TECH` が空の場合、`DISCORD_WEBHOOK_URL` をTECH用の後方互換として利用します。
- `.env` は機密情報を含むため、Gitへコミットしないでください。

## 5. Discord通知ルーティング

- `Topic.TECH` -> `DISCORD_WEBHOOK_TECH`
- `Topic.FUND_INTEL_FLASH` -> `DISCORD_WEBHOOK_FUND_INTEL_FLASH`
- `Topic.FUND_INTEL_DETAIL` -> `DISCORD_WEBHOOK_FUND_INTEL_DETAIL`
- `Topic.PROPOSALS` -> `DISCORD_WEBHOOK_PROPOSALS`

フォールバック:

- `DISCORD_WEBHOOK_FUND_INTEL_FLASH` 未設定時は `DISCORD_WEBHOOK_FUND_INTEL` を使用
- `DISCORD_WEBHOOK_FUND_INTEL_DETAIL` 未設定時は `DISCORD_WEBHOOK_FUND_INTEL` を使用

thread利用:

- `config/notify.yaml` の `discord.threads.*` を設定
- Routerは `?thread_id=<id>&wait=true` で送信

## 6. スケジュール（既定）

TECH:

- morning: `0 8 * * 1-5`
- close: `30 15 * * 1-5`

FUND / THEME:

- fund_weekly: `0 7 * * 1`
- fund_daily: `10 7 * * 1-5`
- theme_weekly: `20 7 * * 1`
- theme_daily: `40 7 * * 1-5`

INTEL:

- background: `*/20 * * * *`（`config/intel.yaml` の `schedule.enabled=true`）
- recovery: `15 * * * 1-5`（`config/intel.yaml` の `recovery.enabled=true`）

起動時の自動回復:

- app起動後は `TECH -> FUND` の順でキャッチアップ（欠損営業日を段階的に回復）
- 回復中は `INTEL background` を自動スキップ（先にTECH/FUNDを埋める）
- キャッチアップは `startup_catchup.cron` ごとに進み、`max_days_per_run` 件ずつ埋める
- TECH/FUNDの定時実行が近い時間帯は自動で一時停止し、定時処理後に再開

## 6.1 実行中ジョブ確認（TECH / FUND / INTEL）

直近ログで処理種別を確認:

```powershell
docker compose logs --since 30m app | Select-String -Pattern "Pipeline start run_type=|Scheduled aux job start type=|Scheduled intel background start|Scheduled auto recover start"
```

TECHだけ確認:

```powershell
docker compose logs --since 30m app | Select-String -Pattern "Pipeline start run_type=morning|Pipeline start run_type=close|Pipeline end run_type=morning|Pipeline end run_type=close"
```

FUND/THEMEだけ確認:

```powershell
docker compose logs --since 30m app | Select-String -Pattern "Scheduled aux job start type=fund_|Scheduled aux job start type=theme_|One-shot result run_type=fund_|One-shot result run_type=theme_"
```

INTELだけ確認:

```powershell
docker compose logs --since 30m app | Select-String -Pattern "Scheduled intel background start|Scheduled intel background end|run_type=intel_background|fund_intel|Intel deep-dive paused|Intel queue item failed"
```

リアルタイム監視:

```powershell
docker compose logs -f app
```

Intelキュー件数（pending/done/failed）を確認:

```powershell
@'
from sqlalchemy import create_engine, text
import os

e = create_engine(os.environ["DATABASE_URL"])
with e.connect() as c:
    rows = c.execute(text("select status, count(*) as cnt from intel_queue group by status order by status")).fetchall()
    for r in rows:
        print(f"{r.status}\t{r.cnt}")
'@ | docker compose run --rm -T app python -
```

## 7. one-shot実行コマンド

TECH:

```powershell
docker compose run --rm app python -m jpswing.main --once --run-type morning --date 2026-02-13
docker compose run --rm app python -m jpswing.main --once --run-type close --date 2026-02-13
```

FUND / THEME:

```powershell
docker compose run --rm app python -m jpswing.main --once --run-type fund_weekly --date 2026-02-13
docker compose run --rm app python -m jpswing.main --once --run-type fund_daily --date 2026-02-13
docker compose run --rm app python -m jpswing.main --once --run-type fund_backfill --date 2026-02-13
docker compose run --rm app python -m jpswing.main --once --run-type fund_auto_recover --date 2026-02-16
docker compose run --rm app python -m jpswing.main --once --run-type theme_weekly --date 2026-02-13
docker compose run --rm app python -m jpswing.main --once --run-type theme_daily --date 2026-02-13
```

INTEL / 復旧:

```powershell
docker compose run --rm app python -m jpswing.main --once --run-type intel_background --date 2026-02-16
docker compose run --rm app python -m jpswing.main --once --run-type auto_recover --date 2026-02-16
docker compose run --rm app python -m jpswing.main --once --run-type recover_range --from-date 2026-02-10 --to-date 2026-02-14 --recover-mode close_only
```

RAG:

```powershell
docker compose run --rm app python -m jpswing.main --once --run-type rag_index
```

## 8. 実行ロジック概要

TECH:

- Step0: 営業日判定
- Step1: Universeフィルタ
- Step2: テクニカルスコアでTop30
- Step3: Top30の各銘柄を「1銘柄1LLM呼び出し」で評価し、`confidence_0_100` 優先でTop10化（同点はStep2順位）
- 検証失敗時は同銘柄に対して再プロンプトでJSON修復を1回実施
- それでも失敗した銘柄は入力データから決定論フォールバックを生成して継続
- 最終候補はStep3評価対象（LLM/決定論フォールバック済み）の範囲で確定し、Step2追加補完はしない

FUND:

- 財務サマリからスコア計算
- `IN / WATCH / OUT` 更新
- carry-forward により未開示日でも状態維持可能

INTEL:

- 候補プール A+B（FUND状態 + EDINET更新 + Theme）
- 優先度順に deep-dive
- EDINET / whitelist IR / MCP（設定時）を情報源に要約
- 結果を `intel_items` 等に保存

速報通知条件（FUND_INTEL_FLASH）:

- `critical_risk=true`
- 高シグナルタグの新規付与
- FUND状態変化

深掘り通知（FUND_INTEL_DETAIL）:

- 深掘り1件ごとに送信

## 9. LM Studio / MCP

- Intelは `INTEL_USE_MCP=true` かつ `INTEL_MCP_PLUGIN_IDS` 設定時、MCP経路を優先
- MCP失敗時は `/v1/chat/completions` へフォールバック
- LM Studio側でMCPを有効でも、アプリ側設定が空ならMCPは使いません

## 10. データ保存方針

- TECH日次テーブルは同一日を置換（再実行で更新）
- `intel_items` は追記型（履歴が増える）
- `intel_queue` / `intel_daily_budget` で重複実行を抑制

## 10.1 自動回復の設定

`config/intel.yaml` の `recovery`:

- `enabled`: TECH回復ジョブの有効化
- `run_on_startup`: app起動時にTECH回復を1回実行
- `lookback_business_days`: 欠損検出範囲
- `max_days_per_run`: 1回で埋める最大営業日数

`config/intel.yaml` の `startup_catchup`:

- `enabled`: 起動後キャッチアップゲートの有効化
- `cron`: 起動後キャッチアップの実行間隔
- `pause_lead_minutes`: TECH/FUND定時実行の何分前から一時停止するか

`config/fund.yaml` の `recovery`:

- `enabled`: FUND回復の有効化
- `run_on_startup`: app起動時にFUND回復を1回実行
- `lookback_business_days`: 欠損検出範囲
- `max_days_per_run`: 1回で埋める最大営業日数
- `run_on_holiday`: 休場日実行可否
- `force`: FUND計算を強制するか（通常は `false` 推奨）

## 11. トラブルシュート

- `no configuration file provided: not found`
  - `docker-compose.yml` のあるディレクトリで実行してください。
- J-Quants `403 Forbidden`
  - プラン外APIまたは権限不足です。該当データは未取得として継続します。
- EDINET `302 Redirect`
  - クライアントは候補ベースURLを順次試行します。キーや接続も確認してください。
- LM Studio `400 Bad Request`
  - モデル名/APIキー/MCP plugin起動状態を確認してください。
- `VECTOR(...)` 関連エラー
  - pgvector拡張不足の可能性。以下を実施:

```powershell
docker compose exec postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

## 12. テスト

```powershell
python -m pytest -q
```

## 13. セキュリティ運用

- `.env` をリポジトリに含めない
- Webhook/APIキーをログや通知文に出さない
- 取得不可データは「未取得」として扱い、推測で埋めない
- 通知末尾に免責文を必ず含める
