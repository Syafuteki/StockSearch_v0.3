# AGENTS.md

このファイルは `jp-stock-swing-agent` の運用メモです。

## 1. システム目的
- 日本株（日足）向けに TECH / FUND / INTEL を実行し、Discordへ通知する。
- 自動発注はしない（通知のみ）。
- ルール改善案は保存のみで、自動適用しない。

## 2. 主要コンポーネント
- TECH: 日次テクニカル選定（Top30 -> Top10）
- FUND: 財務系スコアで IN / WATCH / OUT を更新
- INTEL: EDINET/IR情報を深掘りし、タグ・リスクを更新
- THEME: 週次探索 + 日次強度更新
- RAG: `kb/` と承認済みアイテムの索引化

## 3. スケジュール（現在設定）
- TECH morning: `0 8 * * 1-5`
- TECH close: `30 15 * * 1-5`
- FUND weekly: `0 7 * * 1`
- FUND daily refresh: `10 7 * * 1-5`
- THEME weekly: `20 7 * * 1`
- THEME daily: `40 7 * * 1-5`
- INTEL background: `*/20 * * * *`（有効時）
- AUTO recovery: `15 * * * 1-5`（有効時）

起動時回復:
- app起動後は TECH -> FUND の順でキャッチアップ（欠損営業日を段階的に回復）
- キャッチアップ完了までは INTEL background を自動スキップ
- TECH/FUND 定時実行が近い時間帯は一時停止し、定時処理後に再開

補足:
- INTEL background は `intel.yaml` の `run_on_holiday: true` 時、休場日でも実行可。
- 休場日の business_date は `use_previous_business_day_on_holiday` に従って決まる。

## 4. 実行コマンド（one-shot）
- TECH morning:
  - `docker compose run --rm app python -m jpswing.main --once --run-type morning --date YYYY-MM-DD`
- TECH close:
  - `docker compose run --rm app python -m jpswing.main --once --run-type close --date YYYY-MM-DD`
- FUND weekly:
  - `docker compose run --rm app python -m jpswing.main --once --run-type fund_weekly --date YYYY-MM-DD`
- FUND daily:
  - `docker compose run --rm app python -m jpswing.main --once --run-type fund_daily --date YYYY-MM-DD`
- FUND auto recover:
  - `docker compose run --rm app python -m jpswing.main --once --run-type fund_auto_recover --date YYYY-MM-DD`
- THEME weekly:
  - `docker compose run --rm app python -m jpswing.main --once --run-type theme_weekly --date YYYY-MM-DD`
- THEME daily:
  - `docker compose run --rm app python -m jpswing.main --once --run-type theme_daily --date YYYY-MM-DD`
- INTEL background:
  - `docker compose run --rm app python -m jpswing.main --once --run-type intel_background --date YYYY-MM-DD`
- AUTO recovery:
  - `docker compose run --rm app python -m jpswing.main --once --run-type auto_recover --date YYYY-MM-DD`
- 日付範囲回復:
  - `docker compose run --rm app python -m jpswing.main --once --run-type recover_range --from-date YYYY-MM-DD --to-date YYYY-MM-DD --recover-mode close_only`
- RAG index:
  - `docker compose run --rm app python -m jpswing.main --once --run-type rag_index`

## 5. 必須環境変数（主要）
- DB:
  - `POSTGRES_USER`
  - `POSTGRES_PASSWORD`
  - `POSTGRES_DB`
  - `DATABASE_URL`
- API:
  - `JQUANTS_API_KEY`
  - `EDINET_API_KEY`
  - `EDINET_BASE_URL`
- LLM:
  - `LMSTUDIO_BASE_URL`
  - `LMSTUDIO_API_KEY`
  - `LLM_MODEL_NAME`
- Embedding:
  - `EMBEDDING_BASE_URL`
  - `EMBEDDING_API_KEY`
  - `EMBEDDING_MODEL_NAME`
- Optional:
  - `ALPHAVANTAGE_API_KEY`
- Intel MCP:
  - `INTEL_USE_MCP`
  - `INTEL_MCP_SERVER`
  - `INTEL_MCP_ENDPOINT`
  - `INTEL_MCP_PLUGIN_IDS`

## 6. Discord通知ルーティング
- `DISCORD_WEBHOOK_TECH`
- `DISCORD_WEBHOOK_FUND_INTEL`
- `DISCORD_WEBHOOK_FUND_INTEL_FLASH`
- `DISCORD_WEBHOOK_FUND_INTEL_DETAIL`
- `DISCORD_WEBHOOK_PROPOSALS`

Topic対応:
- `Topic.TECH` -> TECH webhook
- `Topic.FUND_INTEL_FLASH` -> FUND速報 webhook
- `Topic.FUND_INTEL_DETAIL` -> FUND深掘り webhook
- `Topic.PROPOSALS` -> proposals webhook

フォールバック:
- `FUND_INTEL_FLASH` 未設定時は `FUND_INTEL` を使用
- `FUND_INTEL_DETAIL` 未設定時は `FUND_INTEL` を使用

## 7. Intel通知条件
- 速報通知（FUND_INTEL_FLASH）は以下のいずれか:
  - `critical_risk=true`
  - 高シグナルタグの新規付与
  - FUND state 変化（IN/WATCH/OUT）
- 深掘り通知（FUND_INTEL_DETAIL）は深掘り1件ごとに送信

補足:
- TECHのLLM評価は「Top30を1銘柄ずつ」実行し、`confidence_0_100` と Step2順位でTop10を決定。
- TECHは検証失敗時に再プロンプト修復を1回実施し、それでも失敗した銘柄は決定論フォールバックを生成。
- INTELのLLM評価も「1銘柄ずつ」実行。

## 8. LM Studio / MCP挙動
- `INTEL_USE_MCP=true` かつ `INTEL_MCP_PLUGIN_IDS` が設定されていると、MCP経路を優先。
- MCPで失敗した場合は `/v1/chat/completions` にフォールバック。
- LM Studio側でMCPをONでも、アプリ側設定が空ならMCP経路は使わない。

## 9. データ保存の基本方針
- TECH日次テーブルは原則「同一日を置換（upsert相当）」。
- `intel_items` は追記型（履歴蓄積）。
- `intel_queue` / `intel_daily_budget` で重複実行を抑止。

## 10. 運用注意
- `.env` は機密情報を含むため Git にコミットしない。
- API未取得時は「未取得 / data_gaps」で扱い、推測で埋めない。
- 通知末尾の免責文は必ず付与する。

## 11. よくあるエラー
- J-Quants `403`:
  - プラン外API、またはキー/権限不足。該当データは未取得として継続。
- EDINET `302`:
  - ベースURL/リダイレクト条件の差。クライアントは候補URLを順次試行。
- LM Studio `400`:
  - モデル名不一致、APIキー不一致、MCPプラグイン起動失敗の可能性。
- Docker `no configuration file provided: not found`:
  - `docker-compose.yml` があるディレクトリで実行する。

## 12. 起動と監視
- 常駐起動:
  - `docker compose up -d --build`
- ログ監視:
  - `docker compose logs -f app`
- コンテナ状態:
  - `docker compose ps`
