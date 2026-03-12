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
- TECH morning: `disabled`（`scheduler.morning_cron: ""`）
- TECH close: `0 17 * * 1-5`
- FUND weekly: `0 7 * * 1`
- FUND daily refresh: `10 7 * * 1-5`
- THEME weekly: `0 17 * * 0`（日曜 17:00）
- THEME daily: `0 16 * * 1-5`
- INTEL background: `*/20 * * * *`（有効時）
- AUTO recovery: `15 * * * 1-5`（有効時）
- Startup catch-up step: `*/2 * * * *`（有効時）

起動時回復:
- app起動後は `TECH -> FUND -> THEME -> INTEL` の順でキャッチアップ（欠損営業日を段階的に回復）
- キャッチアップ完了までは `INTEL background` を自動スキップ
- `TECH/FUND/THEME` 定時実行が近い時間帯は一時停止し、定時処理後に再開

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
- FUND backfill:
  - `docker compose run --rm app python -m jpswing.main --once --run-type fund_backfill --date YYYY-MM-DD`
- FUND auto recover:
  - `docker compose run --rm app python -m jpswing.main --once --run-type fund_auto_recover --date YYYY-MM-DD`
- THEME weekly:
  - `docker compose run --rm app python -m jpswing.main --once --run-type theme_weekly --date YYYY-MM-DD`
- THEME daily:
  - `docker compose run --rm app python -m jpswing.main --once --run-type theme_daily --date YYYY-MM-DD`
- THEME auto recover:
  - `docker compose run --rm app python -m jpswing.main --once --run-type theme_auto_recover --date YYYY-MM-DD`
- INTEL background:
  - `docker compose run --rm app python -m jpswing.main --once --run-type intel_background --date YYYY-MM-DD`
- INTEL auto recover:
  - `docker compose run --rm app python -m jpswing.main --once --run-type intel_auto_recover --date YYYY-MM-DD`
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
  - `LLM_TEMPERATURE`
  - `LLM_TIMEOUT_SEC`
- Intel LLM overrides:
  - `INTEL_LLM_MODEL_NAME`
  - `INTEL_LLM_TEMPERATURE`
  - `INTEL_LLM_TIMEOUT_SEC`
  - `INTEL_LLM_RETRIES`
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
  - `INTEL_MCP_CONTEXT_LENGTH`
  - `INTEL_LMSTUDIO_CHAT_ENDPOINT`

補足:
- `INTEL_USE_MCP=true` だけでは Intel LLM の MCP 経路は有効にならない。`INTEL_MCP_PLUGIN_IDS`、`INTEL_MCP_SERVER`、または `search.mcp_integrations` で integration が1件以上必要。
- `INTEL_MCP_SERVER=playwright` のような指定は内部で `mcp/playwright` に正規化される。
- `INTEL_MCP_ENDPOINT` は source 収集用の任意 backend。LM Studio の `/api/v1/chat` 上書きは `INTEL_LMSTUDIO_CHAT_ENDPOINT` を使う。
- `INTEL_LLM_*` が空なら、Intel は共通 LLM 設定を引き継ぐ。

## 6. Discord通知ルーティング
- `DISCORD_WEBHOOK_TECH`
- `DISCORD_WEBHOOK_THEME`
- `DISCORD_WEBHOOK_FUND_INTEL`
- `DISCORD_WEBHOOK_FUND_INTEL_FLASH`
- `DISCORD_WEBHOOK_FUND_INTEL_DETAIL`
- `DISCORD_WEBHOOK_PROPOSALS`

Topic対応:
- `Topic.TECH` -> TECH webhook
- `Topic.THEME` -> THEME webhook
- `Topic.FUND_INTEL_FLASH` -> FUND速報 webhook
- `Topic.FUND_INTEL_DETAIL` -> FUND深掘り webhook
- `Topic.PROPOSALS` -> proposals webhook

フォールバック:
- `THEME` 未設定時は `FUND_INTEL` を使用
- `FUND_INTEL_FLASH` 未設定時は `FUND_INTEL` を使用
- `FUND_INTEL_DETAIL` 未設定時は `FUND_INTEL` を使用

## 7. Intel通知条件
- 速報通知（FUND_INTEL_FLASH）は以下のいずれか:
  - `critical_risk=true`
  - 高シグナルタグの新規付与
  - FUND state 変化（IN/WATCH/OUT）
- 深掘り通知（FUND_INTEL_DETAIL）は深掘り1件ごとに送信

THEME通知:
- `THEME週次更新`: テーマ数 / 紐づけ件数
- `THEME日次更新`: 対象テーマ数 / 重要変化数 / 上位5テーマ
- `THEME日次更新(復旧)`: THEME回復で復旧できた営業日ごと
- `THEME復旧`: 復旧日数 / 欠損検出日数 / 復旧サンプル日

補足:
- TECHのLLM評価は「Top30を1銘柄ずつ」実行し、`confidence_0_100` と Step2順位でTop10を決定。
- TECHは検証失敗時に再プロンプト修復を1回実施し、それでも失敗した銘柄は決定論フォールバックを生成。
- INTELのLLM評価も「1銘柄ずつ」実行。
- INTELの初回要約は EDINET / whitelist IR / `INTEL_MCP_ENDPOINT`（設定時）を束ねて収集し、`sources.full_text` と `xbrl_facts` を優先して LLM に渡す。
- `data_gaps` が残り、かつ MCP integration がある場合は gap research を追加実行する。
- gap research では gap ごとに `gap_resolution_targets` を組み、欠損要素、想定カテゴリ、探すべき事実、検索クエリ、優先資料種別を MCP に渡す。
- Browser MCP では API URL / ダウンロード URL を直接開く前提にせず、会社名、コード、doc id、headline、提出日、公式サイト向け検索ヒントでブラウザ到達可能なページを探す。
- gap research の採用判定は `data_gaps` 減少を最優先に、同数なら `evidence_refs` と `facts` の改善を見る。

## 8. LM Studio / MCP挙動
- 共通LLMは通常 `LMSTUDIO_BASE_URL + /chat/completions` を使う。
- `INTEL_MCP_ENDPOINT` は source 収集用 backend で、Intel LLM の browser MCP chat とは別設定。
- Intel MCP chat は `INTEL_LMSTUDIO_CHAT_ENDPOINT` を優先し、未設定なら `LMSTUDIO_BASE_URL` から `/api/v1/chat` を導出する。
- Intel が MCP を使う条件は `INTEL_USE_MCP=true` かつ integration が1件以上あること。integration は `INTEL_MCP_PLUGIN_IDS`、`INTEL_MCP_SERVER`、`search.mcp_integrations` から組み立てる。
- Intel 初回要約の MCP 呼び出しが 4xx / 5xx / timeout / load failure のときは `/v1/chat/completions` にフォールバックする。
- Intel gap research は MCP専用。gap research が timeout / validation failure のときは、初回要約結果をそのまま採用する。
- LM Studio で認証を有効にしている場合、`LMSTUDIO_API_KEY` は `/v1/chat/completions` と `/api/v1/chat` の両方に送る。

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
  - まず `/v1/chat/completions` と `/api/v1/chat` のどちらで失敗しているかを分けて確認する。
  - `/api/v1/chat` の `Failed to load model ... Operation canceled` は、その1リクエスト失敗を示すことがある。単発発生だけで MCP 全体停止と決めない。
  - Intel MCP 利用時はモデル名、`INTEL_MCP_PLUGIN_IDS` / `INTEL_MCP_SERVER`、LM Studio 側 integration 有効化を確認する。
- LM Studio `401` / `invalid_api_key`:
  - LM Studio 側で認証を有効にしている場合、`LMSTUDIO_API_KEY` を確認する。
- `search.use_mcp=true but no mcp integrations configured`:
  - `INTEL_MCP_PLUGIN_IDS`、`INTEL_MCP_SERVER`、または `search.mcp_integrations` を設定する。
- `Intel LLM MCP gap research failed` / timeout:
  - `/api/v1/chat` が `200` を返していても、長い browser MCP セッションは後段で timeout することがある。
  - `INTEL_LLM_TIMEOUT_SEC` を延ばす、Intel の同時実行負荷を下げる、または一時的に `INTEL_USE_MCP=false` で切り分ける。
- Docker `no configuration file provided: not found`:
  - `docker-compose.yml` があるディレクトリで実行する。

## 12. 起動と監視
- 常駐起動:
  - `docker compose up -d --build`
- ログ監視:
  - `docker compose logs -f app`
- コンテナ状態:
  - `docker compose ps`
