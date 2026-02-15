# jp-stock-swing-agent (MVP)

日本株（東証）の日足データを使って、毎営業日 `08:00` / `15:30` にスクリーニングし、Top10候補をDiscordへ通知する通知専用エージェントです。  
自動発注はしません。ローカルLLM（LM StudioのOpenAI互換API）でTop30→Top10の文章化と改善提案を行います。

## 1. 構成

```text
app/src/jpswing/
  main.py                 # スケジューラ起動
  pipeline.py             # TECH Step0-3の実行本体
  fund_intel_orchestrator.py # TECH完了フックでFUND/Intel実行
  config.py               # yaml + env 読み込み
  db/                     # SQLAlchemy models/session
  ingest/                 # J-Quants / FX 取得
  fund/                   # FUND一次スクリーニング / 状態更新
  intel/                  # Intel検索 / priority / JSON schema
  theme/                  # 週次探索 / 日次ThemeStrength
  rag/                    # kb indexer / retrieve API
  features/               # テクニカル指標
  screening/              # Step1/Step2
  enrich/                 # イベント/地合い/SQ
  llm/                    # TECH用LM Studio client + schema検証
  notify/                 # Discord通知整形/送信
app/tests/
config/
  app.yaml
  rules.yaml
  tag_policy.yaml
```

## 2. 必要環境

- Windows 11
- Docker Desktop + Docker Compose
- LM Studio（`gpt-oss-20b` 等をロードしてAPI有効化）
- J-Quants APIキー（Standard想定）

## 3. セットアップ

1. `.env.example` を `.env` にコピー
2. `.env` の機密値を設定（特に `JQUANTS_API_KEY`, `EDINET_API_KEY`, `DISCORD_WEBHOOK_URL`）
3. 必要に応じて `config/*.yaml` を編集
4. 起動

```powershell
Copy-Item .env.example .env
docker compose up --build
```

追加設定ファイル:

- `config/fund.yaml`
- `config/intel.yaml`
- `config/theme.yaml`

## 4. セキュリティ方針（今回のMVP）

- `.env` はGit管理対象外（`.gitignore`）
- DB認証情報は `.env` 必須（`docker-compose.yml` で必須化）
- Postgresの `5432` はデフォルト非公開
- appコンテナは非rootユーザーで実行
- GitHub Actionsで `pip-audit` を定期実行（`.github/workflows/security.yml`）

DBポートをローカルから直接使いたい場合のみ、追加Composeファイルを重ねて起動します。

```powershell
docker compose -f docker-compose.yml -f docker-compose.db-local.yml up --build
```

## 5. 単発実行

```powershell
# 朝レポート相当
docker compose run --rm app python -m jpswing.main --once --run-type morning

# 引け後レポート相当
docker compose run --rm app python -m jpswing.main --once --run-type close

# FUND/Theme/RAG系の個別実行
docker compose run --rm app python -m jpswing.main --once --run-type fund_weekly
docker compose run --rm app python -m jpswing.main --once --run-type fund_daily
docker compose run --rm app python -m jpswing.main --once --run-type theme_weekly
docker compose run --rm app python -m jpswing.main --once --run-type theme_daily
docker compose run --rm app python -m jpswing.main --once --run-type rag_index
```

日付指定:

```powershell
docker compose run --rm app python -m jpswing.main --once --run-type morning --date 2026-02-13
```

## 6. スケジュール仕様

- `08:00 JST`: 前営業日終値ベースで通知
- `15:30 JST`: ジョブ開始。`/v2/equities/bars/daily` 更新をポーリングしてから処理
- 営業日判定は `/v2/markets/calendar`
- 休場時は `config/app.yaml` の `send_holiday_notice` に従って通知有無を切替
- TECH `morning/close` 成功後、FUND+Intel deep-diveをフック実行
  - morning cap=4
  - close cap=6
  - daily hard limit=10

## 7. スクリーニング手順

1. `Step0`: 営業日判定
2. `Step1`: ユニバース絞り込み（株価・出来高・売買代金・時価総額）
3. `Step2`: テクニカルスコアでTop30を機械決定
4. `Step3`: イベント/地合いを付与しLLMでTop10化
5. LLM JSONが壊れている場合はStep2上位10へフォールバック

## 8. FUND / Intel / Theme

- FUND:
  - 週次一次スクリーニングで `IN/WATCH/OUT` 更新
  - 日次は金融データ更新 or Intelリスク反映時のみ差分更新
- Intel:
  - 候補プール A+B（FUND state + EDINET更新 + Theme強い/上昇）
  - deterministic priority で deep-dive 対象選定
- EDINET + whitelist IR + optional MCP でソース取得
  - MCPは `INTEL_MCP_ENDPOINT` 設定時のみ有効（未設定は自動スキップ）
  - LLM strict JSON schema検証（invalid時フォールバック）
- Theme:
  - 週次でseedテーマ再構築と symbol map 更新
  - 日次で ThemeStrength 更新

Discord通知条件:
- 重大リスク☠️
- high-signal tags
- FUND state変更
- proposal存在時

## 9. RAG

- `kb/*.md` を front-matter付きで index
- chunk + embedding を `kb_documents / kb_chunks` に保存
- `books_fulltext` は index 可能だが、LLM向け retrieval では除外
- retrieve API: `jpswing.rag.api.RagService.retrieve(query, filters, top_k)`

## 10. ルール成長設計

- `config/rules.yaml` にルール本体を保持
- `rule_versions` にバージョン保存
- LLM提案は `rule_suggestions` に保存のみ（自動適用しない）
- 人間が採用時に `rules.yaml` を更新して再起動

## 11. テスト

```powershell
python -m pytest -q
```

## 10. 脆弱性チェック（ローカル）

```powershell
python -m pip install pip-audit
python -m pip_audit --strict
```

## 11. Alembic

初期状態では `create_all` でMVP起動します。  
運用でマイグレーション管理を行う場合:

```powershell
docker compose run --rm app alembic -c app/alembic.ini upgrade head
```

## 12. 例外時の動作

- API欠損/未契約: 該当機能をスキップしてログ記録
- 株価更新遅延: ポーリングで待機、タイムアウト後は取得済みデータで継続
- 休場: スキップまたは休場通知
- LLM不正JSON: フォールバックでTop10確定

## Discord Notification Router (3 Webhooks)

Required env vars (`.env`):
- `DISCORD_WEBHOOK_TECH` : TECH report channel webhook
- `DISCORD_WEBHOOK_FUND_INTEL` : FUND Intel important change channel webhook
- `DISCORD_WEBHOOK_PROPOSALS` : proposals-only channel webhook

Routing:
- TECH (`Topic.TECH`) -> `DISCORD_WEBHOOK_TECH`
- FUND Intel (`Topic.FUND_INTEL`) -> `DISCORD_WEBHOOK_FUND_INTEL`
- Proposals (`Topic.PROPOSALS`) -> `DISCORD_WEBHOOK_PROPOSALS`

Optional thread routing:
- Set `config/notify.yaml` `discord.threads.tech|fund_intel|proposals`
- Router sends webhook execute URL with `?thread_id=<id>&wait=true`

Behavior:
- Missing webhook for a topic: warning log + skip (no crash)
- HTTP 429: respects `Retry-After` then retries
- 5xx: exponential backoff retry
- Message limits enforced:
  - content split at 2000 chars
  - embeds split to max 10 per message
- Proposals channel posts only when proposals exist (no no-op messages)

## pgvector Error Recovery

If startup fails around `CREATE TABLE kb_chunks ... embedding VECTOR(1024)`:

1. Recreate DB with the updated pgvector image:
```powershell
docker compose down -v
docker compose up --build
```
2. If you must keep volume data, run extension creation manually once:
```powershell
docker compose exec postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "CREATE EXTENSION IF NOT EXISTS vector;"
```
