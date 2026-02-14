# AGENTS.md — Agent Instructions

## 0. このドキュメントの目的
このリポジトリで作業するAIエージェント（Codex等）が、
- 何を作るべきか（仕様）
- 何をやってはいけないか（制約）
- どこをどう触るか（構成・規約）
- どう動作確認するか（手順）
を短時間で理解できるようにする。

---

## 1. プロジェクト概要（要約）
**jp-swing-agent**：日本株（日足）スイング向けの銘柄抽出・通知システム。
- 自動売買はしない。Discordへ通知のみ。
- 1日2回：08:00（寄り前） / 15:30開始（引け後、J-Quants更新待ちポーリングして実行）。
- データ：J-Quants Standard（主）、EDINET（開示）、USDJPYは Alpha Vantage（主）。
- ローカルLLM：LM Studio API（gpt-oss 20b）を **要約・タグ化・改善提案**に使用。
- **決定（Top30/Top10確定）はプログラムの決定論**で行い、LLMは説明・提案に限定する。

---

## 2. 重要な制約（絶対）
### 2.1 事実とデータ
- 実在しないAPIフィールドを決め打ちしない。レスポンスは存在確認してパースする。
- 取得できないデータは **欠損として扱い**、推測で埋めない。
- LLMの出力は **JSONスキーマで検証**し、不正ならフォールバック（プログラムのスコア上位）。

### 2.2 投資助言禁止
- 通知文末に免責（投資助言でない・自己責任）を必ず付与。

### 2.3 エージェントの責務分離
- **プログラムが決める**：ユニバース、Top30、Top10、スコア、除外フラグ、最終ランキング。
- **LLMがやる**：文章化（理由/注意点/シナリオ）と改善提案（採用は人間）。

---

## 3. データ更新と実行タイミング（JST）
- 08:00：前営業日終値ベースの候補＋当日注意（SQ/決算/信用など）
- 15:30：処理開始。J-Quants日次株価が未更新なら **一定間隔でポーリング**して当日データ取得後に計算・通知。
- 休場日は、設定に応じて「何もしない」or「休場通知のみ」。

---

## 4. 一次（テクニカル）スクリーニング仕様（v1）
### Step1: ユニバース（母集団）
- 株価（終値/調整後終値） >= **300円**
- 20日平均出来高 >= **200,000株**
- 20日平均売買代金（adv20_traded_value） >= **100,000,000円（1億円）**
- （可能なら）時価総額 >= 100億円：取得不能なら自動無効化してログ

### Step2: テクニカル一次スクリーニング（Top30決定）
- 日足（調整後推奨）から指標を算出（MA/ROC/RSI/ATR/出来高倍率等）
- ブレイクアウト／トレンド整列／出来高増を基軸にルールを適用
- スコア（Composite）で上位 **Top30** を確定しDBへ保存

※パラメータ・重みは `config/rules.yaml` に寄せる（コードに埋め込まない）。

---

## 5. 動の情報（Top30→Top10）の扱い（v1）
### 5.1 タグ（v1で反映ON）
- ⚠️ margin_alert（日々公表信用）【Hard】
- 💧 equity_offering_risk（希薄化：公募/CB/ワラント等）【Hard】
- 🧾 earnings_today_or_tomorrow（決算超近い）【Soft（状況でHardにしてもよい）】
- ✅ upward_revision（上方修正）【Soft】
- 🔁 buyback_announcement（自己株買い）【Soft】
- ☠️ critical_risk（継続企業/不正/上場廃止など）【Hard】

`config/tag_policy.yaml` に定義し、Discord表示の絵文字マッピングもここで管理。

### 5.2 SQ（守りモード）
- SQ週/日フラグを計算し、SQ週は **守り**
  - 通知は Top10→Top5 に縮小（または閾値上げ）
  - 流動性/出来高倍率/スコア閾値を厳格化（ルールで切替可能）

---

## 6. 通知フォーマット（確定）
Discordの1行（事実パート）：
`{順位}位 {銘柄コード} {銘柄名} スコア{総合スコア} 終値{終値} 前日比{前日比%} 5日{5日騰落%} 出来高x{出来高倍率} SQ:{SQモード} {警戒フラグ} {注目タグ1}/{注目タグ2}`

LLMはこの後に「理由・注意点・条件付きシナリオ・目安の売買案」を短く補足する。

---

## 7. 構成（触る場所）
推奨構成（例）：
- `src/jpswing/ingest/`：J-Quants/EDINET/FX取得
- `src/jpswing/features/`：指標計算
- `src/jpswing/screening/`：Step1/Step2（Top30）
- `src/jpswing/enrich/`：イベント/地合い/SQ/タグ生成
- `src/jpswing/llm/`：LM Studioクライアント、プロンプト、JSON検証
- `src/jpswing/notify/`：Discord整形と送信
- `config/*.yaml`：閾値とルール（コードにハードコードしない）
- `tests/`：スコア計算・整形・JSON検証のテスト

---

## 8. 依存・起動（エージェントが必ず整備する）
- Docker compose：app + postgres
- `.env.example` を必ず用意（キーは空でOK、必須/任意を明記）
- `README.md` に Windows + Docker + LM Studio の起動手順を記載
- LM Studio接続は OpenAI互換 `/v1/chat/completions` を想定し、base_urlを環境変数で切替可能にする。

### ローカル実行コマンド（例）
- `docker compose up --build`
- `python -m jpswing.main --once`（単発実行モードがあるとデバッグしやすい）
- `pytest -q`

---

## 9. 実装時の品質要件（DoD）
- 主要ジョブ（08:00/15:30）が起動し、DBが更新され、Discordへ送信されること
- J-Quants更新待ちのポーリングが動くこと（失敗時は前営業日フォールバック等）
- LLMのJSONがスキーマで検証され、失敗時フォールバックすること
- ログに「取得件数」「欠損」「APIエラー」「LLM検証失敗」が残ること
- ユニットテスト：スコア計算・Discord整形・JSON検証

---

## 10. エージェント作業ルール（推奨）
- 変更は小さく、動作確認しながら進める（失敗を早く露出させる）
- 新規機能を足すときは `config/` でON/OFFできるようにする
- 外部APIは必ずモック可能な形（クライアントクラス/インターフェース）にする
- “推測で仕様を埋めない”。不明なフィールドはログ＋安全なデフォルト。

## Discord通知（Webhook 3ch 分割）

### 必要な環境変数（.env）
- DISCORD_WEBHOOK_TECH
- DISCORD_WEBHOOK_FUND_INTEL
- DISCORD_WEBHOOK_PROPOSALS

※Webhook URL は秘匿情報。Gitにコミットしないこと（.env / secrets 管理）。

### 送信先チャンネル
- TECH結果 → #tech-alerts（DISCORD_WEBHOOK_TECH）
- FUND Intel重要変化 → #fund-intel（DISCORD_WEBHOOK_FUND_INTEL）
- 提案（Proposal） → #proposals（DISCORD_WEBHOOK_PROPOSALS）

### 通知ポリシー
- #tech-alerts：毎回（既存のタイミング）
- #fund-intel：重要変化のみ
  - 重大リスク☠️の新規発生
  - 高シグナル注目タグ（✅🔁💧🤝など）の新規発生
  - FUND状態（IN/WATCH/OUT）の変化
- #proposals：提案がある日だけ（no-op通知禁止）

---

## FUND Intel 深掘り（MCP/公式ソース）

### 日次予算
- deep-dive 上限：10件/日
- 朝（TECH 08:00 完了後）：最大4件
- 引け後（TECH 引け後バッチ完了後）：残り最大6件
- 合計が10件を超えないこと（DBで当日カウント＆二重起動防止）

### 起動条件（依存関係）
- TECHバッチが SUCCESS で完了したときのみ起動/再開
- TECH失敗時は Intel をスキップ（必要なら運用ログのみ）

### ソース優先順位（Xなし）
- EDINET（API v2）
- 会社公式IR（ドメインホワイトリスト）
- TDnet は将来 API で対応（現状はインターフェースstub）

### LLM出力要件（捏造防止）
- 根拠URLと取得日時を必須
- 根拠に無い事実は unknown / data_gaps に記載
- 用語は TECH と統一：注目タグ / 警戒フラグ（Hard/Soft）/ 重大リスク☠️
