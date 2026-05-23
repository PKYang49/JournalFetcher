# JournalFetcher

自動追蹤主要醫學期刊文章，支援兩種工作流：

- 互動式瀏覽：從 PubMed 抓最新文章，產生繁體中文摘要，終端機勾選後下載 PDF。
- 每週週報：每週一自動抓取期刊文章，產生中文摘要與個人化短評，輸出 GitHub Pages HTML，並延後發 Discord 通知。

## 支援期刊

- NEJM
- Lancet
- JAMA
- JACC
- European Heart Journal
- EuroIntervention
- Circulation
- Heart
- JAMA Cardiology
- BJSM
- Medicine & Science in Sports & Exercise
- Sports Medicine
- Journal of Applied Physiology

## 安裝

```bash
pip install -r requirements.txt
playwright install chromium
```

建立 `.env`：

```env
# Weekly Discord webhook
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

# Discord Bot（選用：DOI 下載機器人）
DISCORD_BOT_TOKEN=...
DISCORD_ALLOWED_USER_IDS=123456789
DISCORD_CHANNEL_ID=987654321
BOT_OUTPUT_DIR=~/GoogleDrive/papers
BOT_SEND_PDF=true

# 週報精選回饋中繼（選用：Google Apps Script web app）
FEEDBACK_ENDPOINT_URL=https://script.google.com/macros/s/.../exec
FEEDBACK_SYNC_TOKEN=自訂亂碼，需與 Apps Script 內 SYNC_TOKEN 一致
```

摘要與評讀採雙後端,主要走 `claude -p`,撞到限額自動 fallback 到 `codex exec`。兩邊都不需要 API key,各自用 CLI 訂閱登入。

```bash
claude --version                      # Claude Code CLI(主要)
codex --version
codex login status                    # 應顯示 Logged in using ChatGPT
```

- **主要後端 `claude -p`**(吃 Claude 訂閱 / 6/15 後的 Agent SDK 額度):
  Haiku 4.5 跑摘要,Opus 4.6 跑評讀。透過 macOS keychain OAuth 認證,不需要 `ANTHROPIC_API_KEY`。
- **Fallback `codex exec`**(吃 ChatGPT 訂閱):
  撞到 Claude 限額(rate-limit / credit / quota / monthly 等錯誤訊息)時自動切換,本次 process 後續全走 codex,下次 run 重新嘗試 claude。GPT 5.4 摘要、GPT 5.5 評讀。

Dispatcher 邏輯與限額判斷字串集中在 `modules/claude_exec.py`。

模型覆寫(`.env` 或 export):

| 變數 | 用途 | 預設 |
|---|---|---|
| `JOURNAL_FETCHER_CLAUDE_SUMMARY_MODEL` | claude 摘要模型 | `claude-haiku-4-5` |
| `JOURNAL_FETCHER_CLAUDE_APPRAISAL_MODEL` | claude 評讀模型 | `claude-opus-4-6` |
| `JOURNAL_FETCHER_CODEX_MODEL` | codex 摘要 fallback | Codex default 降一個 minor |
| `JOURNAL_FETCHER_APPRAISAL_MODEL` | codex 評讀 fallback | Codex default / latest |
| `JOURNAL_FETCHER_APPRAISAL_CHAR_BACKSTOP` | 評讀文章字元上限 | `1500000`(超過會被 flag 為 `too_large`,不會被截斷) |

## 互動式抓取與下載

```bash
# 抓全部期刊（預設最近 30 天，每本 20 篇）
python fetch_journals.py

# 只抓特定期刊
python fetch_journals.py --journals NEJM JAMA JACC

# 調整篇數與時間範圍
python fetch_journals.py --count 15 --days 14

# 只生成摘要，不進入下載流程
python fetch_journals.py --no-download

# 跳過摘要，直接列標題勾選下載
python fetch_journals.py --no-summary
```

流程：

1. PubMed E-utilities 抓取 metadata。
2. `codex exec` 產生三句繁體中文摘要。
3. `questionary` checkbox 選擇文章。
4. 透過機構內網 IP 授權與各出版社 fallback 下載 PDF。

輸出會放在：

```text
output/YYYY-MM-DD/
├── errors.log
├── download_failures.log
└── *.pdf
```

## 每週自動週報

手動執行：

```bash
# 完整流程：抓文獻、摘要、渲染 HTML、commit/push、發 Discord
python3 -m weekly.run_weekly

# 只產生 HTML，不 push、不發 Discord
python3 -m weekly.run_weekly --dry-run

# 產生週報並 push，但不發 Discord
python3 -m weekly.run_weekly --no-discord

# 精選 N 篇，下載 PDF 並產生完整評讀
python3 -m weekly.run_weekly --select-top 8

# 測試版面，不消耗摘要額度
python3 -m weekly.run_weekly --dry-run --no-summarize --count 2 --journals NEJM
```

目前 launchd 排程：

- 週一 03:00：`python3 -m weekly.run_weekly --no-discord --select-top 8`
  - 抓文獻、產生摘要與短評
  - 精選最值得評讀的文章，下載 PDF 並產生完整評讀
  - 渲染本週 HTML、更新 `docs/`，commit 並 push 到 GitHub
- 週一 08:00：`python3 -m weekly.notify_latest`
  - 讀取 `docs/_index.json` 最新週報
  - 發 Discord webhook 通知
- 每 15 分鐘：`python3 -m weekly.process_appraisal_requests`
  - 讀取週報 HTML 送出的「文獻評讀」請求
  - 下載 PDF、產生評讀 HTML、push 到 GitHub Pages
  - 用 Discord 發送該篇完整評讀連結

安裝或更新 launchd：

```bash
mkdir -p output/logs
cp scripts/com.pokai.weekly-journal.plist ~/Library/LaunchAgents/
cp scripts/com.pokai.weekly-journal-discord.plist ~/Library/LaunchAgents/
cp scripts/com.pokai.weekly-appraisal-requests.plist ~/Library/LaunchAgents/

launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.pokai.weekly-journal.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.pokai.weekly-journal-discord.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.pokai.weekly-appraisal-requests.plist
```

檢查狀態：

```bash
launchctl print gui/$(id -u)/com.pokai.weekly-journal
launchctl print gui/$(id -u)/com.pokai.weekly-journal-discord
launchctl print gui/$(id -u)/com.pokai.weekly-appraisal-requests
```

log：

```text
output/logs/weekly.out.log
output/logs/weekly.err.log
output/logs/weekly-discord.out.log
output/logs/weekly-discord.err.log
output/logs/weekly-appraisal-requests.out.log
output/logs/weekly-appraisal-requests.err.log
```

GitHub Pages 輸出：

```text
docs/
├── index.html
├── _index.json
└── YYYY-Wxx.html
```

正式網址：

```text
https://pkyang49.github.io/JournalFetcher/
```

## 週報 HTML 版面

每週 HTML(`docs/<YYYY>-Wxx.html`)由上而下分三區,文章不重複:

1. **本週精選評讀** — `weekly/select_articles.py` 系統自動挑選並完成的評讀。
2. **已評讀** — 使用者透過週報內「請求評讀」按鈕、由 `weekly/process_appraisal_requests.py` 完成的評讀(`selection_tags` 含 `manual_request`,卡片以綠色左邊框區分)。
3. **本週文章摘要** — 其餘文章的四句中文摘要 + 短評。

去重邏輯在 `weekly/render.py`(用 `selected_pmids` 排除上方兩區的 PMID)。1、3 區帶 👍/👎 回饋按鈕,2 區因為是使用者自己請求的所以不放。

## 週報回饋迴路

「本週精選評讀」與「本週文章摘要」每篇有 👍/👎 回饋按鈕。點擊後:

1. 回饋透過 Google Apps Script web app 寫入 Google 試算表（手機等任何裝置皆可）。
2. 下次 `run_weekly` 選文前，`weekly/sync_feedback.py` 以 POST body 傳送 `FEEDBACK_SYNC_TOKEN`，把回饋拉回 `data/interest_feedback.jsonl`。
3. `weekly/select_articles.py` 選文時據此調整權重，讓精選越來越貼近個人興趣。

同一支 Apps Script 也處理週報摘要區的「文獻評讀」請求。GitHub Pages
上的公開 HTML 只會開啟 Apps Script 驗證頁，不會直接寫入 request；輸入
server-side passphrase 正確後，Apps Script 才把請求寫入
`appraisal_requests` sheet。本機 `weekly/process_appraisal_requests.py` 會用
`FEEDBACK_SYNC_TOKEN` 拉回請求，確認該文章的 PMID 或 DOI 曾出現在本機週報
或已發布週報 HTML 後，才下載 PDF、產生完整評讀、更新 Pages，並用
Discord 發送評讀連結。

評讀請求識別規則：
- 優先使用 `week + pmid` 作為 request key。
- 若文章沒有 PMID，改用 `week + doi` 作為 request key。
- Apps Script 會允許 DOI-only request，但 DOI 必須符合 `10.xxxx/...` 格式。
- 本機同步端會從 `output/weekly/*/articles.json` 與 `docs/20xx-Wxx.html` 建立白名單，避免公開 HTML 觸發任意 DOI 評讀。

部署步驟見 `scripts/feedback_relay.gs`，並在 `.env` 填入
`FEEDBACK_ENDPOINT_URL`、`FEEDBACK_SYNC_TOKEN`。Apps Script 的「指令碼屬性」
需另外設定 `APPRAISAL_PASSPHRASE`，這組密語只存在 Apps Script server-side，
不要放進 `.env` 或 GitHub HTML。

安全性設計：
- HTML 內的 `FEEDBACK_ENDPOINT_URL` 是公開的；寫入端不放 token。
- 評讀請求需通過 Apps Script 的 `APPRAISAL_PASSPHRASE` 驗證；passphrase 不會出現在公開 HTML。
- Apps Script 會驗證 `week/pmid/doi/verdict` 格式、限制欄位長度，並以 `week + pmid` 或 `week + doi` upsert，避免同一篇被無限 append。
- Sheet 超過列數上限會自動裁切舊資料；本機同步端仍會用已知 PMID/DOI 白名單過濾。
- 同步讀取使用 POST body 傳送 `FEEDBACK_SYNC_TOKEN`，不使用 URL query。

測試評讀 request relay：

```bash
# 只確認本機能讀取 Apps Script request，不下載、不評讀、不發 Discord
python3 -m weekly.process_appraisal_requests --dry-run --limit 5

# 真跑一篇，但不 push、不發 Discord
python3 -m weekly.process_appraisal_requests --limit 1 --no-push --no-discord

# 完整流程：下載 PDF、評讀、更新 Pages、Discord 通知
python3 -m weekly.process_appraisal_requests --limit 1
```

若要測 DOI-only request，可用週報 HTML 內沒有 PMID 但有 DOI 的文章；本機 log
會顯示 `doi:<doi>`，而不是 `pmid:<pmid>`。

## 單篇 DOI 下載

```bash
python dlbydoi.py 10.1056/NEJMoa2301743
python dlbydoi.py https://doi.org/10.1001/jama.2026.7886/
```

PDF 下載策略：

1. 先嘗試 direct PDF URL、DOI redirect、Unpaywall、PMC。
2. Elsevier/JACC：Elsevier API 與 ScienceDirect browser fallback。
3. NEJM、OUP/EHJ、JAMA Network、Springer Sports Medicine：Playwright browser fallback。
4. Circulation：Primo/Ovid Playwright fallback。
5. MSSE：LWW tokenized PDF fallback。
6. Heart (BMJ)：一般路徑失敗後，走 ProQuest Playwright fallback。

`dlbydoi.py` 會正規化 DOI/URL，支援 `https://doi.org/...` 與常見貼上格式。

週報 HTML 每篇文章會提供：

- `複製 DOI`：貼到 Discord DOI Bot 頻道下載。
- `複製下載指令`：貼到 terminal 執行 `python dlbydoi.py <DOI>`。

## Discord DOI Bot

```bash
python journal_bot.py
```

Bot 啟動後，在指定頻道傳入 DOI、`https://doi.org/...`，或含 `citation_doi` 的期刊文章 URL，會自動下載 PDF 並回傳到頻道。

目前手動背景啟動方式：

```bash
mkdir -p output/logs
nohup /Library/Frameworks/Python.framework/Versions/3.13/Resources/Python.app/Contents/MacOS/Python \
  /Users/pokai/JournalFetcher/journal_bot.py \
  >> /Users/pokai/JournalFetcher/output/logs/journal_bot.out.log \
  2>> /Users/pokai/JournalFetcher/output/logs/journal_bot.err.log &
```

檢查 process：

```bash
ps aux | rg journal_bot.py
```

log：

```text
output/journal_bot.log
output/logs/journal_bot.out.log
output/logs/journal_bot.err.log
```

停止 bot：

```bash
pkill -f journal_bot.py
```

## 文獻評讀

使用Codex自動化摘要，搭配 `skills/literature-appraisal/SKILL.md` 進行結構化評讀。


## 主要檔案

```text
fetch_journals.py                 # 互動式主程式
modules/pubmed.py                 # PubMed 查詢與 metadata 解析
modules/crossref.py               # Crossref API(BJSM 等 PubMed 涵蓋不佳的期刊)
modules/claude_exec.py            # claude -p 主要後端 + codex fallback dispatcher
modules/codex_model.py            # codex 模型解析與 env 限制
modules/summarize.py              # 三句摘要(claude/codex 雙後端)
modules/selector.py               # 終端機選擇介面
modules/downloader.py             # PDF 下載
dlbydoi.py                        # 單篇 DOI/URL 下載
journal_bot.py                    # Discord DOI 下載 bot

weekly/run_weekly.py              # 每週週報主流程
weekly/summarize_weekly.py        # 週報摘要與短評(claude/codex 雙後端)
weekly/select_articles.py         # 精選值得評讀的文章
weekly/appraise_selected.py       # 精選文章完整評讀(雙後端 + 1.5M 字元 backstop)
weekly/sync_feedback.py           # 同步週報回饋
weekly/process_appraisal_requests.py  # 處理 HTML 手動評讀請求
weekly/render.py                  # HTML 渲染與 index 維護(三區版面)
weekly/publish.py                 # git push 與 Discord webhook
weekly/notify_latest.py           # 只發最新週報 Discord 通知
weekly/templates/                 # Jinja2 templates

skills/literature-appraisal/      # 文獻評讀 Skill v3.3(SKILL.md + style guide + references/)
scripts/*.plist                   # launchd 排程
scripts/feedback_relay.gs         # 回饋中繼 Apps Script
docs/                             # GitHub Pages 輸出
```

## 需求

- Python 3.10+
- **Claude Code CLI**(主要後端,需 keychain 已登入 Claude 訂閱;不需要 `ANTHROPIC_API_KEY`)
- **Codex CLI**(fallback,需 `codex login`,顯示 `Logged in using ChatGPT`)
- 機構內網或可存取期刊 PDF 的網路環境
- GitHub Pages:`main` branch 的 `/docs`
- Discord webhook(每週通知選用)
