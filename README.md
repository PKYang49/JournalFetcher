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
```

摘要功能使用 Codex CLI：

```bash
codex --version
codex login status
```

`codex login status` 應顯示 `Logged in using ChatGPT`。摘要流程呼叫 `codex exec`，不再依賴 `claude -p`。

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

# 測試版面，不消耗摘要額度
python3 -m weekly.run_weekly --dry-run --no-summarize --count 2 --journals NEJM
```

目前 launchd 排程：

- 週一 03:00：`python3 -m weekly.run_weekly --no-discord`
  - 產生本週 HTML
  - 更新 `docs/`
  - commit 並 push 到 GitHub
- 週一 08:00：`python3 -m weekly.notify_latest`
  - 讀取 `docs/_index.json` 最新週報
  - 發 Discord webhook 通知

安裝或更新 launchd：

```bash
mkdir -p output/logs
cp scripts/com.pokai.weekly-journal.plist ~/Library/LaunchAgents/
cp scripts/com.pokai.weekly-journal-discord.plist ~/Library/LaunchAgents/

launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.pokai.weekly-journal.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.pokai.weekly-journal-discord.plist
```

檢查狀態：

```bash
launchctl print gui/$(id -u)/com.pokai.weekly-journal
launchctl print gui/$(id -u)/com.pokai.weekly-journal-discord
```

log：

```text
output/logs/weekly.out.log
output/logs/weekly.err.log
output/logs/weekly-discord.out.log
output/logs/weekly-discord.err.log
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

下載後手動上傳 PDF 至 Claude.ai，搭配 `skills/literature-appraisal-SKILL.md` 進行結構化評讀。

## 主要檔案

```text
fetch_journals.py                 # 互動式主程式
modules/pubmed.py                 # PubMed 查詢與 metadata 解析
modules/summarize.py              # 三句摘要
modules/selector.py               # 終端機選擇介面
modules/downloader.py             # PDF 下載
dlbydoi.py                        # 單篇 DOI/URL 下載
journal_bot.py                    # Discord DOI 下載 bot

weekly/run_weekly.py              # 每週週報主流程
weekly/summarize_weekly.py        # 週報摘要與短評
weekly/render.py                  # HTML 渲染與 index 維護
weekly/publish.py                 # git push 與 Discord webhook
weekly/notify_latest.py           # 只發最新週報 Discord 通知
weekly/templates/                 # Jinja2 templates

scripts/*.plist                   # launchd 排程
docs/                             # GitHub Pages 輸出
```

## 需求

- Python 3.10+
- Codex CLI（`codex exec`，需 `codex login`）
- 機構內網或可存取期刊 PDF 的網路環境
- GitHub Pages：`main` branch 的 `/docs`
- Discord webhook（每週通知選用）
