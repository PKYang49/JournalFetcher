# JournalFetcher

醫學文獻追蹤與週報自動化工具。它會定期抓取主要醫學期刊文章，產生繁體中文摘要、個人化短評、精選文獻評讀，並發布成 GitHub Pages 週報。

正式頁面：

```text
https://pkyang49.github.io/JournalFetcher/
```

## 功能

- **每週週報**：每週自動抓取新文章，摘要、排序、渲染 HTML，並推送到 GitHub Pages。
- **精選評讀**：每週自選 5 篇值得深入閱讀的文章，下載 PDF 後產生結構化文獻評讀。
- **手動評讀請求**：週報內可對單篇文章送出評讀請求，本機排程會定期處理。
- **回饋迴路**：週報上的正負回饋會回流到選文權重，讓精選逐週貼近個人興趣。
- **互動式下載**：可從終端機瀏覽文章、勾選並下載 PDF。
- **Discord DOI Bot**：丟 DOI 或期刊文章 URL 到指定頻道，自動抓 PDF 回傳。

## 後端策略

摘要與評讀採雙後端：

- **主要後端**：`claude -p`
  - 摘要：`claude-haiku-4-5`
  - 評讀：`claude-opus-4-6`
  - 使用 Claude CLI keychain OAuth，不需要 `ANTHROPIC_API_KEY`
- **Fallback**：`codex exec`
  - 摘要：`gpt-5.4`
  - 評讀：`gpt-5.5`
  - 當 Claude 回報 rate-limit、credit、quota、monthly limit、Agent SDK limit 等訊號時自動切換

切換邏輯集中在 `modules/claude_exec.py`。hit limit 後，本次 process 後續呼叫都走 Codex；下一次排程會重新先嘗試 Claude。

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

## 快速開始

```bash
pip install -r requirements.txt
playwright install chromium
```

確認 CLI 登入：

```bash
claude --version
codex --version
codex login status
```

建立 `.env`：

```env
# Weekly Discord webhook
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

# Feedback relay / appraisal requests
FEEDBACK_ENDPOINT_URL=https://script.google.com/macros/s/.../exec
FEEDBACK_SYNC_TOKEN=自訂亂碼

# Discord DOI Bot
DISCORD_BOT_TOKEN=...
DISCORD_ALLOWED_USER_IDS=123456789
DISCORD_CHANNEL_ID=987654321
BOT_OUTPUT_DIR=~/GoogleDrive/papers
BOT_SEND_PDF=true
```

## 常用指令

產生週報：

```bash
# 完整流程：抓文獻、摘要、渲染 HTML、commit/push、發 Discord
python3 -m weekly.run_weekly

# 只產生 HTML，不 push、不發 Discord
python3 -m weekly.run_weekly --dry-run

# 週報 + 5 篇精選評讀，但不發 Discord
python3 -m weekly.run_weekly --no-discord --select-top 5

# 測試版面，不消耗摘要額度
python3 -m weekly.run_weekly --dry-run --no-summarize --count 2 --journals NEJM
```

互動式抓取與下載：

```bash
# 抓全部期刊，摘要後勾選下載
python fetch_journals.py

# 只抓特定期刊
python fetch_journals.py --journals NEJM JAMA JACC

# 調整篇數與時間範圍
python fetch_journals.py --count 15 --days 14
```

單篇 DOI 下載：

```bash
python dlbydoi.py 10.1056/NEJMoa2301743
python dlbydoi.py https://doi.org/10.1001/jama.2026.7886/
```

處理手動評讀請求：

```bash
# 只檢查 request，不下載、不評讀、不推送
python3 -m weekly.process_appraisal_requests --dry-run --limit 5

# 真跑一篇，但不 push、不發 Discord
python3 -m weekly.process_appraisal_requests --limit 1 --no-push --no-discord
```

## 目前排程

```text
週一 03:00   python3 -m weekly.run_weekly --no-discord --select-top 5
週一 08:00   python3 -m weekly.notify_latest
每 15 分鐘   python3 -m weekly.process_appraisal_requests
```

launchd plist 放在 `scripts/*.plist`。目前 weekly 主排程精選評讀 5 篇，先用 Claude 額度，hit limit 後自動 fallback 到 Codex。

## 週報版面

每週 HTML 位於 `docs/<YYYY>-Wxx.html`，由上而下分三區：

1. **本週精選評讀**：系統自選並完成的完整評讀。
2. **已評讀**：使用者手動請求後完成的評讀。
3. **本週文章摘要**：其餘文章的中文摘要與短評。

`docs/index.html` 與 `docs/_index.json` 由 pipeline 自動更新，用於 GitHub Pages 首頁與 Discord 通知。

## 設定

| 變數 | 用途 | 預設 |
|---|---|---|
| `JOURNAL_FETCHER_CLAUDE_SUMMARY_MODEL` | Claude 摘要模型 | `claude-haiku-4-5` |
| `JOURNAL_FETCHER_CLAUDE_APPRAISAL_MODEL` | Claude 評讀模型 | `claude-opus-4-6` |
| `JOURNAL_FETCHER_CODEX_MODEL` | Codex 摘要 fallback | Codex default 降一個 minor |
| `JOURNAL_FETCHER_APPRAISAL_MODEL` | Codex 評讀 fallback | Codex default / latest |
| `JOURNAL_FETCHER_APPRAISAL_CHAR_BACKSTOP` | 評讀文章字元上限 | `1500000` |
| `DISCORD_WEBHOOK_URL` | 週報與評讀完成通知 | 無 |
| `FEEDBACK_ENDPOINT_URL` | Apps Script 回饋中繼 | 無 |
| `FEEDBACK_SYNC_TOKEN` | 本機同步回饋與 request 的 token | 無 |

## 主要檔案

```text
fetch_journals.py                 # 互動式抓取與下載
dlbydoi.py                        # 單篇 DOI/URL 下載
journal_bot.py                    # Discord DOI 下載 bot

modules/claude_exec.py            # Claude primary + Codex fallback dispatcher
modules/summarize.py              # 互動式摘要
modules/downloader.py             # PDF 下載策略

weekly/run_weekly.py              # 每週週報主流程
weekly/summarize_weekly.py        # 週報摘要與短評
weekly/select_articles.py         # 精選文章排序
weekly/appraise_selected.py       # 精選文章完整評讀
weekly/process_appraisal_requests.py  # 手動評讀請求
weekly/render.py                  # HTML 渲染與 index 維護
weekly/publish.py                 # GitHub Pages push 與 Discord webhook

skills/literature-appraisal/      # 文獻評讀 skill 與 style guide
scripts/*.plist                   # launchd 排程
scripts/feedback_relay.gs         # Google Apps Script 回饋中繼
docs/                             # GitHub Pages 輸出
```

## 需求

- Python 3.10+
- Claude Code CLI，已登入 Claude 訂閱或 2026-06-15 後的 Agent SDK 額度
- Codex CLI，已 `codex login`
- Playwright Chromium
- 可存取期刊 PDF 的網路環境
- GitHub Pages：`main` branch 的 `/docs`
