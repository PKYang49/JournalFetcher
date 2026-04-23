# JournalFetcher

自動抓取頂尖醫學期刊最新文章、生成繁體中文摘要、下載 PDF，並透過 Discord Bot 支援 DOI 觸發下載。

## 功能

- **Phase 1**：從 PubMed E-utilities API 抓取最新文章（PMID、標題、摘要、DOI、作者）
- **Phase 2a**：呼叫 Claude API 生成三句繁體中文摘要
- **Phase 2b**：Terminal checkbox 介面勾選感興趣的文章
- **Phase 3**：機構內網 IP 授權直接下載 PDF（含多重 fallback 策略）
- **Discord Bot**：傳 DOI 或 URL 給 Bot，自動下載 PDF 並回傳到頻道

## 支援期刊

NEJM、Lancet、JAMA、JACC、EHJ、EuroIntervention、Circulation

## 安裝

```bash
pip install -r requirements.txt
playwright install chromium
```

建立 `.env`：

```env
ANTHROPIC_API_KEY=sk-ant-...
# Discord Bot（選用）
DISCORD_BOT_TOKEN=...
DISCORD_ALLOWED_USER_IDS=123456789
DISCORD_CHANNEL_ID=987654321
BOT_OUTPUT_DIR=~/GoogleDrive/papers
BOT_SEND_PDF=true
```

## 用法

### 主程式

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

### 單篇 DOI 下載

```bash
python dlbydoi.py 10.1056/NEJMoa2301743
```

### Discord Bot

```bash
python journal_bot.py
```

Bot 啟動後，在指定頻道傳入 DOI（`10.xxxx/...`）或完整 URL，Bot 自動下載並回傳 PDF。

## 輸出結構

```
output/
├── YYYY-MM-DD/
│   ├── errors.log
│   └── download_failures.log
└── pdfs/
    └── {pmid}_{first_author}_{year}.pdf
```

## PDF 下載策略

1. Playwright 抓機構授權頁面（NEJM、OUP 系列優先）
2. DOI redirect → 期刊頁面解析 PDF 連結
3. Unpaywall API 取 open-access PDF

## 文獻評讀

下載後手動上傳 PDF 至 Claude.ai，搭配 `skills/literature-appraisal-SKILL.md` 進行結構化評讀。

## 需求

- Python 3.10+
- Anthropic API key
- 機構內網（IP 授權 PDF 下載）
