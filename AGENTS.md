# Journal Fetcher — Codex Project

## 專案目標
自動抓取 NEJM / Lancet / JAMA / JACC 最新一期文章列表，生成三句繁體中文摘要，
讓使用者在 terminal 勾選，勾選後下載 PDF，最後呼叫文獻判讀 Skill 輸出評讀報告。

## 執行環境
- 本機在機構內網，PDF 下載無需 VPN 或 cookie，直接用 DOI 打期刊網站即可
- Python 3.10+
- 所有設定放在 `.env`，不 hardcode

## 檔案結構
```
journal-fetcher/
├── AGENTS.md               # 本文件
├── .env                    # API keys（ANTHROPIC_API_KEY）
├── fetch_journals.py       # 主程式入口
├── modules/
│   ├── pubmed.py           # Phase 1：PubMed E-utilities API
│   ├── summarize.py        # Phase 2a：Codex API 生成中文摘要
│   ├── selector.py         # Phase 2b：terminal checkbox 勾選介面
│   ├── downloader.py       # Phase 3：PDF 下載
│   └── appraise.py         # Phase 4：文獻評讀（載入 Skill + 呼叫 Codex API）
├── skills/
│   └── literature-appraisal-SKILL.md   # 文獻判讀 Skill（已建好，直接複製進來）
├── output/
│   ├── pdfs/               # 下載的 PDF
│   └── reports/            # 評讀報告（Markdown）
└── requirements.txt
```

## 四個 Phase 說明

### Phase 1：抓取文章列表（modules/pubmed.py）
- 工具：PubMed E-utilities API（免費，無需 API key）
- 端點：
  - `esearch.fcgi`：搜尋最新一期文章的 PMID 列表
  - `efetch.fcgi`：批次取得每篇文章的 metadata（標題、摘要、作者、DOI）
- 搜尋策略：
  - NEJM：`"N Engl J Med"[Journal] AND "2026"[Date - Publication]`
  - Lancet：`"Lancet"[Journal] AND "2026"[Date - Publication]`
  - JAMA：`"JAMA"[Journal] AND "2026"[Date - Publication]`
  - JACC：`"J Am Coll Cardiol"[Journal] AND "2026"[Date - Publication]`
- 每本期刊取最新 20 篇（可調整）
- 輸出格式：List of dict，每篇包含 `{pmid, title, abstract, doi, journal, authors}`

### Phase 2a：生成中文摘要（modules/summarize.py）
- 呼叫 Anthropic API（Codex-sonnet-4-20250514）
- System prompt：「你是醫學文獻摘要助手。用繁體中文，以三句話摘要這篇文章：
  第一句說研究設計和族群，第二句說主要發現和數字，第三句說臨床意義。
  不超過 120 字，不使用條列，不加標題。」
- 輸入：英文 abstract
- 輸出：三句繁體中文摘要字串
- 批次處理：並發呼叫（asyncio + aiohttp），控制 concurrency ≤ 5

### Phase 2b：勾選介面（modules/selector.py）
- 套件：`questionary`（checkbox 模式）
- 顯示格式：
  ```
  [期刊] 標題
  摘要（三句中文）
  ```
- 支援上下鍵 + 空白鍵勾選，Enter 確認
- 回傳勾選的文章 list

### Phase 3：PDF 下載（modules/downloader.py）
- 機構內網直接下載，無需驗證
- 下載策略（依序嘗試）：
  1. DOI redirect：`https://doi.org/{doi}` → 跟隨 redirect 到期刊頁面，
     再找 PDF 連結（`.pdf` 或含 `pdf` 的 link）
  2. Unpaywall API：`https://api.unpaywall.org/v2/{doi}?email=your@email.com`
     → 取 `best_oa_location.url_for_pdf`
- 儲存路徑：`output/pdfs/{pmid}_{first_author}_{year}.pdf`
- 失敗時記錄到 `output/download_failures.log`，不中斷流程

### Phase 4：文獻評讀（modules/appraise.py）
- 載入 `skills/literature-appraisal-SKILL.md` 作為 system prompt
- 將 PDF 轉為 base64 傳入 Codex API（vision/document 模式）
- model：`Codex-sonnet-4-20250514`，max_tokens：8000
- 輸出儲存至 `output/reports/{pmid}_{first_author}_{year}_appraisal.md`
- 每篇評讀前先顯示進度：`正在評讀 [X/N]：{標題}`

## 重要 Constraints

### API 相關
- Anthropic API key 從 `.env` 讀取（`ANTHROPIC_API_KEY`）
- PubMed E-utilities 請在 URL 加上 `&email=your@email.com`（避免被限速）
- Anthropic API rate limit：summarize 階段並發 ≤ 5；appraise 階段循序執行（PDF 大）

### PDF 下載
- 機構內網，IP 授權，直接 GET
- User-Agent 設為正常瀏覽器字串，避免被期刊網站擋
- timeout=30 秒，失敗重試 2 次

### 錯誤處理
- 每個 phase 獨立 try/except，單篇失敗不影響其他篇
- 所有錯誤記錄到 `output/errors.log`
- Phase 3 下載失敗的文章，Phase 4 跳過並標記 `[PDF 下載失敗，無法評讀]`

### 輸出
- 評讀報告為 Markdown 格式，按 SKILL.md 的 SECTION-0 + SKILL-A/B 結構輸出
- 每次執行結果存到 `output/runs/{YYYY-MM-DD}/` 資料夾，不覆蓋歷史紀錄

## 執行方式
```bash
# 安裝依賴
pip install -r requirements.txt

# 執行主程式
python fetch_journals.py

# 只跑特定期刊
python fetch_journals.py --journals NEJM JAMA

# 只抓不評讀（適合快速瀏覽）
python fetch_journals.py --no-appraise

# 從已下載的 PDF 重新評讀
python fetch_journals.py --reappraise output/pdfs/
```

## requirements.txt 內容
```
anthropic>=0.40.0
aiohttp>=3.9.0
questionary>=2.0.0
python-dotenv>=1.0.0
requests>=2.31.0
```

osascript -e 'display notification "完成：[任務名稱]" with title "Codex"'
