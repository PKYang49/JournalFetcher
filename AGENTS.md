# Journal Fetcher — Agent 共用指南

> 這份檔案是 Claude Code 與 Codex 共用的專案規範。本機的 `CLAUDE.md` 是指向本檔的 symlink，所以兩個工具讀到的內容完全一致。

## 專案目標

- **互動模式**：抓取數本期刊最新文章列表，生成繁體中文摘要，terminal 勾選後下載 PDF（評讀由使用者另行處理）。
- **週報模式**：每週一自動抓 13 本期刊、生成四句中文摘要，渲染成 HTML 部署到 Cloudflare Static Assets，並推播連結到 Discord。每週自選 N 篇進行完整文獻評讀；使用者可在 HTML 上「請求評讀」追加個別文章。GitHub repository 與自動 push 僅保留為版本紀錄，不提供正式網站。

## 執行環境

- Python 3.10+。
- 本機在機構內網：PDF 下載**無需 VPN 或 cookie**，直接用 DOI 打期刊網站即可。
- 設定全部走 `.env`，**不 hardcode**。
- **不需要 Anthropic API key、也不需要 OpenAI API key** — 兩個 backend 都靠 CLI 訂閱登入：
  - claude → macOS keychain OAuth（Agent SDK 月額度）
  - codex → `~/.codex/config.toml` + `auth.json`

## 期刊清單（13 本）

`modules/pubmed.py::JOURNAL_QUERIES`

| Key | PubMed Journal Name |
|---|---|
| NEJM | N Engl J Med |
| Lancet | Lancet |
| JAMA | JAMA |
| JACC | J Am Coll Cardiol |
| EHJ | Eur Heart J |
| EuroIntervention | EuroIntervention |
| Circulation | Circulation |
| BJSM | Br J Sports Med（用 90–100d window，crossref 抓）|
| MSSE | Med Sci Sports Exerc |
| SportsMed | Sports Med |
| JAP | J Appl Physiol (1985) |
| JAMACardio | JAMA Cardiol |
| Heart | Heart（用 90–97d window）|

`JOURNAL_DEFAULT_WINDOW` 為部分期刊指定歷史抓取區間（覆寫預設 7d）。

## 雙後端契約（重要）

```
summarize_one / appraise_pdf / classify
        │
        ▼
_run_<role>_prompt (各檔自己的 wrapper)
        │
        ▼
modules/claude_exec.py :: try_claude_or_fallback
        │
   ┌────┴────┐
   ▼         ▼
 claude -p   codex exec   ← Codex 是這條路徑
 (Haiku 4.5  (GPT 5.4 摘要
  Opus 5)     GPT 5.6 評讀)
```

**切換規則**：

- `ClaudeLimitError`（錯誤訊息含 `rate_limit` / `billing` / `credit` / `monthly limit` / `usage limit` / `quota` / `agent sdk` 等子字串）→ **永久切換**：設 `_session["claude_exhausted"] = True`，本 process 之後所有呼叫直接走 codex。下個 process（下次 run）重新從 claude 嘗試。
- 其他 `ClaudeError`（timeout、parse、暫時性 5xx）→ **單次 fallback**：本次走 codex，後續仍會繼續試 claude。
- 設計刻意：使用者明確**不開 usage credits**，所以額度歸零時 claude 端會直接回錯，fallback 是唯一信號。**不要在程式內自己算用量**。
- **例外：完整評讀預設 claude-only**（`JOURNAL_FETCHER_CLAUDE_ONLY=1`），撞限不 fallback codex 而是等 ~5h 視窗重置續跑。摘要 / 短評 / classify 仍照上面的 fallback 規則。詳見 [launchd 排程 → 評讀的用量上限策略](#評讀的用量上限策略claude-only-opus--5h-續跑)。

## 模型對照

| 角色 | Claude（主） | Codex（fallback） | 覆寫 env |
|---|---|---|---|
| 摘要 / 短評 / classify | `claude-haiku-4-5` | `gpt-5.4` | `JOURNAL_FETCHER_CLAUDE_SUMMARY_MODEL` / `JOURNAL_FETCHER_CODEX_MODEL` |
| 完整評讀 | `claude-opus-5` | `gpt-5.6` | `JOURNAL_FETCHER_CLAUDE_APPRAISAL_MODEL` / `JOURNAL_FETCHER_APPRAISAL_MODEL` |

**評讀用最新 Opus 5**（2026-07-25 從 Opus 4.8 升上來）：`claude-opus-5` 已實測可透過 `claude -p --model` 訂閱 CLI 服務（`modelUsage` 回報 `claude-opus-5`、非 alias；亂填 `claude-opus-99` 會 `is_error:true`）。評讀走 CLI 訂閱額度、非 raw API，故只需改 model 字串、無 API 參數變更（`modules/claude_exec.py::FALLBACK_CLAUDE_APPRAISAL_MODEL`）。⚠️ Opus 5 的正式定價與 tokenizer 尚未查證（單次小 prompt 實測成本與 4.8 同量級，但不足以當定價依據）；先前 4.8 那段「$5/$25、新 tokenizer +35% tokens」的成本估算未在 Opus 5 上重新驗證，月成本與撞限頻率需觀察首週實跑再校正。

## 一個一定不能踩的雷：launchd 環境的 claude auth

`modules/claude_exec.py::claude_env()` 做兩件事：

1. 移除 `ANTHROPIC_API_KEY`（強制走 keychain OAuth = 訂閱計費，不是 raw API）。
2. **若 `USER` 或 `LOGNAME` 沒設，從目前 uid 反查並補上**。

launchd 啟動的 process env 幾乎是空的（只有 plist 裡寫的 PATH/HOME）。macOS Keychain 需要 `USER` / `LOGNAME` 識別 owner，否則 `claude -p` 30ms 就回 `"Not logged in · Please run /login"`，整條 claude 路徑全部失效、100% fallback 到 codex。

**修這段時務必保留 `USER` / `LOGNAME` 的 backfill 邏輯**，不然週日 08:50 launchd 跑出來的就只剩 codex。

## 檔案結構

```
JournalFetcher/
├── AGENTS.md                   # 本文件（兩個 agent 共用）
├── CLAUDE.md                   # symlink → AGENTS.md（本機，.gitignore）
├── .env                        # DISCORD_WEBHOOK_URL / FEEDBACK_ENDPOINT_URL 等
├── fetch_journals.py           # 互動模式入口
├── modules/
│   ├── pubmed.py               # PubMed E-utilities API；JOURNAL_QUERIES、pub_type 分類
│   ├── crossref.py             # BJSM 走 Crossref（PubMed 索引慢）
│   ├── claude_exec.py          # claude -p dispatcher + codex fallback；prompt cache、WebSearch、Read 工具
│   ├── codex_model.py          # codex model 解析 + env allowlist
│   ├── summarize.py            # 三句中文摘要（互動模式）
│   ├── selector.py             # terminal checkbox 勾選
│   ├── downloader.py           # PDF 下載（DOI redirect、Unpaywall 備援）
│   └── ego_browser.py          # ego lite adapter：真實瀏覽器 profile 抓 cookie/challenge 擋住的 PDF
├── weekly/
│   ├── run_weekly.py           # 週報主程式（launchd 觸發）
│   ├── summarize_weekly.py     # 四句中文摘要
│   ├── classify_article.py     # 文章類型分類：heuristic-first → Haiku fallback → default
│   ├── select_articles.py      # 自選每週 N 篇（讀 interest_feedback.jsonl）
│   ├── appraise_selected.py    # 完整評讀；prompt cache + WebSearch + JAMA references
│   ├── process_appraisal_requests.py  # on-demand「請求評讀」worker（每 15 分鐘）
│   ├── sync_feedback.py        # 從 Cloudflare Worker + D1 同步使用者 👍/👎
│   ├── notify_latest.py        # Discord 推播（與 run_weekly 拆開排程）
│   ├── render.py               # Jinja2 HTML 渲染
│   ├── publish.py              # GitHub 備份 push + Cloudflare deploy + Discord
│   └── templates/{weekly.html,index.html}
├── docs/                       # Cloudflare 靜態資產；同步提交到 GitHub 留存
├── skills/literature-appraisal/
│   ├── SKILL.md                # v3.6（全域規則 + SECTION-0 + 路由表）
│   ├── fragments/              # 每 route 一份；appraise_selected 按 route 載入
│   ├── references/             # 本機 local-only（.gitignore）
│   │   ├── output_quality_style_guide.md  # 唯一 tracked
│   │   ├── index.md                       # JAMA 檔案索引
│   │   └── jamaevidence/                  # 18 份 JAMA Users' Guides + WhatIf 因果推論教科書
│   └── agents/openai.yaml      # codex agent 設定（如有）
├── data/
│   ├── interest_feedback.jsonl # 使用者 👍/👎 歷史
│   └── appraisal_requests_processed.jsonl  # on-demand 請求 state
├── output/
│   ├── pdfs/                   # 互動模式下載
│   ├── weekly/<week>/          # articles.json / selected_articles.json / pdfs/ / appraisals/
│   └── logs/                   # launchd 輸出
└── scripts/
    ├── com.pokai.weekly-journal.plist  # launchd 設定檔
    └── feedback_relay.gs               # 舊版 Apps Script relay（不再是正式服務）
```

## 互動模式 Phase

### Phase 1 — 抓取文章列表（`modules/pubmed.py`）
- 工具：PubMed E-utilities（免費、無需 API key）。`&email=` 帶上避免被限速。
- 端點：`esearch.fcgi`（找 PMID）+ `efetch.fcgi`（取 metadata）。
- 搜尋條件：`<journal> AND "last 30 days"[dp] AND hasabstract[text]`，按 pub date 排序。
- 輸出 dict：`{pmid, title, abstract, doi, journal, authors, year, volume, issue, pages, pub_type, pub_types}`。

### Phase 2a — 中文摘要（`modules/summarize.py`）
- 主要：`claude -p --model claude-haiku-4-5 --output-format json`，prompt 透過 stdin，`cwd=tmpdir`。
- Fallback：`codex exec` GPT 5.4（整個 process 永久切換）。
- System prompt：三句話（研究設計/族群、主要發現、臨床意義），≤ 120 字。循序執行（避免 session 互搶）。

### Phase 2b — 勾選介面（`modules/selector.py`）
- `questionary` checkbox，顯示「[期刊] 標題 + 摘要」。

### Phase 3 — PDF 下載（`modules/downloader.py`）
- 機構內網 IP 授權，直接 GET。
- 策略：DOI redirect → 期刊頁面找 PDF link；備援 Unpaywall API。
- 儲存：`output/pdfs/{pmid}_{first_author}_{year}.pdf`。
- 失敗記到 `output/download_failures.log`，不中斷流程。
- 已知 quirk：OUP（EHJ）下載要走 `page.request.get` 不是 `page.goto`；NEJM 單 DOI 場景 Playwright + homepage warmup 優於 nodriver。

#### ego lite 路徑（`modules/ego_browser.py`，2026-07-24）

需要真實瀏覽器 profile 的下載改走 **ego lite**（`ego-browser` CLI）。相對 Claude-in-Chrome 的關鍵差異：ego 給 **CLI + Node runtime**，所以能被 `subprocess` 呼叫、能塞進 launchd 無人值守，且 `fs.writeFileSync` 直接落檔（不需要 `ovid_pdf_receiver.py` 那種 local receiver）。

核心原語 `fetch_pdf_via_ego(nav_url, *, link_js, pdf_url_js, wait_for_url)`：navigate → （選配）在頁面內求值 `link_js` 拿下一跳 URL 並導過去 → 在最終頁 `fetch(pdf_url_js)` → base64 過 CDP → Node 落檔。**PDF 的簽章 URL 幾乎都綁 browser session**（Elsevier `X-Amz-Expires=300`、Silverchair token），從 Python 抓會 403，所以 bytes 一定要從那個分頁取。

| 出版社 | 函式 | 機制 | 需要人工？ |
|---|---|---|---|
| Ovid（MSSE） | `_try_ego_ovid` | `/fulltext/` 取席 → `/pdf/` | 否（cookie 有效時） |
| Elsevier（JACC/Lancet） | `_try_ego_sciencedirect` | PII → 文章頁抓 `pdfft` → 導過去解 challenge | **是，撞 Turnstile 時** |
| Silverchair（JAMA/OUP）、Springer、BMJ、NEJM | `_try_ego_citation_pdf` | `citation_pdf_url`（NEJM 用 `/doi/pdf/` anchor）→ 導過去 | 否 |
| AHA / Atypon（Circulation） | `_try_ego_aha` | 文章頁讀 `/doi/pdf/` anchor → **同頁 fetch，不導過去** | 否 |

- **一律是新增 tier，不取代既有路徑**：ego 回 `None` 就落回原本的 Playwright / nodriver cascade，行為不變。`JOURNAL_FETCHER_EGO=0` 或 CLI 不存在也一樣。
- **Elsevier 無法無人值守**：Cloudflare Turnstile 過關後約 **1.5–2 小時**就復發，agent 的 CDP 點擊無效、必須人點。但既有 nodriver 路徑同樣要人（`_nodriver_wait_for_cloudflare` 會提示點擊），而且 `uc.start()` 每次開拋棄式 profile、每批都重撞；ego 的 clearance 留在真實 profile，窗口內跨文章跨 run 都有效。定位是「機會主義 tier」。
- **其餘出版社不需 captcha**：走機構 IP 授權，ego 的真實 profile IP 對就過 → 這些**可以無人值守**。
- 每篇約 13–18 秒。`cliLog` 寫的是 **stderr** 不是 stdout（接 subprocess 時要讀兩個串流）。
- 觀察：Heart / BJSM 測試時在 DOI redirect（純 HTTP）就成功，沒用到 ego 也沒用到 ProQuest；ProQuest 路徑可能已是歷史包袱，但單次觀察不足以下結論。

- **MSSE 已從 LWW 改到 Ovid（~2026-07），走 ego lite 自動下載**：DOI 現在 redirect 到 `www.ovid.com/jnls/acsm-msse/...`，PDF 需要 **httpOnly entitlement cookie**；headless curl_cffi 抓不到（直打 `/pdf/` 會 bounce 回 `/fulltext/` 的 HTML），cookie-bridge 也不可行。**現行做法：`modules/ego_browser.py`**——`_try_ego_ovid` 用 `requests` 跟 doi.org redirect 拿到帶 slug 的 `/fulltext/` URL，再交給 `fetch_pdf_via_ego` 在 ego lite 內 navigate（**這步才會讓 session entitled**）→ 同源 `fetch(location.href.replace('/fulltext/','/pdf/'))` → Node `fs` 直接落檔。`download_pdf` 與 `dlbydoi.download_one` 的 `is_msse` 分支都走這條，`_try_lww_direct` 降為 legacy host 備援。
  - **UI 上不需處於登入態**：頁面 nav 顯示 Login、`signedIn: false` 照樣拿得到 PDF，關鍵是 profile 裡的 httpOnly cookie。**cookie 壽命未測**，所以 `deferred` 重試機制要保留。
  - **Ovid 3 concurrent seats**，每開一次 fulltext 佔一席：`_OVID_MIN_INTERVAL`（8s）節流，且 `fetch_pdf_via_ego` 在 `finally` 一定 `completeTaskSpace({keep:false})` 釋放席位。
  - `JOURNAL_FETCHER_EGO=0` 或 CLI 不存在 → 回 `None` 優雅降級；`download_failures.log` 會分「ego 不可用」與「ego 有跑但 Ovid 沒給」兩種訊息。
  - ego lite **沒有自啟機制**（不在登入項目，`com.citrolabs.EgoUpdater.wake` 只是更新檢查），重開機後是關著的。CLI 二進位含 `launch_application.mm` / `LSOpen`，且 launchd job 在 Aqua session，冷啟動應可自動拉起——但**未實測**。
  - 舊的 Claude-in-Chrome + `scripts/ovid_pdf_receiver.py`（127.0.0.1:8799）已不再需要，檔案保留當退路。細節見 memory `project_msse_ovid_download`。
- **on-demand 請求評讀的 MSSE 失敗改為可重試**：`process_appraisal_requests` 把 Ovid-gated 期刊（`JOURNAL_FETCHER_OVID_GATED_JOURNALS`，預設 `MSSE`）的 `pdf_failed` 記成 `deferred`（reason `ovid_auth_required`，每 24h 重試，上限 `OVID_AUTH_RETRY_MAX`=14）而非永久 `failed`，讓文章正式出版 / 補抓後能自動接上。

### Phase 4 — 文獻評讀（互動模式由使用者另行處理；週報模式見下）

## 週報模式（`weekly/`）

### 流程（`weekly/run_weekly.py`）

1. 每本期刊抓最新 10 篇（13 本約 130 篇）。
2. 對每篇 abstract 用 claude Haiku（fallback codex GPT 5.4）產生**四句**中文摘要：背景、方法、結果、結論。
3. （可選）`select_articles.select_top_articles` 自選 N 篇，下載 PDF，跑完整評讀。
4. 渲染 `docs/<YYYY>-W<week>.html`，三區由上而下：
   - **本週精選評讀**：系統自選並完成評讀
   - **已評讀**：使用者透過週報「請求評讀」按鈕、由 `process_appraisal_requests` 完成（`selection_tags` 含 `manual_request`）
   - **本週文章摘要**：其餘文章；render 用 `selected_pmids` 去重
5. 更新 `docs/index.html`（最新在頂端）。
6. `git add docs/ && commit && push` → GitHub 留存版本，再部署 Cloudflare Worker + Static Assets。
7. POST Discord webhook（embed 卡片 + 前 3 篇精華 + Cloudflare 正式週報連結）。

`appraisal_card` Jinja macro 重用三區卡片 markup；「已評讀」用 `.appraised-card` 綠色左邊框、不放 👍/👎。

### 評讀流程（`weekly/appraise_selected.py`）

完整管線：

```
PDF
  ↓ pymupdf4llm（主）/ markitdown（fallback）
  ↓ _strip_references（regex 砍掉 References 段，~30% 體積）
markdown
  ↓ classify_article.classify()
route ∈ {rct, observational, preclinical, sr, nma, cpg, consensus, narrative, diagnostic, default}
  ↓ _load_skill_for_route：SKILL.md + 對應 fragment
  ↓ _build_references_section(route)：列出該 route 對應的 JAMA Users' Guides 絕對路徑（cached）
  ↓ APPRAISAL_SYSTEM_PROMPT + APPRAISAL_USER_PROMPT
  ↓ claude -p Opus 5（--append-system-prompt-file + WebSearch + Read on references）
  ↓   fallback：codex GPT 5.6（CODEX_REFERENCE_INSTRUCTIONS 前置）
appraisal.md → render.publish_appraisals → docs/<pmid>.html → git push → Discord
```

關鍵機制：

- **Heuristic-first classify**：`classify_article._classify_by_heuristic` 看 title + journal + `pub_type`，命中（W22 實測 ~53%）就直接回，省掉 LLM 呼叫。命中失敗才打 Haiku；Haiku 不可用（claude exhausted / limit / 暫時錯誤）時 fallback 到 **codex GPT 5.4**（`_classify_with_codex`，與摘要同 backend 邏輯）；codex 也失敗才退 `default` 路由（載完整 SKILL）。`source ∈ {heuristic, haiku, codex, fallback}`。
- **Prompt cache**：SKILL + fragment + style guide + references catalog 全進 `--append-system-prompt-file`，claude 自動 1h ephemeral cache。同 route 第二篇以後 ~10% 價（cache_read）。
- **WebSearch**：appraisal 路徑預設開（`--tools "WebSearch,WebFetch" --allowedTools "..."`），讓模型實際執行 SECTION-0 的「外部搜尋」需求（作者、期刊 IF、editorial）。`JOURNAL_FETCHER_APPRAISAL_WEB_SEARCH=0` 可關。
- **外部專業意見全文擷取**：進模型前先跑 deterministic preflight（`weekly/editorial_lookup.py`）：PubMed linked comment / title query，加上 NEJM article page 的 `dc.Relation` metadata（PubMed 不一定掛 linked comment）。找到同期 editorial / comment DOI 後，程式先呼叫 `weekly/fetch_editorial.py <DOI>`（`dlbydoi.download_one` 機構內網下載 → 轉 markdown），再把 DOI、書目資料、全文 markdown 注入同一份 user prompt。Claude / Codex 都吃同一份 preflight 結果；模型不應再自行宣稱「未找到 editorial」。
  - **Bash 閘門**：headless 的 `--allowedTools "Bash(prefix:*)"` **無法**只限單一指令（實測非白名單指令照跑）；真正生效的是 `--tools` 可見性（全有全無）。故改用 `modules/bash_gate_hook.py` PreToolUse hook（claude_exec 動態產生臨時 `--settings`）：只放行 `fetch_editorial.py` 開頭的指令，其餘 deny → 模型拿到的是「只能跑這支」的 Bash，不是任意 shell。
  - codex path 也可接：評讀 fallback 若啟用 editorial helper，`codex exec` 從 `read-only` 改用 `workspace-write`，並把 `TMPDIR` 指到該次暫存目錄，讓 `dlbydoi.py` 可寫暫存 PDF / markdown；若期刊瀏覽器 session 需要更大權限，可用 `JOURNAL_FETCHER_CODEX_APPRAISAL_SANDBOX=danger-full-access` 覆寫。
  - 工具指令與 preflight 結果注入 **user prompt**（非 cached system prompt），避免破壞 prompt cache key。
  - `JOURNAL_FETCHER_APPRAISAL_EDITORIAL=0` 可關（離開機構內網時）。每次呼叫記到 `output/logs/editorial_fetch.log`（CALL/OK/FAIL）。
  - 已知限制：Elsevier（JACC / Lancet / EuroIntervention）editorial 走 Cloudflare 互動驗證，無人值守會抓失敗 → 退回 `[無法取得全文]`；JAMA / NEJM / OUP / Heart(ProQuest) 等有 Playwright 路徑的可成功。
- **JAMA references（v3.6）**：
  - claude path：`--add-dir <references_dir>` + `Read` 工具，模型按需 Read 對應方法學的 JAMA Users' Guides
  - codex path：一般用 `codex exec --sandbox read-only` 讀檔；若同次啟用 editorial/comment 全文擷取則改用 `workspace-write`，prompt 開頭注入 `CODEX_REFERENCE_INSTRUCTIONS` 明確告知可用 `cat` / `rg`
  - **Local-only**：`references/jamaevidence/` 跟 `references/index.md` 在 `.gitignore`，本機沒這目錄時 helper 自動 return ""（graceful degradation）
- **Article size backstop**：`ARTICLE_CHAR_BACKSTOP=1,500,000`（可用 `JOURNAL_FETCHER_APPRAISAL_CHAR_BACKSTOP` 覆寫）。一般論文（4–9 萬字元）與完整 ACC/AHA 指引（~1.05M 字元）都完整送進去。超過 → `ArticleTooLargeError` → `appraisal_status = "too_large"`，不產出。
- **Route cache**：`appraise_pdf` 把 classify 結果寫進 `article["appraisal_route"]`，後續 manual re-run 跳過 LLM。

### Discord 推播

- 用 webhook（不是 bot），單向推播。
- URL 存在 `.env` 的 `DISCORD_WEBHOOK_URL`。
- Embed：標題、各期刊文章數、前 3 篇亮點、完整週報連結。
- 週報生成（Sun 08:50）與 Discord 推播（Mon 08:00）拆開排程，讓 Cloudflare 部署完成；中間的時間差也吸收評讀撞限後的 5h 續跑。

### 回饋迴路（feedback loop）

- 週報 HTML 按鈕 → POST 到 Access 保護的 Cloudflare Worker `/api` → 寫進 D1。
- `run_weekly` 每次寫 `output/weekly/<week>/articles.json`（PMID 白名單）。
- `weekly/sync_feedback.py`：選文前從 D1 relay 取回資料，併進 `data/interest_feedback.jsonl`；用 articles.json + selected_articles.json 過濾垃圾資料；同一 `(week, pmid)` 以最新 `ts` 為準。
- `weekly/select_articles.py` 的選文 prompt 會讀 `interest_feedback.jsonl` 調整權重。
- 設定：`.env` 的 `FEEDBACK_ENDPOINT_URL`、`FEEDBACK_SYNC_TOKEN`。
- 舊版 Apps Script：`scripts/feedback_relay.gs`（僅保留遷移參考）。
- 關閉同步：`python -m weekly.run_weekly --no-sync-feedback`。

### On-demand「請求評讀」（`weekly/process_appraisal_requests.py`）

- launchd 每 15 分鐘跑一次。
- 從 Cloudflare Worker + D1 拉「請求評讀」清單。
- 對每筆：下載 PDF → `appraise_selected.appraise_pdf` → render 該篇 HTML → 更新該週的 weekly.html「已評讀」區 → Discord 推播。
- State：`data/appraisal_requests_processed.jsonl`。

## 重要 Constraints

### API
- PubMed E-utilities URL 帶 `&email=` 避免被限速。
- 摘要 / 評讀 / 分類**主要走 `claude -p`**，用完自動 fallback `codex exec`。兩邊額度各算各的。

### PDF 下載
- 機構內網 IP 授權直接 GET。
- User-Agent 設正常瀏覽器字串避免被擋。
- timeout 30 秒，失敗重試 2 次。

### 錯誤處理
- 每個 phase 獨立 try/except，單篇失敗不影響其他篇。
- 所有錯誤記到 `output/errors.log`。
- 週報模式：單篇摘要失敗顯示 `[摘要生成失敗]` 並繼續。

### 輸出
- 互動模式：PDF → `output/pdfs/`。
- 週報模式：HTML → `docs/`（commit 進 repo），歷史保留。

## 執行方式

```bash
# 安裝依賴
pip install -r requirements.txt

# 互動模式
python3 fetch_journals.py
python3 fetch_journals.py --journals NEJM JAMA

# 週報模式
python3 -m weekly.run_weekly                         # 跑當週 + push + Discord
python3 -m weekly.run_weekly --dry-run               # 只生成 HTML，不 push、不推播（⚠️ 仍會覆寫本機當週 docs/<週>.html）
python3 -m weekly.run_weekly --no-push               # 跳過 git push
python3 -m weekly.run_weekly --no-discord            # 跳過 Discord
python3 -m weekly.run_weekly --no-summarize          # 用佔位符（debug HTML layout）
# 安全驗證 render（不碰真實當週檔）：用不存在的假 week label 寫到拋棄式檔案
python3 -m weekly.run_weekly --dry-run --no-summarize --count 2 --journals NEJM --week 2099-W01
rm -f docs/2099-W01.html && git checkout HEAD -- docs/index.html docs/_index.json  # 清掉驗證產物
python3 -m weekly.run_weekly --select-top 5          # 自選 5 篇做完整評讀
python3 -m weekly.run_weekly --no-sync-feedback      # 跳過 feedback 同步
python3 -m weekly.run_weekly --journals NEJM Lancet --count 5 --days 14

# On-demand「請求評讀」worker
python3 -m weekly.process_appraisal_requests        # 預設一次最多 3 筆
python3 -m weekly.process_appraisal_requests --limit 0 --force   # 全跑、忽略 state

# 單篇手動評讀 CLI（重用 appraise_selected pipeline；一樣 render + push + Discord）
python3 -m weekly.request_appraisal --doi 10.1016/j.jacc.2026.01.020   # 抓 metadata→下載 PDF→評讀
python3 -m weekly.request_appraisal --pdf ~/Downloads/paper.pdf --doi 10.1016/...  # 指定本機 PDF，跳過下載
python3 -m weekly.request_appraisal --pdf ~/Downloads/paper.pdf --title "..."      # 純 PDF 無 DOI
python3 -m weekly.request_appraisal --doi <DOI> --no-push --no-discord --force     # debug / 重跑
python3 -m weekly.request_appraisal --doi <DOI> --backend codex                    # 指定用 codex（跳過 claude）
# --backend 預設 claude（claude-only Opus，撞限 exit 10、不 silent fallback codex）；--claude-only 為舊別名

# 分類 heuristic regression（不打 LLM）
python3 -m weekly.classify_article
```

## launchd 排程（現況）

```
Sun 08:50   python3 -m weekly.run_weekly --no-discord --select-top 5
Mon 08:00   python3 -m weekly.notify_latest                   # Discord 推播
每 15 分鐘   python3 -m weekly.process_appraisal_requests
```

- 週報主程式**週日 08:50** 起跑；評讀「撞限 → 等視窗 → 續跑」的 5h 緩衝到 Mon 08:00 推播前仍足夠。
- launchd 不會主動喚醒 Mac；**週日凌晨到週一**都要保持機器醒著（或合蓋接電源 Power Nap）。
- log：`output/logs/weekly.{out,err}.log` 等。

### 評讀的用量上限策略（claude-only Opus + 5h 續跑）

- **週報評讀預設 claude-only**：`run_weekly` 進評讀階段前設 `JOURNAL_FETCHER_CLAUDE_ONLY=1`（摘要階段仍維持 Haiku→codex fallback，不受影響），全部用 Opus 5，**不 fallback codex**。
- 撞 `ClaudeLimitError` 時 `appraise_with_resume` 同 process `time.sleep` 一個滾動視窗（預設 5h，`JOURNAL_FETCHER_APPRAISAL_RETRY_WAIT` 覆寫）後 reset exhausted flag、續跑沒評完的篇目（`appraise_pdf` 用 report 檔案存在判斷跳過已完成的，不重複燒 Opus）。最多 `JOURNAL_FETCHER_APPRAISAL_RETRY_MAX_CYCLES`（預設 5）輪，之後放棄剩餘篇目。
- 想退回舊的 codex fallback 行為：`run_weekly --appraise-allow-codex`。
- **on-demand「請求評讀」也 claude-only Opus**：`process_appraisal_requests` 開頭設同一 env。15 分鐘 worker 不能 in-process 睡 5h，所以撞限時寫 `status=deferred` + `retry_after`（now+5h）到 state，視窗未到就跳過、到了由後續 run 自動重試（`_state_blocks`）。

## 通知小工具

```bash
osascript -e 'display notification "完成：[任務名稱]" with title "Journal Fetcher"'
```

## 已知的設計決策與其理由

1. **不用 `--bare` 模式**：`--bare` 需要 `ANTHROPIC_API_KEY`（raw API 計費），違反「吃 Claude 訂閱 / Agent SDK 額度」的目的。接受 Claude Code base system prompt 的少量 token overhead。
2. **subprocess `cwd` 設為 `tmpdir`**：避免 `claude -p` 自動載入專案 CLAUDE.md / skills，造成不必要的 token 開銷。
3. **Prompt caching（v3.6 新接）**：用 `--append-system-prompt-file`，1h ephemeral cache。同 route 第二篇以後 cache_read（~10% 價）。`--exclude-dynamic-system-prompt-sections` 用來把 per-machine 段（cwd / git status）移到 user message，讓 cache key 跨次穩定。
4. **`process_appraisal_requests` 與 `run_weekly` 共用 `appraise_selected`**：兩條路徑共用 PDF markdown backstop 與 references 機制。Claude / Codex 額度由 dispatcher 自動分配，不再手動分流。
5. **訂閱優先於 batch**：使用者有現存 Claude / ChatGPT 訂閱，Batch API 是獨立 raw-API 計費，不採用。
6. **Heuristic-first classify**：用 PubMed 的 `pub_type`（Meta-Analysis、Practice Guideline、RCT、Editorial、Observational ...）+ title 規則，命中就跳過 LLM 呼叫。命中失敗 → Haiku；Haiku 不可用 → **codex GPT 5.4 fallback**（`_classify_with_codex`）；codex 也失敗才走 `default`。（先前曾移除 codex 這層、直接 default，後因無類型標籤的觀察性研究被誤判 default 而重新加回。）
7. **JAMA references 為 local-only**：`.gitignore` 排除 `references/*`（只破例追蹤 `output_quality_style_guide.md`）。Helper 自動偵測目錄是否存在；不存在就降級。原因：避免把 JAMA 版權內容放進 public repo。

## 接手後可能會被問到的事

- **launchd 跑完發現 claude path 全部失敗**：先看 `output/logs/weekly.err.log`，確認是否 `USER` / `LOGNAME` 缺失導致的 `Not logged in`。若是，檢查 `claude_env()` 是否還在補。
- **要不要把 `--select-top` 降回 3**：成本估算每月 ~$25–30（8 篇 Opus + 130 篇 Haiku）；若想塞進 Pro $20 Agent SDK 額度，降到 3 篇是其中一個槓桿。或反過來提到 8 篇但接受月超額。
- **參考文獻剝除已實作**：`_strip_references` 用 regex 砍 `^#{1,3} References?$` 之後到結尾。指引 / SR 跨期刊測過 OK；偶爾 PDF→md 把 References 轉成粗體無 heading 會漏砍 — 必要時再加 fallback regex。
- **6/15 之後**：`claude -p` 從訂閱用量改成 Agent SDK 月額度（$20 Pro / $100 Max 5x）；需要一次性 claim。程式邏輯不變 —— dispatcher 看到限額錯誤就 fallback。

## 開發習慣

- Python 3.10+，型別註記齊全，避免 flag parameter。
- 修改後 `python3 -m py_compile <file>` 起手；週報相關改動跑空殼驗證 render 時**務必帶假 week label**避免覆寫當週真檔：`python3 -m weekly.run_weekly --dry-run --no-summarize --count 2 --journals NEJM --week 2099-W01`，驗完 `rm -f docs/2099-W01.html && git checkout HEAD -- docs/index.html docs/_index.json`。
  - ⚠️ `--dry-run` 只跳過 git push / Discord，**仍會寫本機 `docs/<當週>.html`**。若不帶 `--week`，當天所屬 ISO 週的完整頁面（含所有評讀）會被 2 篇佔位符版本覆蓋。真檔在 git HEAD，誤覆寫時 `git checkout HEAD -- docs/<週>.html docs/index.html docs/_index.json` 還原。
- 分類改動跑 `python3 -m weekly.classify_article` 確認 regression 通過。
- Git commit 前確認沒留 debug code。Commit 訊息接 Co-Authored-By 自己（claude）/ codex 自己的格式。
- **不開 `ANTHROPIC_API_KEY`、不開 `OPENAI_API_KEY`** —— 雙後端都靠 CLI 訂閱登入。
- 本機在機構內網，PDF 下載直接 GET DOI，不需要 VPN / cookie。
