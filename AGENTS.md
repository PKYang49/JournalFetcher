# Journal Fetcher — Codex 交班文件

> Codex 接手時讀這份。專案的標準參考是 **`CLAUDE.md`**(隨程式碼一起維護的事實來源);本檔補充交班時的狀態、雙後端架構、最近的設計決策與待辦,讓 Codex 能直接接上。

## 一句話狀態

雙後端 pipeline:`claude -p` 為主、`codex exec` 為 fallback。Codex 在本專案的角色是**後備備案**,當 Claude 訂閱的 Agent SDK 月額度撞限時自動接手。

## 必讀順序

1. `CLAUDE.md` — 期刊清單、env、檔案結構、流程、constraints,都在裡面且是最新的。
2. `weekly/run_weekly.py` — 週報 pipeline 入口,理解整條流程從這裡。
3. `modules/claude_exec.py` — dispatcher 與限額判斷邏輯(本次交班的核心新檔)。
4. `weekly/appraise_selected.py` — 評讀流程;Codex 是這裡的 fallback backend。
5. `skills/literature-appraisal/SKILL.md` — 1162 行的文獻評讀 Skill v3.3,評讀路徑會把這份完整塞進 prompt。

## 雙後端契約(重要)

```
summarize_one / appraise_pdf
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
  Opus 4.6)   GPT 5.5 評讀)
```

切換規則:

- **`ClaudeLimitError`**(錯誤訊息含 `rate_limit` / `billing` / `credit` / `monthly limit` / `usage limit` / `quota` / `agent sdk` 等子字串)→ **永久切換**:設 `_session["claude_exhausted"] = True`,本 process 之後所有呼叫直接走 codex。下個 process(下次 run)重新從 claude 嘗試。
- **其他 `ClaudeError`**(timeout、parse、暫時性 5xx 等)→ **單次 fallback**:本次走 codex,後續仍會繼續試 claude。
- 設計刻意:使用者明確**不開 usage credits**,所以額度歸零時 claude 端就會回錯,fallback 是唯一信號。**不要在程式內自己算用量**。

## 模型對照

| 角色 | Claude(主) | Codex(fallback) | 覆寫 env |
|---|---|---|---|
| 摘要 / 短評 | `claude-haiku-4-5` | `gpt-5.4` | `JOURNAL_FETCHER_CLAUDE_SUMMARY_MODEL` / `JOURNAL_FETCHER_CODEX_MODEL` |
| 完整評讀 | `claude-opus-4-6` | `gpt-5.5` | `JOURNAL_FETCHER_CLAUDE_APPRAISAL_MODEL` / `JOURNAL_FETCHER_APPRAISAL_MODEL` |

**為什麼 Opus 4.6 而非 4.7**:每 token 價格一模一樣($5/$25),但 4.7 換了新 tokenizer 同樣中文文字會多吃最多 35% tokens。4.6 比較省。

## 一個一定不能踩的雷:launchd 環境的 claude auth

`modules/claude_exec.py::claude_env()` 做兩件事:

1. 移除 `ANTHROPIC_API_KEY`(強制走 keychain OAuth = 訂閱計費,不是 raw API)。
2. **若 `USER` 或 `LOGNAME` 沒設,從目前 uid 反查並補上**。

launchd 啟動的 process env 幾乎是空的(只有 plist 裡寫的 PATH/HOME)。macOS Keychain 需要 `USER` / `LOGNAME` 識別 owner,否則 `claude -p` 30ms 就回 `"Not logged in · Please run /login"`,整條 claude 路徑全部失效、100% fallback 到 codex。

**修這段時務必保留 `USER`/`LOGNAME` 的 backfill 邏輯**,不然下週一 03:00 launchd 跑出來的就只剩 codex。

## 評讀文章大小政策

`weekly/appraise_selected.py` 用 `ARTICLE_CHAR_BACKSTOP`(預設 1,500,000 字元,可用 `JOURNAL_FETCHER_APPRAISAL_CHAR_BACKSTOP` 覆寫)當高位 backstop。

- 一般論文(4-9 萬字元)與完整 ACC/AHA 指引(~1.05M 字元)都**完整送進去**,不截斷。
- 超過 backstop → raise `ArticleTooLargeError` → `appraisal_status = "too_large"`、不產出評讀(而不是拿病態 PDF/MarkItDown 異常去燒 token)。

Codex 5.5 的 context window 我不確定;如果 fallback 到 codex 跑 1M 字元的指引時 codex 報 context overflow,把 backstop 調小或在 codex 路徑加自己的截斷。

## 週報 HTML 三區版面

`weekly/render.py::render_weekly()` 依 `selection_tags` 拆兩個區:

```
本週精選評讀  ← select_articles.py 系統自選(語意 tags)
已評讀        ← process_appraisal_requests.py(tags 含 "manual_request")
本週文章摘要  ← 其餘文章,去除上面兩區的 PMID
```

`appraisal_card` Jinja macro 重用三區的卡片 markup。「已評讀」卡片用 `.appraised-card` 綠色左邊框,且不放 👍/👎(使用者自己請求的,不需要再回饋)。

## launchd 排程(現況)

```text
Mon 03:00  python3 -m weekly.run_weekly --no-discord --select-top 5
Mon 08:00  python3 -m weekly.notify_latest               (Discord 推播)
每 15 分鐘  python3 -m weekly.process_appraisal_requests
```

- `--select-top 5`:每週 5 篇 Opus 評讀,先消耗 Claude Agent SDK 額度; hit limit 後本 process 會自動 fallback 到 Codex。
- launchd 不會主動喚醒 Mac;週日晚上要保持機器醒著(或合蓋接電源 Power Nap)。
- log:`output/logs/weekly.{out,err}.log` 等。

## 本次交班時未 commit 的檔案

```
modules/claude_exec.py            (新檔)
modules/summarize.py              (改用 dispatcher)
weekly/summarize_weekly.py        (改用 dispatcher)
weekly/appraise_selected.py       (dispatcher + ArticleTooLargeError backstop)
weekly/render.py                  (三區版面 split + 摘要區去重)
weekly/templates/weekly.html      (appraisal_card macro + 已評讀 section + .appraised-card)
CLAUDE.md                         (五處同步雙後端架構)
docs/2026-W21.html                (用三區重渲過、本機,未 push)
README.md                         (本次更新)
AGENTS.md                         (本次更新)
```

驗證狀態:`py_compile` 全過、imports OK、Claude Haiku 主路徑端到端跑通($0.04,~30 秒/篇)、Opus 4.7 評讀基準量過($1.01/篇、無有效 cache 跨文章複用)。**Opus 4.6 路徑尚未實際跑過評讀**,第一次真實呼叫會發生在 2026-05-25 Mon 03:00。

## 已知的設計決策與其理由

1. **不用 `--bare` 模式**:`--bare` 需要 `ANTHROPIC_API_KEY`(raw API 計費),違反「吃 Claude 訂閱 / Agent SDK 額度」的目的。所以接受 Claude Code base system prompt 的少量 token overhead。
2. **subprocess `cwd` 設為 `tmpdir`**:避免 claude -p 自動載入專案 CLAUDE.md / skills,造成不必要的 token 開銷。
3. **Prompt caching 經實測效果有限**(1h ephemeral cache 內重跑同樣 prompt 只有 21% token hit、$0.06 saving)。Cost model 不依賴 caching 折扣。
4. **`process_appraisal_requests` 走同一條 dispatcher**:on-demand 評讀請求和週報自選評讀共用 `appraise_selected.appraise_selected()`,所以雙後端與 backstop 對它們自動生效。
5. **訂閱優先於 batch**:使用者有現存 Claude/ChatGPT 訂閱,Batch API 是獨立 raw-API 計費,改用會變成額外付費,所以不採用。

## 接手後可能會被問到的事

- **如果 launchd 跑完發現 claude path 全部失敗**:先看 `output/logs/weekly.err.log`,確認是不是 `USER`/`LOGNAME` 缺失導致的 `Not logged in`。若是,檢查 `claude_env()` 是否還在補。
- **要不要把 `--select-top` 降回 5**:成本估算是月 ~$25-30(8 篇 Opus + 80 篇 Haiku);若想塞進 Pro $20 Agent SDK 額度,降到 5 篇是其中一個槓桿。
- **參考文獻剝除**:討論過但沒做。指引 / SR 的 reference 段佔 markdown 30-50%,方法學評讀用不到。要實作就在 `_convert_pdf_to_markdown` 之後加 regex 剝除(`## References` 之後砍掉)。但 regex 跨期刊格式較脆弱,建議謹慎。
- **6/15 之後**:`claude -p` 從訂閱用量改成 Agent SDK 月額度($20 Pro / $100 Max 5x);需要一次性 claim。但程式邏輯不變 —— dispatcher 看到限額錯誤就 fallback。

## 開發習慣

- Python 3.10+,型別註記齊全,避免 flag parameter。
- 修改後 `python3 -m py_compile` 起手,週報相關改動可用 `python3 -m weekly.run_weekly --dry-run --no-summarize --count 2 --journals NEJM` 跑空殼。
- Git commit 前確認沒留 debug code。Commit 訊息接 Co-Authored-By 自己。
- 不開 ANTHROPIC_API_KEY,不開 OpenAI API key —— 雙後端都靠 CLI 訂閱登入。
- 本機在機構內網,PDF 下載直接 GET DOI,不需要 VPN / cookie。
