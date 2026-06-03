---
name: literature-appraisal
description: Use this skill when the user asks to critically appraise, interpret, critique, or generate a structured evidence-based review of a medical paper, clinical trial, observational study, diagnostic/prognostic model, systematic review, meta-analysis, guideline, narrative review, expert opinion, or uploaded PDF/article link. The workflow routes articles by study type, produces Traditional Chinese clinical-methodology appraisal, and uses bundled JAMA Users' Guides / causal inference references when needed.
---

# SKILL: 醫學文獻批判性評讀 v3.6

## 融合來源

1. 原始 literature-appraisal-SKILL.md
2. JAMA Users' Guides to the Medical Literature（JAMAevidence）
3. CI + MCID + Bayesian RCT 結果分類框架（Harrell / Zampieri / ASA / Pocock / Gelman & Carlin）
4. Gyawali B. How I Read a Clinical Trial Report? JCO Oncol Pract. 2026
5. SANRA (Baethge et al. 2019) — narrative review 專屬品質評估工具

## v3.6 變更摘要

- **重新引入 JAMA Users' Guides 參考機制（取代 v3.4「移除 Bundled references」決定，v3.5 沿用該決定）**：weekly pipeline 改為主動讓 backend 取用本機的 `references/jamaevidence/`：
  - **claude backend**：呼叫 `claude -p` 時帶 `--add-dir <references_dir>` 與 `--tools Read --allowedTools Read`，模型可按需 `Read` 對應方法學的 JAMA Users' Guide markdown。
  - **codex backend**：`codex exec --sandbox read-only` 本來就能讀任何使用者可讀檔案；在 prompt 前段注入 `CODEX_REFERENCE_INSTRUCTIONS` 明確告知可用 `cat` / `rg` 讀取。
  - **取用策略**：appraise pipeline 透過 `_REFERENCES_BY_ROUTE` 與 `_build_references_section()` 按 route 列出對應檔案的**絕對路徑**到 system prompt（共用 prompt cache）。模型只在批判實際涉及對應方法學議題時才讀，引用格式為 `[參考: <filename> 第 X 節]`。
- **Local-only 機制（重要）**：`references/jamaevidence/` 跟 `references/index.md` 都在 `.gitignore`，**只存在維護者本機**。CI / clone 環境下整個目錄不存在 → helper 自動 return ""，pipeline 等價於 pre-v3.6 行為（沒有 references 層）。文字提及檔案是 graceful degradation 而非 hard requirement。
- **v3.4 的「移除 Bundled references」段（v3.5 沿用）現在已過時**：保留在下方版本歷史以便追溯，但**本版本路由與 SECTION-0 規則不再受其約束**。
- **`_REFERENCES_BY_ROUTE` 路由對照**（程式碼層；非 prompt 內規範）：
  - `rct` → 5 份（therapy validity、亞群、非劣性、platform trial、surgical RCT）
  - `observational` → `jama_agoritsas_…`、`hernanrobins_WhatIf_…`
  - `sr` / `nma` → `jug140001.md`、`jug120001_…`
  - `cpg` / `consensus` → `jama_brignardellopetersen_…`
  - `diagnostic` → `jama_alba_…`、`jama_liu_…`、`jml90008.md`
  - `preclinical` / `narrative` / `default` → 無對應，不掛載 references 層

## v3.4 變更摘要

- **A2（CPG / Consensus）新增「明確不適用的標準」段**：仿 A3 narrative review 寫法，明確指出 PROSPERO 註冊、獨立 SR、GRADE 框架、PRISMA flow、雙人篩選等屬於 systematic review 的方法學要求，不應作為 ACC/AHA / ESC 等專科學會型 CPG 的失敗批判；這些是制度差異而非品質缺陷。
- **A2.1 AGREE-II 改為定性評估**：取消「每面向 1–7 分、總分 __/42」的偽精度評分（正規 AGREE-II 是 23 個 item 換算 domain percentage，6 個面向各打一分不是 AGREE-II）。改為六面向各自定性評定「強 / 中 / 弱」並附理由。
- **A2.0「建議分布概覽」禁止輸出估計總數**：當 PDF→markdown 轉檔導致部分建議方塊遺失時，必須明確列出遺失章節清單，不得從建議框出現次數或 Table 1 計數推算「總數 N–M 條」這類不可驗證的估計值。
- **F. 最終裁決 CPG 專屬欄位同步更新**：移除「AGREE-II 總分 __/42」、改為六面向定性；建議清單完整性改報「可見並完整重建 N 條 + 遺失章節 M 個」。
- ~~**移除「Bundled references」段**~~（**已被 v3.6 取代**；保留為歷史紀錄）：原本因 `cwd=tmpdir` 與 references 不在 repo 而撤掉相關引導。v3.6 透過 `--add-dir` (claude) 與 `read-only` sandbox (codex) 重新讓本機 references 可被取用；本機沒有時 helper 自動降級不輸出 references 段，等價於 pre-v3.6 行為。

## v3.3 變更摘要

- **A2 子模組（Clinical Practice Guideline / Consensus Statement）新增強制段落 A2.0**：在進入方法學批判前，必須先以結構化表格完整列出文章本身給出的所有具體建議。
- **A2.0 定位為「論證架構重建」階段的延伸**：先忠實重建，不得在此段混入批判。
- **強化路由規則**：CPG / Consensus 類文章在 SECTION-0 完成後，必須先執行 A2.0「建議清單重建」，再進入 A2.1 / A2.2 方法學評估。
- **SELF-CHECK 新增 CPG / Consensus 專屬檢查項**：確認建議清單完整、分級清楚、強建議低證據條目已計數、共識統計透明度已標注。
- **明確表格分工**：原始研究的數據整理放在 SECTION-0「研究結果重點整理」；SR/MA/NMA 用 evidence synthesis table；CPG/Consensus 用 A2.0 建議清單。

## v3.2 變更摘要（保留）

- **路由規則細分**：將原本 SKILL-B 一網打盡的 review 類文章，依「證據宣稱範疇」拆為四個子型，分別對應不同的 A 段子模組
- **新增 A3：Narrative Review 子模組**——以 SANRA 為基礎方法學評估，額外加上 narrative review 特有的紅旗清單
- **修訂 SECTION-0 第 0 節**——要求明確判斷 review 子型
- **新增「混血文章」警示**——對自稱 review 但方法學介於 narrative 與 systematic 之間的文章特別標注

---

## 觸發條件

當使用者上傳 PDF 或提供文章連結，並要求「評讀」、「判讀」、「appraise」、「critique」時啟動此 Skill。

## 執行順序

所有文章一律先執行 SECTION-0（通用描述），再根據路由規則執行對應 SKILL（A/B/C）。

## 最終輸出邊界

- 只輸出最終文獻評讀，不輸出內部草稿、逐步思考、執行筆記或 self-check checklist。
- SELF-CHECK 只在輸出前內部執行，用來修正遺漏與矛盾，不得作為獨立段落呈現給使用者。

## 輸入來源與可見性限制

- 在 weekly journal 自動化流程中，文章通常是**原始 PDF 經 MarkItDown 轉出的 markdown**，不是可直接視覺檢查的原始 PDF。
- 只能判讀 markdown 中實際保留下來的正文、表格文字、圖說、補充資料文字與頁碼/章節標籤；不得聲稱看過 markdown 中沒有呈現的圖像細節、曲線形狀、影像、流程圖或 supplement 內容。
- 若 markdown 未保留表格、圖片、頁碼、supplement 或部分內容被截斷，必須在相關段落明確標註限制，例如「[限制：PDF 轉檔 markdown 未保留 Figure 2 圖像內容]」。
- 引用文內依據時，優先使用 markdown 保留的 section / table / figure caption / paragraph / page label；若沒有頁碼，不得自行編造頁碼，改標註「[markdown 未保留頁碼]」。
- 只有在使用者實際提供可視覺檢查的原始 PDF / 圖片，且當前工具能讀取圖像內容時，才可進行圖表視覺判讀；否則圖表判讀限於 markdown 轉出的文字與圖說。

## 全域輸出語言規則

- 全文以**台灣臨床醫師習慣的自然繁體中文**輸出；避免中國用語、直譯腔與大段英文原文堆疊。
- 醫學專有名詞、疾病/藥物/裝置正式名稱、試驗名稱、量表、統計術語、指引分級與常用縮寫可保留英文或中英並列，例如 `GRADE`、`Class IIa`、`HFpEF`、`hazard ratio`、`SGLT2 inhibitor`。
- 除上述專有名詞外，研究背景、建議內容、臨床解讀、方法學批判、紅旗與實務建議都必須用清楚的繁體中文表達。
- 若需要保留英文原句以避免失真，只能短句引用，並立刻附上繁體中文精準翻譯或轉述；不得以整段英文替代中文評讀。
- 表格欄位中的「建議內容」「作者主張」「臨床意義」「批判重點」均應以繁體中文為主，英文只作為必要術語或括號補充。

## 全域表格使用規則

- **原始研究**：RCT、觀察性研究、診斷/裝置驗證、生物標記研究、動物/細胞/前臨床研究的數據，放在 `SECTION-0` 第 5 節「研究結果重點整理」；數字很多時可用「主要結果整理」或「關鍵結果」小型表格。
- **證據整合型文章**：systematic review、meta-analysis、NMA、scoping review，或文章本身以多篇研究證據整合為核心時，使用 evidence synthesis table。
- **Guideline / consensus**：使用 A2.0「文章建議清單完整重建」呈現建議；A2.0 仍須以繁體中文完整重建建議內容。

## 路由規則

收到文章後，先掃描以下特徵判斷類型，再選擇對應 SKILL 與子模組：

| 文章特徵 | 啟動 | 子模組 |
|---|---|---|
| RCT、隨機對照試驗、crossover、pilot study、clinical trial | SKILL-A | 主流程 + A1（特殊設計） |
| 觀察性研究、世代研究、病例對照、橫斷面研究 | SKILL-A | 主流程 + 觀察性子模組 + Appendix 因果推論框架 |
| 動物實驗、細胞實驗、前臨床機轉研究 | SKILL-A | 主流程 + 前臨床/機轉子模組；不套用人體臨床試驗專屬要求 |
| Diagnostic accuracy、sensitivity/specificity、AUC、ML 模型、clinical prediction rule | SKILL-C | 主流程 |
| **Systematic Review、Meta-analysis** | **SKILL-B** | **A1（量化合成型）** |
| **Network Meta-analysis、Multiple Treatment Comparison** | **SKILL-B** | **A1（NMA 段）** |
| **Clinical Practice Guideline、Recommendation** | **SKILL-B** | **A2.0（建議清單重建）+ A2.1（方法學評估）** |
| **Consensus Statement、Delphi、Position Paper** | **SKILL-B** | **A2.0（建議清單重建）+ A2.2（共識方法評估）** |
| **Guideline 內含 Delphi 共識成分** | **SKILL-B** | **A2.0 + A2.1 + A2.2 全部執行** |
| **Narrative Review、State-of-the-Art Review、Focus Seminar、Educational Review** | **SKILL-B** | **A3（敘事型，本版新增）** |
| **Expert Opinion、Viewpoint、Perspective、Commentary** | **SKILL-B** | **A3（敘事型，簡化版）** |
| Scoping Review | SKILL-B | A1 + A3 混合判斷 |
| 無法判定 | 詢問使用者確認後再執行 | — |

### 判斷子型的具體流程

收到自稱「review」的文章後，依以下順序判斷：

1. **是否有 PRISMA flow chart 或明確的搜尋策略表？** 有 → 偏向 systematic（A1）；無 → 偏向 narrative（A3）
2. **是否有預先指定的 PICO 與納排標準？** 有 → A1；無 → A3
3. **是否有雙人篩選與品質評估（如 ROB-2、ROBINS-I）？** 有 → A1；無 → A3
4. **是否有量化合成（forest plot、I²、pooled effect estimate）？** 有 → A1；無 → A3
5. **核心輸出是「臨床建議」或「應做什麼」？** 是 → 加上 A2 評估
6. **文章自我宣稱為何？** 與實際方法學是否一致？

**⚠️ 混血文章警示**：若文章自稱 "review" 但實際方法學介於 narrative 與 systematic 之間（例：有引用 PRISMA 但無雙人篩選；有提及搜尋策略但無預先註冊；有 forest-plot-like 圖表但無正式異質性檢驗），必須在 SECTION-0 第 0 節明確標注：「**自稱方法學類別與實際執行存在落差**」，並同時套用 A1 與 A3 的相關評估點。

---

## SECTION-0：文章全面描述（所有類型皆執行）

### 目的

在進入批判性評讀前，先對這篇文章做完整的情境建立。明確標注哪些資訊來自文內、哪些來自外部搜尋。

### SEARCH INSTRUCTIONS

以下項目必須主動搜尋外部資料：

- 作者背景與團隊過往研究（Google Scholar / PubMed 作者搜尋）
- 期刊屬性、Impact Factor、審查週期（期刊官網 / Clarivate / Scimago）
- 該期刊 peer review 平均接受天數（期刊官網 editorial stats 或 journalguide）
- 外部專業意見 / editorial / commentary（PubMed 搜尋同期 editorial；搜尋 "[論文標題] commentary" 或 "[論文標題] editorial"）

**標注格式**：

- 文內資料 → [文內]
- 外部搜尋 → [外部: 來源名稱]
- 找不到 → [無法取得]

**外部資料與推論邊界**：

- 不得用模型記憶補外部資料。凡是作者背景、期刊指標、審查週期、editorial/commentary、同期外部評論等外部欄位，必須來自實際搜尋結果。
- 若外部搜尋找不到，標註「[無法取得]」，不要用印象或常識補齊。
- 若是根據文內線索做合理推論，必須明確標註「[推論]」並說明推論依據。
- 不要輸出與目標文章無關的泛泛教科書內容；方法學批判必須回扣本文設計、數據、限制或外部證據。

### OUTPUT FORMAT — SECTION-0

#### 0. 文章身份證

- 標題、期刊、年份、DOI
- 文章類型（路由判斷結果）
- **若為 review 類**：明確標注子型（systematic / NMA / CPG / consensus / **narrative** / expert opinion）
- **混血文章警示（若適用）**：自稱類別 vs 實際方法學的落差說明

#### 1. 作者團隊評論

[需搜尋外部資料]

- 通訊作者：姓名、機構、專長領域
- 作者團隊構成：各作者分工與專長（若文內有 Author Contributions）
- 所屬機構聲譽：機構在此領域的研究地位
- 團隊過往研究：此團隊在相關主題的代表性發表（3–5 篇，附年份與期刊）；是否為該領域核心研究者？或跨領域首次切入？
- **作者立場史（narrative review 與 CPG / Consensus 特別關鍵）**：作者過去公開立場與本篇結論是否一致？若高度一致，可能存在確認偏差
- 利益揭露：
  - 文內申報的 COI / funding 來源 [文內]
  - 評估：funding 是否可能影響研究方向或結論呈現？

#### 2. 文獻 Metadata 整理

[部分需搜尋外部資料]

- Received date / Accepted date：[文內]；審查天數（自行計算）
- 該期刊平均審查天數：[外部]（來源標注）
- 比較：本篇審查速度相對平均值是快 / 慢 / 相當，並給出可能解讀
- 期刊屬性：Impact Factor（最新）[外部]；期刊分區（Q1–Q4）；Open Access 狀態
- **邀稿 vs 投稿**：是否為主編邀稿（invited review）？[文內若有揭露]
  - Invited review 通常 peer review 較寬鬆，且作者立場較受編輯偏好影響

#### 3. 研究背景

[僅根據本篇文內資料]

- 既有事實與證據：作者引用了哪些已知事實作為研究出發點？
- 知識缺口：作者指出目前文獻有什麼不足或爭議？
- Research question：明確的研究問題是什麼？（若文內未明確陳述，根據 Introduction 末段推論並標注「[推論]」）

#### 4. 研究設計與方法

- **PICO**：
  - P（Population）：
  - I（Intervention / Exposure / Index test）：
  - C（Comparison / Reference standard）：
  - O（Outcome）：主要終點 + 次要終點
- 關鍵研究設計細節：設計類型、樣本來源、收案期間、追蹤時長、隨機化方式（若適用）、致盲層級（若適用）、統計方法（主要分析、ITT vs per-protocol、多重比較處理）
- 主要變項定義：主要 outcome 的操作型定義（如何測量？cut-off 如何設定？）

**對 narrative review 的調整**：第 4 節改為「文章範疇與架構」——

- 文章涵蓋的主題範疇：作者宣稱要回顧的範圍
- 文章章節架構：主要章節順序與篇幅分配
- 文獻來源描述（若有）：搜尋的資料庫、時間範圍、語言限制；若無，標注「[無搜尋策略描述]」
- 引用文獻總數與年份分布

**對 CPG / Consensus 的調整**：第 4 節改為「指引制定方法學」——

- 委員會構成：學科組成、地理代表性、患者代表是否納入
- 證據檢索與評估方法：是否自行進行 SR？是否使用 GRADE 或等效系統？
- 共識方法（若為 consensus）：Delphi / RAND-UCLA / Nominal Group Technique，幾輪投票，匿名性，共識門檻
- COI 管理機制
- 外部審查流程

#### 5. 研究結果重點整理

（同原版，視文章類型適用）

**對 narrative review 的調整**：第 5 節改為「核心論點與證據呈現」——

- 作者的核心論點（3–5 條）
- 支撐各論點的證據類型分布（RCT / 觀察性 / 動物 / 細胞 / 機轉 / 專家意見）
- 是否有量化呈現？若有，是哪一類圖表？（forest-plot-like？整合圖？時間軸？）

**對 CPG / Consensus 的調整**：第 5 節留作簡述（建議總數、強度分布的高階概覽），詳細建議清單由 A2.0 處理。

#### 6. 作者討論與評論重點

- 作者的主要訊息：作者認為最重要的貢獻是什麼？
- 與現有文獻的異同：結果一致處 / 不一致處（作者如何解釋？）
- 限制（作者觀點） [文內]：逐條列出；評估是否完整或避重就輕
- [評估]：作者的解釋是否合理？臨床建議是否超出數據支持的範圍？

#### 7. 外部專業意見

[需搜尋外部資料]

- 若找到外部意見：摘要重點，標注來源（作者、期刊、年份）
- 若未找到：標注「[無法取得]——未發現同期 editorial 或公開評論」

#### 8. 臨床建議

- 可採用這篇研究結論的臨床情境（具體說明適用條件）
- 不建議直接套用的情境（族群、setting、或條件不符）
- 給臨床醫師的實務建議（1–3 點，直接、具體、可操作）
- 研究證據強度分級：強 / 中 / 弱（附理由）

### SECTION-0 完成後

繼續執行路由規則，進行對應 SKILL 的批判性評讀。

**重疊處理原則**：SKILL-A/B/C 的「補充技術細節」只記錄 SECTION-0 尚未涵蓋的細節，不重複已呈現的內容。偏差與方法學批判完全由 SKILL 的 B–E 節處理。

---


## Fragment 載入機制（v3.5 新增）

Pipeline (`weekly/appraise_selected.py`) 會先用 `weekly/classify_article.py` 預判文章類型，然後把對應 fragment 接在這份 SKILL.md 後面送入 prompt。完整路徑：

| Route                                  | Fragment 檔                                  |
|----------------------------------------|----------------------------------------------|
| `rct` / `observational` / `preclinical`| `fragments/skill_a.md`                       |
| `sr` / `nma`                           | `fragments/skill_b_sr.md`                    |
| `cpg` / `consensus`                    | `fragments/skill_b_cpg.md`                   |
| `narrative`                            | `fragments/skill_b_narrative.md`             |
| `diagnostic`                           | `fragments/skill_c.md`                       |
| `default`（無法判定）                  | 所有 fragments 串接 = 拆分前完整 SKILL.md    |

本檔含全域規則（觸發、輸出邊界、輸入限制、語言、表格、路由、SECTION-0）；fragment 含對應 SKILL-A / SKILL-B(A1/A2/A3) / SKILL-C 的具體執行框架。請依下方接上的 fragment 執行其指示。

若 fragment 未接上（極少數異常路徑），預設執行 SECTION-0 後即停止並標注「[限制：未載入對應評讀 fragment]」，不要強行用記憶填補。

---

## Version 3.6

**v3.6 主要變更**（JAMA references 重新可用 + local-only graceful degradation）：
- 透過 `claude -p --add-dir <references_dir>` + `Read` 工具，讓 claude backend 可按需讀取 `references/jamaevidence/` 內的 JAMA Users' Guides
- 透過 `codex exec --sandbox read-only` + `CODEX_REFERENCE_INSTRUCTIONS` 注入，讓 codex backend 可 shell-read 同一批檔案
- 新增 `_REFERENCES_BY_ROUTE` 對照表（rct / observational / sr / nma / cpg / consensus / diagnostic 有對應檔；preclinical / narrative / default 無對應，不掛載 references 層）
- 新增 `_build_references_section()` helper：把 route 對應檔案以**絕對路徑**列入 cached system prompt；fragment 末段補上「何時讀」的觸發條件
- **Local-only**：`references/jamaevidence/` 與 `references/index.md` 在 `.gitignore`，本機沒這目錄時 helper 自動 return ""，等價 pre-v3.6 行為（無 references 層）
- 撤銷 v3.4「移除 Bundled references」決定（v3.5 沿用該決定）；該決定當時是因 cwd=tmpdir 與 repo 不含目錄而做的安全考量，v3.6 用 `--add-dir` 與絕對路徑 + 存在性檢查解決了該疑慮

## Version 3.5

**v3.5 主要變更**（SKILL.md fragment 拆分）：
- 將 SKILL-A / SKILL-B(A1/A2/A3) / SKILL-C 三大主體拆出 `fragments/` 目錄
- 本 SKILL.md（entry）只保留全域規則 + SECTION-0 + 路由表
- `weekly/appraise_selected.py` 透過 `classify_article.classify()` 預判 route，只載入對應 fragment
- 對單一 route 的 appraisal，context 從 ~26k tokens (full skill) 降至 ~12-16k tokens（依 route 不同）
- 「default」route fallback 串接所有 fragments = 拆分前行為，零風險
- 分類用 Haiku 4.5 主、codex gpt-5.4 備、本地 heuristic 第三層、default 第四層

## Version 3.4

**v3.4 主要變更**（CPG 路徑的類別錯誤與偽精度修正）：
- A2 新增「明確不適用的標準（CPG / Scientific Statement）」段：明定 PROSPERO 註冊、獨立 SR、GRADE、PRISMA flow、雙人篩選等屬於 systematic review 的方法學要求，不應作為 ACC/AHA / ESC 型 CPG 的失敗批判
- A2.1 AGREE-II 改為六面向定性評估（強 / 中 / 弱），禁止輸出「__/42」這類偽加總分數
- A2.0 (3) 禁止輸出「估計總數 N–M 條」，改為「可見 N 條 + 遺失章節清單」
- F. 最終裁決 CPG 專屬欄位同步更新
- SKILL-B SELF-CHECK 新增三項 CPG 相關檢查項
- 移除「Bundled references」段（weekly pipeline 將 skill inline 進 prompt 且 cwd=tmpdir，`references/jamaevidence/` 實際不可達，且本 repo 未提供）

## Version 3.3

**v3.3 主要變更**：
- 新增 A2.0：CPG / Consensus 文章必須先完整重建文章本身的建議清單，再進行方法學批判
- CPG / Consensus 路由拆成 A2.0 + A2.1 / A2.2，混合型全執行
- A2.0 要求逐條列出建議內容、建議強度、證據品質、文內位置與主要支撐文獻；不得省略或歸納合併
- 新增建議分布概覽、強建議低證據交叉分析、共識同意比例透明度檢查
- 最終裁決新增 CPG / Consensus 專屬欄位

## Version 3.2

**v3.2 主要變更**：
- 新增 A3 子模組：Narrative Review / Expert Opinion 專屬評讀框架
- SANRA 量表（Baethge et al. Res Integr Peer Rev 2019）作為方法學基礎
- 新增 narrative review 特有的五大紅旗類別（結構性偏差、敘事框架偏差、作者立場、準量化偽裝、自我宣稱誠實度）
- 路由規則細分為四個 review 子型，避免類別錯誤
- 新增「混血文章」警示機制

**融合來源**：
1. JAMA Users' Guides to the Medical Literature
2. CI + MCID + Bayesian RCT 結果分類框架（Harrell / Zampieri / ASA / Pocock / Gelman & Carlin）
3. 原始 literature-appraisal-SKILL.md
4. Gyawali B. How I Read a Clinical Trial Report? JCO Oncol Pract. 2026
5. **SANRA (Baethge C, Goldbeck-Wood S, Mertens S. Res Integr Peer Rev. 2019;4:5)**

**適用設計**：RCT、Non-inferiority trial、Surgical RCT、Platform trial、觀察性研究、Diagnostic accuracy、AI/ML model、Clinical prediction model、Systematic review、Meta-analysis、Network meta-analysis、Clinical practice guideline、Consensus statement、**Narrative review**、**Expert opinion / Viewpoint / Perspective**

**關鍵參考文獻**：

統計框架：
- Altman & Bland (BMJ 1995) · ASA Statement (Am Stat 2016) · Zampieri et al. (AJRCCM 2021) · Gelman & Carlin (Perspect Psychol Sci 2014) · Pocock & Stone (NEJM 2016) · Hawkins & Samuels (JAMA 2021) · Freiman et al. (NEJM 1978) · Campbell & Gustafson (PLoS ONE 2018)

Narrative review 評估：
- Baethge C, Goldbeck-Wood S, Mertens S. SANRA—a scale for the quality assessment of narrative review articles. Res Integr Peer Rev. 2019;4:5.
- Gasparyan AY, Ayvazyan L, Blackmore H, Kitas GD. Writing a narrative biomedical review: considerations for authors, peer reviewers, and editors. Rheumatol Int. 2011;31:1409-17.
