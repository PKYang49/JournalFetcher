---
name: literature-appraisal
description: Use this skill when the user asks to critically appraise, interpret, critique, or generate a structured evidence-based review of a medical paper, clinical trial, observational study, diagnostic/prognostic model, systematic review, meta-analysis, guideline, narrative review, expert opinion, or uploaded PDF/article link. The workflow routes articles by study type, produces Traditional Chinese clinical-methodology appraisal, and uses bundled JAMA Users' Guides / causal inference references when needed.
---

# SKILL: 醫學文獻批判性評讀 v3.3

## 融合來源

1. 原始 literature-appraisal-SKILL.md
2. JAMA Users' Guides to the Medical Literature（JAMAevidence）
3. CI + MCID + Bayesian RCT 結果分類框架（Harrell / Zampieri / ASA / Pocock / Gelman & Carlin）
4. Gyawali B. How I Read a Clinical Trial Report? JCO Oncol Pract. 2026
5. **新增：SANRA (Baethge et al. 2019) — narrative review 專屬品質評估工具**

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

## Bundled references

JAMA Users' Guides and causal inference references converted with Microsoft MarkItDown live in `references/jamaevidence/`.

Before loading a reference, inspect `references/index.md` and choose only the files relevant to the article type. Do not bulk-load the full reference set; `hernanrobins_WhatIf_2jan25.md` is very large and should be searched with `rg` for targeted causal-inference questions.

Use bundled references as supporting methodology checks, not as replacements for reading the target article.

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

## SKILL-A：原始研究批判性評讀（臨床 ＋ 前臨床）

### ROLE

嚴格的文獻審稿人兼臨床方法學顧問。任務不是幫作者說服讀者，而是主動找出弱點並給出可驗證的理由。先萃取事實，再下結論。

### INPUT HANDLING

- 輸入為 PDF converted markdown：先掃描 markdown 全文；Table 與 Figure 只判讀 markdown 中保留下來的表格文字、圖說與相關正文，未保留的圖像內容必須標註限制。
- 輸入為可視覺檢查的原始 PDF：才可檢視全文頁面、Table 與 Figure 圖像；引用時標注頁碼。
- 引用格式：(Methods) / (Table 1) / (Figure 2 caption) / (p.X，若 markdown 有保留頁碼) / [markdown 未保留頁碼]
- 資訊缺失：寫「無法判定——文內未提供[具體缺漏]」

### CORE RULES

1. 先萃取事實，再下結論。每個批判必須附文內依據（頁碼/段落/表格）。
2. 不確定就明講「無法判定」，禁止腦補數字。
3. p-value 使用原則（臨床研究適用；Altman 1995 / ASA 2016 / Harrell）：
   - p > 0.05 ≠ "no effect"、≠ "no difference"、≠ "negative"
   - 臨床結果分類必須依 CI + MCID，而非 p 值
   - 禁止使用 post-hoc power（post-hoc power = f(p-value)，零資訊量）
   - 臨床研究必須同時評估效果量、CI 寬度、MCID 位置、臨床意義；前臨床研究改評估效果量、變異度、replicate 層級與模型有效性
4. 優先抓讓結果「看起來變好」的偏差：多重比較、選擇性報告、致盲失敗、安慰劑不匹配、基線不平衡、carryover、underpowered positive（Winner's Curse）等。
5. 確認三個核心 EBM 問題：
   - Results valid?（內部效度 / 偏差風險）
   - What are the results?（效果大小、精確度、方向）
   - Will results help my patients?（外推性 / 外部效度）

### 適用性守門規則

- 人體 RCT、非劣性試驗、觀察性研究才套用 randomization、allocation concealment、ITT/per-protocol、MCID、clinical endpoint、真實世界外推性等臨床試驗專屬要求。
- 動物、細胞、ex vivo、mechanistic 或其他前臨床研究，不要求 PICO 完整臨床對照、不要求 MCID/clinical endpoint、不做臨床採用判斷；重點改為模型有效性、劑量/暴露合理性、測量可靠性、轉譯限制與是否過度推論人體效果。
- 若文章混合人體資料與前臨床資料，分段處理：人體資料依臨床研究規則，前臨床資料依前臨床/機轉規則。

### SELF-CHECK（輸出前內部執行，不顯示給使用者）

- [ ] 每個批判都有文內具體依據（頁碼/段落/表格）？
- [ ] 有沒有任何地方腦補了數字或結果？
- [ ] 統計紅旗是否達 5 點？若不足，是否說明原因？
- [ ] 效果量與 CI 是否都有呈現？
- [ ] 三個 EBM 核心問題是否都有回答？
- [ ] 結果是否已用 CI + MCID 分類？
- [ ] 有沒有錯誤使用「p > 0.05 = no difference」？
- [ ] 有沒有錯誤使用 post-hoc power？
- [ ] Underpowered positive 的 Winner's Curse 風險是否已評估？

### OUTPUT FORMAT — SKILL-A

#### A. 補充技術細節

研究基本資料（PICO、設計類型、主次終點、資助）已詳述於 SECTION-0 第 3–6 節，此處僅補充下列 SECTION-0 未涵蓋的統計與設計細節：

- 樣本數估算：是否有事前 power calculation？估算假設為何？實際樣本是否達標？
- 隨機化細節（RCT）：方法（區塊 / 分層 / 電腦亂數）、分派隱藏方式（allocation concealment）
- 致盲細節（若適用）：盲的層級（受試者 / 施測者 / 結果評估者）、是否有盲法成功度檢查
- ITT vs per-protocol：主要分析採用哪一種？兩者結果是否一致？
- 多重比較處理：是否預先指定主要終點？次要終點是否有校正？
- 比例風險假設（Cox 模型）：是否通過 proportional hazards 檢驗？
- Washout / carryover 評估（crossover 設計）：期間長度是否足夠？有無實測 period effect？
- 缺失資料處理：遺失比例、填補方式（carry-forward / multiple imputation）

##### A1. 特殊設計子模組（視文章類型選擇性展開）

**Non-inferiority Trial（若適用）**

- Non-inferiority margin（Δ）是否明確並有臨床與統計上的合理依據？
- Active comparator 是否以最佳劑量與療程給藥？
- ITT 分析在 non-inferiority 設計中是保守的——是否同時報告 per-protocol 分析？
- 觀察到的差異相對於 margin 的臨床意義為何？

**Surgical / Procedural RCT（若適用）**

- 術者專業能力在各組之間是否控制一致（surgeon expertise variability）？
- 是否使用假手術（sham procedure）進行致盲？倫理合理性？
- 是否為 expertise-based design（術者被隨機分配至其擅長術式）？
- 機轉性試驗 vs 實用性試驗——對臨床適用性的影響？

**Platform / Adaptive Trial（若適用）**

- 是否有 master protocol 與預先指定的適應規則？
- 多重比較的 Type I error 如何控制？
- 子試驗是否共用同期對照組（contemporaneous control arm）？
- 入組族群是否符合目標臨床情境？

**Preclinical / Mechanistic Study（若適用）**

- 模型是否能代表作者宣稱的疾病狀態或臨床情境？若只是機轉模型，明確標注「只能支持機轉假說」。
- 動物品系、性別、年齡、樣本數、隨機分組、致盲評估、排除標準與死亡/失敗實驗是否透明？
- 細胞或 ex vivo 模型是否說明來源、passage、刺激條件、劑量/濃度與暴露時間？是否在生理可達範圍？
- 終點是否為 surrogate / mechanistic readout？是否有功能性或疾病相關 outcome 支撐？
- 統計單位是否正確？避免把 technical replicates 當 biological replicates。
- 轉譯聲明是否過度：不得由動物/細胞結果直接推出人體療效、診斷效能或臨床採用。

##### 觀察性研究交代清楚混淆控制方法（若適用）

| 方法 | 強度 | 關鍵要求 |
|---|---|---|
| 多變量迴歸 | 中 | 所有混淆因子皆已測量並納入 |
| Propensity score（matching/weighting） | 中–高 | 共變量分布有足夠重疊（overlap） |
| Instrumental variable（IV） | 高（若有效） | 工具變數與暴露強相關、獨立於混淆、僅透過暴露影響結果 |

- 是否有殘餘混淆（residual confounding）的討論或敏感度分析？
- 暴露與結果的測量是否保有時間序列（temporality）？

#### B. 批判性評估（四大面向）

每點格式：問題 → 文內證據（附位置）→ 風險方向 → 需要什麼才放心

##### B1. 假說與理論邏輯

- 假說是否預先指定？是否存在撒網式測量後再挑顯著（post-hoc hypothesis generation）？
- 機轉是否與情境匹配？劑量與生理可行性？
- 輸出最可能合理機轉 1–3 點；若有過度敘事，列出最可疑敘事跳躍 1–3 點（各附文內依據）。

##### B2. 受試者與外推性

- 受試者是否與宣稱對象一致？基線是否平衡？
- 研究環境能否外推真實世界？
- 臨床研究輸出外推性分級；前臨床研究輸出轉譯距離分級（近 / 中 / 遠，附理由）。
  1. 一般健康族群：可外推條件 / 不該外推原因
  2. 特定病患族群：同上
  3. 特殊族群（運動員 / 老年 / 兒童等）：同上

##### B3. 研究設計與偏差

- 隨機化與分派隱藏是否充分？（allocation concealment）
- 致盲是否可行？是否有致盲成功檢查？
- Crossover 設計：washout 是否足夠？有無 carryover / period effect？
- 試驗是否提早停止？（提早停止會誇大效果估計）
- 依從性與共變項控制？

**對照組適當性（Gyawali 框架）**

- 對照組是否為當前標準治療（SoC）？核心問題：「這是我日常臨床會給患者的治療嗎？」
  - 若否 → 結果只能說「優於劣質對照」，無法直接與 SoC 比較
  - 安慰劑對照的合理情境：無治療為 SoC（末線 / 輔助 / 維持療法）；組合療法（A+B vs A+安慰劑）
- 是否有前期 Phase II 支撐此 Phase III？若 Phase II 為陰性仍進行 Phase III，對任何統計顯著結果應提高懷疑度

**Crossover 與 post-protocol treatment**

- 對照組進展後是否接受了適當後續治療？（若最佳後續治療正是實驗藥物，crossover 應為強制）
- 是否記錄並分析進展後治療（post-progression therapy）？
- 若對照組後續治療不足（某比例患者未接受任何後續治療），OS 差異可能被人為誇大
- 若 crossover 被允許：是否用 RPSFT / two-stage method 校正 OS 分析？
- 輸出最可能造成偏差的設計點，最多 3 個；若不足 3 個，不要硬湊，說明資料限制。能判斷方向時，說明各自讓效果「變大或變小」。

##### B4. 測量工具與實務意義

- 測量工具的信度 / 變異度（test-retest reliability、CV）？
- 終點是否有臨床意義？（surrogate marker vs 真實結果；硬終點 vs 軟終點）
- 統計顯著 ≠ 臨床重要：同時呈現效果量與 CI
- 臨床研究輸出：「就算結果是真的，真實世界可能有多大用處？」（附文內數字）。前臨床研究改輸出：「就算機轉成立，距離人體臨床可用還缺哪幾步？」

#### C. 統計與資料完整性

##### C1. 結果分類（CI + MCID 框架）— 人體臨床研究必須執行

依據 Harrell / Zampieri / Pocock / ASA 框架，拒絕以 p 值單獨分類試驗結果。若為動物、細胞或前臨床機轉研究，改用效果量、變異度、replicate 層級、模型有效性與轉譯限制進行分類，不套用 MCID。

**Step 1 — MCID（δ）確認**

- 文內預先指定的 MCID 為何？（附位置）
- 若未指定：依領域標準估計，標注「[推算]」，並說明依據

**Step 2 — CI 位置判讀**

```
Null value (HR=1 / MD=0)    MCID-benefit (δ)    MCID-harm (δ_harm)
        |___________________________|__________________________|
        ← 利益區 ←                  ← 中性區 →                → 傷害區 →
```

對照以下決策樹判斷主要終點分類：

| 結果類別 | 判斷標準 | 臨床意涵 |
|---|---|---|
| POSITIVE ✅ | p < 0.05 且 CI 完全超越 MCID-benefit | 統計與臨床均顯著 |
| IMPRECISE (+) | p < 0.05 但 CI 跨越 MCID | 顯著但效果量不確定 |
| HARMFUL ☠️ | CI 完全超越 MCID-harm | 有害且具臨床意義 |
| NEUTRAL ⚖️ | p ≥ 0.05 且 CI 窄、完全在 [−δ, +δ] 內 | 兩組實質等效 |
| NEGATIVE ❌ | p ≥ 0.05 且 CI 窄、MCID-benefit 被排除（但傷害仍可能） | 無有意義利益，但非等效 |
| INCONCLUSIVE ❓ | p ≥ 0.05 且 CI 同時跨越 null 與 MCID | 樣本不足，什麼都說不準 |

**Neutral vs Negative 的關鍵差異**：

- Negative = 「這個治療沒有有意義的利益」（單側，傷害仍開放）
- Neutral = 「兩組實質上相同」（雙側，利益與傷害均被排除）
- Neutral 需要更窄的 CI，是更強的聲明，需等效試驗或 Bayesian Pr(ROPE) 正式確認

**Step 3 — 禁止的統計謬誤清單**

- ❌ p > 0.05 = "no difference" / "no effect" / "negative"（54–56% 非顯著 RCT 犯此錯，Gates et al. 2019）
- ❌ Post-hoc power（post-hoc power = f(p-value)，等於零資訊；CONSORT 2010 明確禁止）
- ❌ 以 post-hoc power 判斷 underpowered（正確做法：看 CI 是否同時包含 null 與 MCID）
- ❌ 用「underpowered positive」的效果量做下一個試驗的 power calculation（Winner's Curse）

##### C2. Underpowered 判斷與 Winner's Curse

**Underpowered 定義**：CI 同時包含 null 值與 MCID → 樣本不足以回答問題

**Winner's Curse**（Gelman & Carlin, 2014）：

- 低 power 研究中，只有隨機偏高的估計值才能跨越 p < 0.05 門檻
- 結果：published "positive" 效果量被系統性誇大
- Type M error（magnitude）：效果量可能誇大 5–10 倍
- Type S error（sign）：約 1/4 顯著結果方向完全錯誤（6% power 情境下）

若本篇為 underpowered positive，必須評估：

- Protocol 中的 power calculation 假設效果量為何？
- 實際觀察到的效果量是否遠大於 power calculation 中假設的效果量？（Winner's Curse 訊號）
- 此效果量若用於後續確認試驗的 power calculation，是否會導致嚴重低估所需樣本數？

##### C3. Bayesian 補充分析（當 p 接近 0.05，或 Neutral vs Negative 區分有臨床意義時）

依 Zampieri, Casey, Shankar-Hari, Harrell, Harhay (AJRCCM 2021) 框架。

**三個 Bayesian 核心指標**（針對主要終點）：

| 指標 | 定義 | 對應分類 |
|---|---|---|
| Pr(outstanding benefit) | Pr(效果量 > MCID-benefit) | → Positive 端 |
| Pr(ROPE) | Pr(效果量在實質等效區間內) | → Neutral；唯一能正式量化等效機率的方法 |
| Pr(severe harm) | Pr(效果量 > MCID-harm) | → Harmful 端 |

**三組 Prior 設定**（Zampieri Table 1 規範）：

- Skeptical prior：假設治療無效或有害
- Optimistic prior：假設治療有利
- Non-informative prior：讓資料主導

**敏感度檢驗**：

- 計算三組 prior 結果之間的 I²
- I² < 0.20 → 結論對 prior 選擇不敏感，資料在說話，非 prior 在說話
- 若三組 prior 都指向同方向 → 結論高度穩健

**典型案例**（作為解讀參照）：

- EOLIA（p=0.09）：Bayesian Pr(benefit) = 96% → 被頻率派誤判為"negative"
- ANDROMEDA-SHOCK（p=0.06）：所有 prior 下 Pr(benefit) > 90% → 被誤判為"negative"
- ART（p=0.057）：所有 prior 下 Pr(harm) > 93% → 被頻率派模糊為"borderline"

##### C4. 報告完整性與其他統計問題

**樣本數與 power（雙向評估）**

- 是否有事前 power calculation？估算假設的效果量為何？
- Underpowered：實際樣本 / 事件數是否達標？（→ 見 C2）
- Overpowered（Gyawali）：試驗是否刻意設計以偵測無臨床意義的微小差異？
  - 若 power calculation 設定的最小可偵測差異小於 MCID → 即使統計顯著，臨床上不重要
  - 若實際入組遠超計畫樣本數 → 可能為人為增加偵測力，使臨床無意義的差異達到 p < 0.05
  - 判斷原則：統計顯著 ≠ 臨床重要；必須對照 MCID

**Kaplan-Meier 圖細節判讀**（Gyawali 框架）

- Numbers at risk：隨時間遞減是否急速？末段患者數若過少（如 < 10），曲線尾端的「tail」不可靠，不應作為治癒分率的依據
- Informative censoring：患者是否因毒性或無效退出（非隨機退出）？若是，存活曲線可能呈現假陽性利益；應檢視 supplement 中的 censoring 原因
- Crossing curves：免疫治療常見；早期曲線交叉時，median survival 具誤導性，應同時報告 landmark survival rate（如 2 年、5 年存活率）
- 任意時間點的選擇性報告：若論文強調「18 個月存活率 65% vs 45%」，需確認此時間點是否預先指定；未預先指定的任意時間點報告應視為探索性，不作為主要結論依據

- 多重比較：有沒有校正？哪些結果最像假陽性？
- 分析策略：ITT 還是 per-protocol？遺失資料怎麼處理？
- 報告完整性：是否只報有利結果？是否缺少原始數據 / 變異度？
- 統計面紅旗清單（至少 5 點；不足就說明原因）

#### D. 次組分析評估（若文內有次組分析）

依據以下 5 標準評估每個次組分析的可信度（JAMA Subgroup Criteria）：

1. 是否有 interaction p-value？（光看各組 p 值是否顯著不足以判斷次組效果）
2. 結果方向是否與其他研究一致？
3. 假說是否預先指定並有預期方向？
4. 是否有合理的生物機轉？
5. 若為 meta-analysis：是否來自 within-study 而非 between-study 比較？

每個次組分析逐條回答以上 5 問，並給出可信度評分：高 / 中 / 低 / 推測性

#### E. 紅旗清單

條列，越短越尖銳（設計 / 統計 / 敘事 / COI / 外推）

#### F. 反向解釋

2–4 個替代解釋 ＋ 文內支持 / 反對線索

#### G. 最終裁決

- 整體可信度：高 / 中 / 低（擇一）
- 可採用的條件 ＋ 不建議採用的情境
- 若要改觀，最需要的 3 個關鍵改進
- 一句話結論（20–40 字，給一般讀者）

### CONDITIONAL OUTPUT

**臨床研究必須輸出**（有效果量與 CI 時）：

| 指標 | 介入組 | 對照組 | 效果量 | 95% CI | ARD | MCID | 結果分類 | 實務意義 |
|---|---|---|---|---|---|---|---|---|

**前臨床研究輸出**（有量化數據時）：整理 effect size / fold change / mean difference、變異度、biological replicate 數、technical replicate 處理、模型與轉譯限制；不使用 MCID 欄位。

**臨床研究必須輸出**（非顯著結果時）：明確說明屬於 Neutral / Negative / Inconclusive，並說明 CI 位置與 MCID 的關係。禁止以「no significant difference」作結。

**選擇性輸出**（p 接近 0.05，或 Neutral vs Negative 有臨床爭議時）：執行 C3 Bayesian 補充分析，報告三組 prior 下的 Pr(benefit)、Pr(ROPE)、Pr(harm)。

**紅旗標記**（若未報告效果量或 CI）：在 E 紅旗清單中標記為嚴重缺漏。

---

## SKILL-B：Review Article 批判性評讀

### ROLE

專業學術評審，專長為論證結構分析與引用品質審查。

任務分兩階段：

1. 先忠實重建作者的論證架構（不加評判）
2. 再從外部視角批判引用品質與推論合理性

**絕對不能在第一階段就混入批判。先理解，再評判。**

### INPUT HANDLING

- 輸入為 PDF converted markdown：先掃描 markdown 全文架構（標題層級、章節順序），再逐節分析論證邏輯；Figure 只可依 markdown 保留的圖說或正文描述分析。
- 輸入為可視覺檢查的原始 PDF：才可檢視全文頁面與圖表視覺內容。
- 引用格式：(Section: XXX) / (Figure X caption) / (p.X，若 markdown 有保留頁碼) / [markdown 未保留頁碼]
- 找不到的資訊：寫「無法判定——文內未提供[具體缺漏]」

### CORE RULES

- 第一階段純描述：重建作者論點時，用作者的語言，不插入任何評價。
- 每個批判必須對應到第一階段的具體論點編號。
- 過度推論的判定需說明：① 原始引用說了什麼 ② 作者的結論說了什麼 ③ 兩者之間的跳躍在哪裡
- 引用品質評估需區分：直接證據 / 間接證據 / 類推（動物、細胞、其他族群）
- 不確定就明講「無法判定」，禁止腦補。

### SELF-CHECK（輸出前執行，不顯示）

- [ ] 第一階段有沒有混入批判語氣？
- [ ] 每個過度推論都有指出「原引用說什麼 vs 作者結論說什麼」？
- [ ] 引用品質有區分證據層級？
- [ ] 替代解釋有沒有文內依據？
- [ ] **若為 narrative review：是否套用了 A3 而非錯誤套用 A1 的 systematic review 標準？**
- [ ] **若為混血文章：是否同時標注自稱類別 vs 實際方法學的落差？**
- [ ] **若為 CPG / Consensus：是否先完成 A2.0 建議清單完整重建，且未在 A2.0 混入批判？**
- [ ] **若為 CPG / Consensus：建議分級系統、強度/證據品質分布、強建議低證據條目、共識同意比例透明度是否已整理？**

### OUTPUT FORMAT — SKILL-B

#### A. 補充技術細節

文章基本資料（文章類型、核心主張、研究背景、資助與 COI）已詳述於 SECTION-0 第 1–3 節，此處僅補充：

- 文章撰寫目的的宣稱範疇：作者宣稱結論適用的目標族群/情境是否明確？
- 引用文獻的時間範圍：是否涵蓋近期文獻？是否忽略特定年代的重要研究？

**根據 SECTION-0 路由結果，選擇對應子模組展開（A1 / A2 / A3）**

---

##### A1. Systematic Review / Meta-analysis 子模組（量化合成型）

**搜尋策略與選擇流程**

- 搜尋策略（若有）：關鍵字、資料庫、時間範圍、語言限制
- 納排標準（若有）：是否預先指定？是否有 PRISMA flow？
- 文章選擇流程透明度：單人還是雙人篩選？是否有共識機制（κ 值報告）？
- 是否預先註冊（PROSPERO）？

**文獻可信度（GRADE 五大降級因素）**

| 因素 | 評估 |
|---|---|
| 納入研究的偏差風險（risk of bias） | |
| 不一致性（I²、異質性；無法解釋的不一致） | |
| 間接性（族群、介入、比較、結果與研究問題不符） | |
| 不精確性（寬 CI、少事件數、小樣本） | |
| 發表偏差（漏斗圖不對稱、選擇性結果報告） | |

**Network Meta-analysis / Multiple Treatment Comparison（若適用）**

- 納入研究是否有足夠臨床同質性可進行間接比較？
- Network geometry 是否完整（無孤立節點，transitivity 合理）？
- Direct vs indirect estimate 是否一致（node-splitting / inconsistency test）？
- Treatment ranking（SUCRA/P-score）對模型假設的敏感性？

---

##### A2. Clinical Practice Guideline / Consensus Statement 子模組（規範型）

**執行順序強制要求**：A2.0 → A2.1 → A2.2。不得跳過或合併 A2.0。

A2.0 屬於「第一階段忠實重建」，必須用繁體中文精準翻譯/轉述作者建議，不插入任何評價。批判一律延後至 A2.1 與 A2.2。醫學專有名詞、正式分級、藥物/裝置名稱與常用縮寫可保留英文或中英並列；除此之外不得用大段英文原文取代中文重建。

###### A2.0 文章建議清單完整重建（強制段落）

**目的**：在進入方法學評估前，先以結構化方式完整列出文章本身給出的所有建議。讀者應能僅憑此段落即掌握「這篇文章到底建議了什麼」，無需再回查原文。

**(1) 建議的層級結構**

先說明文章使用的建議分級系統：

- 證據品質分級：使用什麼系統？（GRADE / ACC-AHA Level of Evidence / NICE / 自訂等）各級定義為何？[文內]
- 建議強度分級：使用什麼系統？（GRADE Strong/Conditional / ACC-AHA Class I/IIa/IIb/III / 共識同意比例等）各級操作型定義為何？[文內]
- 若使用多重分級系統，說明兩者如何組合呈現。
- 若未使用正式分級，明確標注「[未使用正式建議分級系統]」。

**(2) 建議清單主表**

依 markdown 可見內容與文章章節順序，逐條列出所有具體建議。**不得省略、不得歸納合併可見的具體建議**。若文章建議極多，仍需完整列出；可用主題分段拆表，但不可只摘要。若 PDF 轉檔缺頁、截斷或未保留建議表，必須標註「[限制：markdown 未完整保留建議內容]」，不得自行補齊不可見建議。

| 編號 | 建議內容（繁體中文精準翻譯/轉述；必要術語可中英並列） | 建議強度 | 證據品質 | 文內位置 | 主要支撐文獻 |
|---|---|---|---|---|---|
| R1 | [完整建議內容] | Class I / Strong / 95% 同意等 | Level A / High 等 | p.X, Table/Box X | 主要引用文獻（編號或第一作者） |
| R2 | ... | | | | |

**格式細則**

- 建議內容必須完整保留「對誰、做什麼、何時、目標為何」四要素；若原文有遺漏，標注「[原文未明確]」。
- 建議內容欄位以繁體中文為主；只保留必要醫學專有名詞、正式推薦分級、藥物/裝置名稱與縮寫原文。不得整句或整段直接貼英文 recommendation 充數。
- 若英文原句措辭本身會影響建議強度（例如 `should be considered`, `is recommended`, `may be reasonable`, `should not`），可在中文翻譯後用括號保留該關鍵短語。
- 若文章使用條件句（例如 "in patients with X, Y is recommended"），條件部分不得省略。
- 若同一條建議在不同章節重複出現或有微妙差異，併列呈現並標注差異。
- 多語言文獻：以繁體中文翻譯/轉述為主；術語翻譯若有疑義，附上原文括號標注。

**(3) 建議分布概覽**

完成清單後，提供統計概覽：

- 建議總數：__ 條
- 各強度分布：Strong/Class I __ 條；Conditional/Class IIa __ 條；Class IIb __ 條；Class III/不建議 __ 條
- 各證據品質分布：High/Level A __ 條；Moderate/Level B __ 條；Low/Level C __ 條；Expert opinion __ 條
- **關鍵交叉分析**：
  - 「強烈建議 + 低證據品質」條目數：__ 條（列出編號）
  - 完全基於專家意見的強烈建議：__ 條（列出編號）
  - 與前一版 guideline 相比的變更（若文內有提及）：新增 __ 條；移除 __ 條；強度升級 __ 條；強度降級 __ 條

**(4) 建議主題分組**

依臨床決策階段或主題分組（如：診斷 / 風險評估 / 藥物治療 / 介入治療 / 追蹤監測 / 特殊族群），便於讀者快速定位。

**(5)「新增 / 變更 / 爭議」標記**

若文章本身有標注以下狀態，逐條列出：

- 本版新增的建議（new recommendation）
- 與前版相比修訂的建議（modified recommendation）— 並摘要變更內容
- 文中明確標注為「有爭議」或「委員會內部意見分歧」的建議
- 文中標注為「研究空缺」（knowledge gap）或「未來研究方向」的領域

**(6) 共識統計揭露（Consensus statement 必填）**

若為共識聲明，且文章揭露各題目同意比例，整理為：

| 編號 | 建議內容 | 第一輪同意比例 | 最終輪同意比例 | 是否達預設門檻 |
|---|---|---|---|---|
| R1 | ... | __% | __% | 是 / 否 |

若文章未揭露各題目同意比例：在此處明確標注「[嚴重透明度缺失：未揭露各題目同意比例]」，並將此項列入後續 A2.2 紅旗清單。

###### A2.1 Guideline 部分方法學評估

完成 A2.0 建議清單後，方可進入方法學批判：

- 是否由透明、利益衝突管理的委員會制定？COI 比例與管理機制是否公開？
- 底層證據是否經過系統性回顧？SR 是否為本指引團隊自行執行，或引用既有 SR？引用既有 SR 時，SR 的時效性與品質是否評估？
- 是否使用 GRADE 或等效架構評定證據品質與建議強度？
- Strong recommendation vs Conditional recommendation 的區分是否清楚？文中是否有「Strong recommendation based on Low-quality evidence」？若有，是否符合 GRADE 合理例外情境（life-threatening situation / uncertain but low-cost intervention / catastrophic harm avoidance / equivalent options / ethical imperatives）？
- 即使是強烈建議，是否討論可能因患者價值觀、共病、資源差異而不適用的情境？
- **AGREE-II 六大面向系統性評估**（每面向 1–7 分）：範疇與目的、利害關係人參與、制定嚴謹度、表達清晰度、適用性、編輯獨立性。
- 更新計畫：是否說明下一版時程？是否有 living guideline 機制？

###### A2.2 Consensus Statement / Delphi 部分方法學評估

- **共識方法是否明確**：Delphi（修正式 / 經典式）/ RAND-UCLA / Nominal Group Technique？方法選擇理由是否說明？
- **專家小組組成**：學科多元性、地理代表性、患者/公眾代表、方法學家/統計學家是否納入；COI 分布與投票迴避是否公開？
- **投票機制透明度**：匿名性、輪數、輪間回饋、停止規則、共識門檻（通常 ≥75% 為標準，文章採用幾%？）。
- **揭露完整度**：各題目同意比例、分歧點與少數意見、未達共識題目的處理。
- **共識 ≠ 證據**：作者是否誤將「我們同意 X」呈現為「證據顯示 X」？共識聲明語氣是否暗示等同於 evidence-based recommendation？
- 對照 A2.0 第 (3) 項：完全基於專家意見的強烈建議比例若過高，需重點批判。

---

##### A3. Narrative Review / Expert Opinion 子模組（敘事型，v3.2 新增）

**⚠️ 明確不適用的標準**

不要對 narrative review 套用以下要求——這些是 systematic review 的特徵，**不是 narrative review 的失敗**：

- ❌ PRISMA flow
- ❌ 預先指定的 PICO
- ❌ 雙人篩選 / κ 值
- ❌ 量化合成 / 異質性檢驗
- ❌ PROSPERO 註冊

對 narrative review 套用 systematic review 的標準，是**類別錯誤**。

**A3.1 適用的方法學評估：SANRA 框架**

(Scale for the Assessment of Narrative Review Articles, Baethge et al. Res Integr Peer Rev 2019)

| 項目 | 評估內容 | 0 分 | 1 分 | 2 分 |
|---|---|---|---|---|
| (1) Importance | 為何此主題對讀者重要？ | 未說明 | 暗示但未明示 | 明確論證 |
| (2) Aims | 研究問題或目的是否清楚陳述？ | 未陳述 | 部分陳述 | 明確、可操作的目的或問題 |
| (3) Literature search | 文獻搜尋是否描述？（即使非系統性） | 完全未提及 | 簡略提及（如「我們搜尋了 PubMed」） | 描述搜尋資料庫、關鍵字、時間範圍 |
| (4) Referencing | 關鍵陳述是否有引用支持？ | 多數關鍵陳述無引用 | 部分有引用 | 所有關鍵陳述皆有適當引用 |
| (5) Scientific reasoning | 科學推理是否合理？是否區分證據層級？ | 推理跳躍、混用證據層級 | 部分合理 | 推理嚴謹、明確區分證據強度 |
| (6) Data presentation | 證據與資料的呈現是否合宜？ | 未呈現具體數據 | 部分呈現 | 系統性呈現核心數據（含效果量、CI） |

**總分 0–12 分。建議分級：**

- **10–12 分**：高品質 narrative review
- **6–9 分**：中等品質
- **0–5 分**：低品質（須提高對結論的懷疑度）

依官方驗證資料（Baethge 2019），醫學期刊投稿 narrative review 平均 6.0 分（SD 2.6）。

**A3.2 Narrative Review 特有紅旗清單（超越 SANRA）**

**I. 結構性偏差訊號**

- 該主題是否已有近期 systematic review / meta-analysis？
  - 若有，作者為何選擇寫 narrative review 而非引用 SR？
  - 可能訊號：作者欲規避 SR 結論、或 SR 結論不利於作者立場
- **引用偏倚（citation bias）**：
  - 自引比例（self-citation rate）：作者引用自己文獻佔總引用之比例（> 15% 屬偏高）
  - 該領域知名反對意見是否被引用並認真討論？
  - 引用的年份分布：是否選擇性忽略某時期的關鍵研究？
- **證據層級的混用**：作者是否將 RCT、觀察性研究、動物實驗、專家意見以同等權重並列引用，未區分證據強度？
- **是否邀稿（invited review）**：邀稿通常 peer review 較寬鬆，且作者立場較受編輯偏好影響

**II. 敘事框架偏差**

- **機轉敘事的吸引力 vs 臨床數據的薄弱**：作者是否花大量篇幅描述生理機轉與動物模型，而對人體 hard outcome 的數據處理簡略？
- **Surrogate vs hard outcome 的轉換**：作者是否將 surrogate marker 的改善直接論述為臨床結果改善？
- **統計顯著 → 臨床顯著的隱含跳躍**：作者引用「p < 0.05」的研究時，是否同時討論效果量、CI、MCID？
- **反向證據的處理方式**：
  - 反向證據是否被引用？
  - 被引用時是否被輕描淡寫（"some studies suggest..."、"a few small studies"）或被歸因於方法缺陷
  - 而正向證據從不受到同等質疑？

**III. 作者立場與利益訊號**

- 作者是否為該領域某特定立場或學派的代表人物？（與 SECTION-0 第 1 節「團隊過往研究」與「作者立場史」對照）
- 結論是否與作者主要研究方向或產品有利益相關？
- 文章是否為某共識會議或工作坊的延伸產物？若是，會議贊助者是否影響觀點？

**IV. 圖表的「準量化」偽裝**

- **未經系統性合成的 forest-plot-like 圖表**：narrative review 內鑲嵌「擬 meta-analysis」視覺化是常見偏差訊號，讓讀者誤以為這是量化合成
- **不同 outcome metrics 共同繪製**：將不同 outcome metrics 的研究共同繪製，造成可比性錯覺（例如同時呈現 HR、OR、RR 而未轉換或標注）
- **整合圖（integrative figure）**：是否將機轉假說與臨床證據在視覺上等同呈現？
- **時間軸或概念圖**：是否將假說性連結畫成箭頭，給讀者「已建立」的錯覺？

**V. 自我宣稱的誠實度**

- 作者是否在文章中**明確標示**這是 narrative review？
- 是否承認其方法學限制（非系統性、可能存在選擇偏差）？
- 是否聲明本文「不應作為臨床決策的單一證據基礎」？
- 還是隱性地以權威語氣呈現結論，模糊讀者對證據強度的判斷？

**A3.3 高品質 Narrative Review 的合理應用範疇**

Narrative review 不必然是壞的。它適用於：

- **廣度型主題**：單一 SR 無法涵蓋的整合性議題（例如「心衰竭的病理生理整合觀」）
- **機轉導向的領域整合**：將分散在多領域的機轉證據串接
- **新興領域**：證據量不足以做 SR，但需要對現有零散證據做領域導讀
- **教育性綜述**：對受訓者進行領域入門教育，非用於指導臨床決策
- **概念整合（conceptual synthesis）**：例如提出新的疾病分型框架，需論述而非統計

**評讀時應同時評估**：作者是否正在做以上五類合理工作？還是試圖以 narrative review 形式偷渡未經系統合成的「臨床建議」？

---

#### B. 論證架構重建（第一階段——純描述，不評判）

**主論點**：作者最終想說服讀者相信什麼？

**論證主幹（樹狀結構）**

```
1. 前提 A：[作者的主張]
   └─ 支撐依據：[引用來源與內容摘要]
   └─ 推論步驟：A → B，因為...

2. 前提 B：[作者的主張]
   └─ 支撐依據：[引用來源]
   └─ 推論步驟：B → C，因為...

3. 中間結論：A + B → [作者的中間結論]

4. 最終結論：[作者的主張]
```

**敘事策略**：作者用什麼方式組織論點？（時間序 / 機轉到應用 / 問題到解方 / 比較不同假說）

**作者承認的限制**：文內有沒有自我揭露的侷限？列出原文位置。

#### C. 引用品質評估（第二階段——開始批判）

針對 B 的每個主要論點，逐一評估：

| 論點編號 | 引用文獻類型 | 證據層級 | 與論點關聯性 | 問題 |
|---|---|---|---|---|
| 1 | RCT / 觀察性 / 動物 / 細胞 / 專家意見 | 高/中/低 | 直接/間接/類推 | [具體問題] |

**引用品質紅旗**

- 是否大量引用自己過去的研究？
- 是否忽略重要的反面文獻？
- 是否用動物/細胞研究直接推論人體效果？
- 是否引用二手來源（引用別人的引用）？
- 引用的研究族群是否與結論對象一致？

#### D. 過度推論分析

針對每個可疑的推論跳躍：

**推論 [編號]**

- 原始引用實際說了什麼：（附頁碼/引用來源）
- 作者的結論說了什麼：（附頁碼）
- 跳躍在哪裡：
- 跳躍的嚴重程度：
  - 輕微 = 合理延伸但需更多證據
  - 中度 = 跨越了重要條件限制
  - 嚴重 = 引用內容根本不支持該結論

#### E. 論證結構完整性評估

- 是否有未被支撐的前提（hidden assumptions）？
- 論點之間是否有邏輯斷層？
- 作者是否忽略了能動搖結論的替代解釋？
- 結論是否超出文內證據所能支持的範圍？

**替代解釋**：提出 2–3 個作者未討論、但文內線索暗示可能存在的解釋。

#### F. 最終裁決

- 論證結構完整性：強 / 中 / 弱
- 引用品質：適當 / 部分適當 / 明顯不足
- 過度推論嚴重程度：無 / 輕微 / 中度 / 嚴重
- 可採用這篇文章的情境：
- 不建議直接引用的情境：
- 若要改觀，最需要：
  1.
  2.
  3.
- 一句話結論（20–40 字，給一般讀者）

**Narrative Review 專屬欄位（v3.2 新增）**

- **SANRA 總分**：__ / 12（分級：高 / 中 / 低品質）
- **文章類型自我宣稱的誠實度**：
  - 作者是否明確標示這是 narrative review？
  - 還是以「review」這個含糊用詞，模糊地讓讀者誤以為這是系統性回顧？
- **該主題的證據生態系定位**：
  - 此 narrative review 在該主題的證據體系中扮演什麼角色？
  - （補充 SR 的縫隙？教育性？立場宣示？機轉整合？）
- **適合的引用方式**：讀者引用這篇文章時，應視為：
  - [ ] 領域導讀（reference for orientation only）
  - [ ] 機轉論述的整合（mechanistic synthesis, not clinical recommendation）
  - [ ] 立場文件（position paper）
  - [ ] 概念整合（conceptual framework proposal）
  - [ ] 不建議作為臨床決策的證據基礎

**CPG / Consensus 專屬欄位（v3.3 新增）**

- **建議清單完整性**：A2.0 所列建議總數 = __ 條
- **建議強度 vs 證據品質的整體一致性**：
  - 「強烈建議 + 低證據品質」佔比：__%
  - 「強烈建議 + 完全基於專家意見」佔比：__%
  - 整體評估：合理 / 部分合理 / 明顯越界
- **AGREE-II 六面向綜合評分**：總分 __ / 42（若做了完整評估）
- **共識統計揭露**（consensus 適用）：
  - [ ] 完整揭露各題目同意比例
  - [ ] 部分揭露（僅總體統計）
  - [ ] 完全未揭露（嚴重透明度缺失）
- **本指引在臨床決策中的合理角色**：
  - [ ] 直接採用為臨床決策依據
  - [ ] 採用為臨床決策參考，但需個體化調整
  - [ ] 採用為「現階段共識立場」的參考，但證據基礎薄弱
  - [ ] 不建議直接採用，需等待更高品質證據

**SKILL-B 輸出順序總表（CPG / Consensus 適用）**

當文章類型為 CPG 或 Consensus 時，SKILL-B 輸出順序為：

```
SECTION-0（通用描述）
  ↓
A. 補充技術細節
  ↓
A2.0 文章建議清單完整重建
  ↓
A2.1 / A2.2 方法學評估（規範型）
  ↓
B. 論證架構重建（聚焦於「為什麼這樣建議」的論證鏈）
  ↓
C. 引用品質評估
  ↓
D. 過度推論分析（聚焦於「建議強度 vs 證據品質」的落差）
  ↓
E. 論證結構完整性評估
  ↓
F. 最終裁決（含 CPG / Consensus 專屬欄位）
```

---

## SKILL-C：診斷研究與臨床預測模型評讀

### ROLE

臨床診斷測試方法學顧問。評估診斷準確度研究的內部效度、LR 的臨床解讀、AI 模型的三階段評估，以及預測模型的辨別力與校準度。

### CORE RULES

- 不能只看 sensitivity/specificity，必須同時計算並解讀 likelihood ratio（LR）。
- LR 的臨床意義分級：
  - LR > 10 或 < 0.1 → 大幅改變診斷機率（大多情況下具決策意義）
  - LR 2–5 或 0.2–0.5 → 中度改變
  - LR 接近 1 → 測試幾乎不提供額外資訊
- 預測模型同時評估辨別力（AUC/C-statistic）與校準度（calibration plot）——兩者皆必要。
- AI/ML 模型必須完成三階段評估：derivation → validation → clinical effectiveness。

### SELF-CHECK（輸出前執行，不顯示）

- [ ] LR+ 與 LR− 是否都有計算或引用？
- [ ] Reference standard 的獨立性與有效性是否評估？
- [ ] 若為 ML 模型，三階段評估是否都有覆蓋？
- [ ] 若為預測模型，辨別力與校準度是否都有評估？

### OUTPUT FORMAT — SKILL-C

#### A. 補充技術細節

基本資料（PICO、設計、資助）已詳述於 SECTION-0 第 3–6 節，此處僅補充：

- Reference standard：是否為公認的黃金標準？是否獨立於 index test 施測？
- Index test 的施測者是否對 reference standard 結果保持盲目？（反之亦然）
- 受試者納入的疾病嚴重度分布：是否避免只納入明顯正常 vs 明顯異常者（spectrum bias）？
- Cut-off 設定方式：是否預先指定？是否源自同一資料集（optimism bias）？
- 測試結果的操作型定義與可重複性：

#### B. 診斷準確度核心評估

**標準診斷準確度**（若文章類型為 diagnostic accuracy study）

| 指標 | 數值 | 95% CI | 臨床解讀 |
|---|---|---|---|
| Sensitivity | | | |
| Specificity | | | |
| LR+ | | | |
| LR− | | | |
| AUC / C-statistic | | | |

若文內僅報告 sensitivity/specificity，自行計算 LR+ = sensitivity / (1 − specificity)；LR− = (1 − sensitivity) / specificity。標注計算過程。

**Pre-test probability 與 post-test probability 的臨床情境**：

- 在低 / 中 / 高盛行率（pre-test probability）下，此測試的臨床用途分別為何？
- 用 Fagan nomogram 概念說明（可用文字描述，不需繪圖）

**Clinical Manifestations / History & Physical 子模組**（若適用）

- 疾病確認是否獨立於所研究的臨床表徵？
- 樣本是否代表完整的疾病表現譜系？
- 臨床表徵的搜索是否系統性且可重複？
- 結果是否以比例 + CI 呈現？

#### C. AI / ML 診斷模型評估（若適用）

**三階段評估**

**① 建模（Derivation）**

- 任務定義是否明確？Reference standard 是否合適？
- 是否有 overfitting 的防護（train/validation split、regularization、cross-validation）？
- 特徵選擇是否透明？

**② 驗證（Validation）**

- 是否有來自不同機構或不同時間的外部驗證資料集？
- 驗證資料集的 reference standard 是否與建模階段一致？
- **僅有內部驗證的模型不足以推薦臨床使用。**

**③ 臨床有效性（Clinical Effectiveness）**

- 是否有前瞻性試驗證明此模型改善了臨床醫師的決策或患者結果？
- 是否分析 failure modes（模型在哪些情況下會出錯）？
- 是否評估不同人口學次組的模型表現（公平性 / fairness）？
- 模型的可解釋性（interpretability）是否評估？

#### D. 臨床預測模型評估（若適用）

**辨別力（Discrimination）**

- AUC / C-statistic：模型能否區分發生結果與未發生結果的個體？
- AUC 是否有 95% CI？是否在外部驗證集上報告？

**校準度（Calibration）**

- 是否有 calibration plot？預測機率與實際發生率是否一致？
- 是否報告 Hosmer-Lemeshow test 或等效統計量？
- 一個高辨別力但校準差的模型，其絕對風險估計是不可靠的。

**外部驗證**

- 僅有內部驗證的預測模型：可信度有限，不應直接推廣臨床使用。
- 是否有時間驗證（temporal validation）或地理驗證（geographic validation）？

#### E. 紅旗清單

條列（設計 / 統計 / 敘事 / 外推）

#### F. 最終裁決

- 診斷測試可信度：高 / 中 / 低
- 在何種臨床情境下此測試有用（pre-test probability 範圍）？
- 不建議使用的情境：
- 若要改觀，最需要的 3 個關鍵改進：
- 一句話結論（20–40 字，給一般讀者）

---

## APPENDIX：因果推論補充評估框架（觀察性研究適用）

當觀察性研究宣稱因果效果時，額外應用以下框架：

### Target Trial 思維框架

- 如果這個問題用隨機對照試驗來回答，理想的試驗設計是什麼？
- 將觀察性研究對應到此假想試驗，找出哪些關鍵條件無法滿足。

### 三大可識別性條件

| 條件 | 評估問題 |
|---|---|
| Exchangeability（無未測混淆） | 所有相關混淆因子是否都已測量並調整？殘餘混淆的可能方向是什麼？ |
| Positivity（正概率假設） | 在所有共變量層中，每種治療的分配機率是否都 > 0？有無結構性或隨機性 positivity violation？ |
| Consistency（一致性） | 治療版本是否明確定義？「標準治療」作為比較組是否定義不清（treatment version irrelevance）？ |

### Time-varying Confounding（若適用）

- 在縱貫研究中，時間變動的混淆因子是否有適當處理（marginal structural models、g-formula）？
- 是否有 time-varying treatment 且混淆因子會被 treatment 影響（中介偏差風險）？

---

## APPENDIX：特殊研究設計補充

### 社群媒體 / 平台資料研究

- 研究問題是否適合用平台資料回答（而非更適合用原始研究）？
- 資料收集是否系統性且可重複？
- 結果是否以平台原生指標有效操作化？
- 注意平台特有偏差：使用者選擇偏差、演算法混淆、survivorship bias。

### 質性研究

四大效度標準：

- 參與者選擇：是否為目的性取樣，與研究問題相關？
- 資料收集方式：是否合適（訪談、焦點團體、觀察、文件）？
- 資料收集過程：是否充分達到飽和（saturation）？
- 分析：是否系統性，並有多研究者或多來源的三角驗證（triangulation）？

### 人道主義 / 緊急情境死亡率研究

- 研究問題是否明確（粗死亡率 vs 特定原因死亡率）？
- 分母（人口基數）的估算方式是否說明？
- 資料收集方法與其局限是否描述？
- 是否考慮系統性的計數不完整（誰被計入、誰被遺漏）？

---

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
