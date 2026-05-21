# Literature Appraisal Output Quality Style Guide

This guide defines the expected output quality for full literature appraisals. It is distilled from high-quality appraisals across RCTs, non-inferiority trials, observational/post-hoc analyses, diagnostic/device validation studies, systematic reviews/network meta-analyses, and consensus statements.

## Core Output Contract

Every appraisal must:

1. Route the article type explicitly before appraisal.
2. Produce `SECTION-0：文章全面描述` before study-type critique.
3. Separate facts by source label: `[文內]`, `[外部]`, `[推論]`, `[無法取得]`.
4. Preserve exact effect sizes, denominators, confidence intervals, margins, event counts, and subgroup sample sizes when available.
5. Avoid `p > 0.05 = no difference`; classify uncertainty with CI + MCID or fit-for-purpose equivalence/non-inferiority logic.
6. Identify where authors' clinical or policy claims exceed the evidence.
7. Provide alternative explanations and a final clinical-use verdict.
8. Include a red-flag list and "若要改觀，最需要的 3 個關鍵改進".

Preferred tone: precise, clinically opinionated, evidence-grounded, and skeptical without being performative. Use concrete numbers instead of vague adjectives.

## Required SECTION-0 Structure

Use this order unless the article type makes a section irrelevant:

1. `文章身份證`
   - Title, journal, year/issue/pages, DOI/PMID, registration number if relevant, article type and routing.
2. `作者團隊評論`
   - Corresponding author identity, institution, field fit, prior work, trial/network ownership, sponsor or device/drug-company role.
   - Use external search where required. If not found, say `[無法取得]`.
3. `文獻 Metadata 整理`
   - Received/revised/accepted/published dates, review duration, journal IF/category, OA status, conference-synchronous publication, editorial/commentary availability.
4. `研究背景`
   - Existing facts, knowledge gap, and research question.
5. `研究設計與方法`
   - PICO, design, setting, dates, sample source, randomization/exposure/test/reference standard, blinding, endpoint definitions, statistical plan, multiplicity plan.
6. `研究結果重點整理`
   - Baseline balance or case-mix, primary result table, clinically important secondary results, safety, missing data/failures, subgroup signals.
7. `結果分類（CI + MCID 框架）`
   - Identify prespecified MCID/margin when present.
   - If absent, clearly label a clinically reasonable threshold as `[推論]`.
   - Classify as POSITIVE, NEGATIVE/HARMFUL, INCONCLUSIVE, IMPRECISE, FAILED NON-INFERIORITY, or BORDERLINE as appropriate.
8. `作者討論與評論重點`
   - What authors claim, what they acknowledge, what they omit, and whether the clinical implication follows.
9. `外部專業意見`
   - Editorials, journal scans, conference discussion, independent critiques, competing meta-analyses, guideline context. State `[無法取得]` if none found.
10. `臨床建議`
    - `可採用情境`, `不建議直接套用`, `給臨床醫師的實務建議`, and evidence-strength rating.

## Study-Type Modules

### RCT

Required critique points:
- Randomization, allocation concealment, blinding, endpoint adjudication.
- ITT, modified ITT, per-protocol, attrition, crossover, adherence, missing data.
- Composite endpoint coherence and patient importance.
- Multiplicity and hierarchy.
- Event timing and KM curve concerns: numbers at risk, crossing curves, informative censoring.
- Effect size plausibility: check Type M error / Winner's Curse when observed effects are much larger than prior trials or power assumptions.
- Placebo/control validity: inertness, active harms, standard-of-care adequacy.

Output must include:
- Underpowered/overpowered judgment.
- Subgroup credibility table when subgroup claims matter.
- Red flags and alternative explanations.

### Non-Inferiority / Equivalence Trial

Required critique points:
- State the prespecified non-inferiority/equivalence margin and whether it is clinically justified.
- Translate absolute margin into relative risk increase when helpful.
- Compare the entire CI against the margin, not only p values.
- Distinguish:
  - `non-inferiority proven`
  - `failed non-inferiority`
  - `inconclusive`
  - `inferiority proven`
  - `superiority proven`
- Failed non-inferiority is not proof of superiority unless the hierarchy permits and statistical criteria are met.
- ITT and per-protocol should be directionally consistent for non-inferiority.
- Check assay sensitivity and whether the active control effect is preserved.
- Identify overly generous margins, open-label ascertainment, crossovers, and selective superiority language.

Preferred phrasing:
> 本研究既未證明 non-inferiority，也未證明 superiority；訊號方向偏向 X，但效果大小仍不確定。

### Observational / Post-Hoc / Registry / Target-Trial-Like Analysis

Required critique points:
- Identify whether exposure groups are truly randomized or post-hoc/observational.
- Confounding by indication, reverse causality, immortal time bias, landmark/survivor bias, period effects, detection bias.
- Whether key baseline imbalances remain after adjustment.
- Whether the model adjusts for the variables that actually determine exposure.
- Missing-data method: avoid accepting mean imputation uncritically.
- Distinguish prognostic marker from causal mediator.

Do not allow:
- "Trajectory predicts outcome" -> "changing trajectory improves outcome" without intervention evidence.
- HR per unit exposure interpreted as pure clinical-action cost when exposure is confounded.

### Diagnostic Accuracy / Device Validation / Method-Comparison Study

Required critique points:
- Define index test, comparator, reference standard, and whether the reference is independent.
- For continuous measurement devices, prefer Bland-Altman, mean difference, SD/limits of agreement, and accepted validation standards over correlation alone.
- For BP devices, benchmark against ISO 81060-2 / AAMI/ESH/ISO when applicable: mean difference <= +/-5 mmHg and SD <= 8 mmHg, plus population/cuff/observer requirements.
- Check spectrum bias, verification bias, failed measurements, exclusion of unreadable tests, and subgroup sample sizes.
- For ML/device studies, assess:
  - training data transparency
  - feature/model selection
  - independent external validation
  - failure-mode analysis
  - fairness/subgroup performance
  - manufacturer involvement and patent/commercial conflicts
- Treat "p > 0.05 for comorbidity coefficient" as insufficient to claim "unaffected by comorbidities"; require equivalence/ROPE or narrow CI within clinical tolerance.

Output must include:
- Whether the study supports regulatory/standard validation, clinical adoption, or only feasibility.

### Systematic Review / Meta-Analysis / Network Meta-Analysis

Required critique points:
- Protocol registration, search completeness, inclusion/exclusion, dual screening, RoB tool, certainty framework.
- GRADE/CINeMA factors: RoB, inconsistency, indirectness, imprecision, publication bias.
- For NMA:
  - transitivity and node definitions
  - direct vs indirect evidence
  - sparse nodes and single-study drivers
  - inconsistency tests
  - ranking metric limitations (P-score/SUCRA does not equal clinical priority)
  - leave-one-out sensitivity
  - whether conclusions rely on surrogate/dose-finding/short-term studies
- Check whether authors use ranking to create guideline-level claims.

Required output:
- Table mapping major author claims to actual evidence strength.
- Explicit "over-inference" analysis for claims such as "safest", "best", or "guideline should prefer".

### Guideline / Consensus / Recommendation Statement

Required critique points:
- Distinguish formal guideline with GRADE from expert consensus.
- Before critique, create `A2.0 文章建議清單完整重建`: extract all concrete article recommendations item-by-item in article order. Do not omit, merge, or summarize recommendations away.
- For each recommendation: quote/summarize the operative recommendation, location, supporting evidence type, and certainty.
- Count how many recommendations are supported by RCTs, observational data, mechanistic reasoning, or pure expert consensus.
- Flag recommendations with strong language but weak evidence.
- For consensus statements, report item-level agreement percentages when available; if unavailable, mark this as a transparency problem.

Useful structure:
- `R1`, `R2`, ... itemized recommendations by article section.
- Summary table: recommendation count by evidence type.
- Practical algorithm only if directly supported by the article.

## Cross-Cutting Critique Patterns

Always look for:

- Claim/evidence mismatch.
- Hidden assumptions.
- Outcome hierarchy mismatch.
- Composite endpoints with unequal patient importance.
- Multiplicity without adjustment.
- Subgroup over-interpretation without interaction testing.
- Sponsor/manufacturer role, in-kind product supply, and commercial narrative alignment.
- Prior commitment bias from the same research group repeatedly publishing from the same dataset.
- Lack of independent replication.
- External validity: geography, race/ethnicity, center expertise, operator volume, standard-of-care level.

## Required Final Sections

End with:

1. `紅旗清單`
   - Separate design, statistics, narrative, COI, and external-validity flags when useful.
2. `反向解釋`
   - At least 3 plausible alternative explanations, each with supporting and opposing clues.
3. `最終裁決`
   - Overall credibility: high / moderate / low-moderate / low.
   - What can be used now.
   - What should not be used.
   - Three changes that would change confidence.
4. `一句話結論`
   - One plain-language, clinically useful conclusion.

## Quality Bar

The output should feel like an expert journal club note, not a generic evidence summary. A high-quality appraisal usually contains:

- Enough tables to preserve numeric precision.
- Enough prose to explain why the numbers matter.
- At least one strong, specific methodological insight.
- At least one clinically actionable caution.
- No unsupported certainty.
