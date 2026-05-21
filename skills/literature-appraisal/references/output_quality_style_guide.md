# Literature Appraisal Output Quality Style Guide

This guide supplements `SKILL.md` with output-quality targets distilled from high-quality appraisals across RCTs, non-inferiority trials, observational/post-hoc analyses, diagnostic/device validation studies, systematic reviews/network meta-analyses, and consensus statements.

`SKILL.md` is the source of truth for routing, SECTION-0 order, input visibility, language rules, and table-use scope. If this guide appears to conflict with `SKILL.md`, follow `SKILL.md`.

## Core Output Contract

Every appraisal must:

1. Follow `SKILL.md` routing and SECTION-0 order.
2. Preserve exact effect sizes, denominators, confidence intervals, margins, event counts, and subgroup sample sizes when available.
3. Avoid `p > 0.05 = no difference`; classify uncertainty with CI + MCID or fit-for-purpose equivalence/non-inferiority logic where relevant.
4. Identify where authors' clinical or policy claims exceed the evidence.
5. Provide alternative explanations and a final clinical-use verdict.
6. Include a red-flag list and "若要改觀，最需要的 3 個關鍵改進".

Preferred tone: precise, clinically opinionated, evidence-grounded, and skeptical without being performative. Use concrete numbers instead of vague adjectives.

## SECTION-0 Quality Checklist

Use the SECTION-0 order defined in `SKILL.md`. Within the relevant SKILL.md sections, make sure the appraisal captures:

- Article identity, type routing, and any registration number when relevant.
- Author/team fit, external context, funding, sponsor role, and COI implications.
- Received/accepted/published dates, journal context, editorial/commentary availability, and any limits of external search.
- PICO, endpoint definitions, statistical plan, multiplicity plan, missing-data handling, and source population.
- Exact primary and clinically important secondary results, including denominators, event counts, confidence intervals, subgroup sample sizes, and safety signals.
- CI + MCID interpretation where relevant; for non-inferiority/equivalence, classify the full CI against the prespecified margin.
- Where authors' discussion or clinical implications exceed the data.
- A practical final clinical-use verdict.

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
- Before critique, create `A2.0 文章建議清單完整重建`: extract all concrete article recommendations visible in the markdown item-by-item in article order. Do not omit, merge, or summarize visible recommendations away; if markdown extraction is incomplete, state the limitation instead of inventing missing recommendations.
- For each recommendation: translate/paraphrase the operative recommendation into Traditional Chinese, preserving necessary English medical terms only; include location, supporting evidence type, and certainty.
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
   - Up to 3 plausible alternative explanations, each with supporting and opposing clues. If fewer than 3 are credible from the available text, do not invent extras; state the limitation.
3. `最終裁決`
   - Overall credibility: high / moderate / low-moderate / low.
   - What can be used now.
   - What should not be used.
   - Three changes that would change confidence.
4. `一句話結論`
   - One plain-language, clinically useful conclusion.

## Quality Bar

The output should feel like an expert journal club note, not a generic evidence summary. A high-quality appraisal usually contains:

- Tables when needed to preserve numeric precision.
- Use tables only when they preserve numeric precision or make comparison easier; do not add decorative or low-information tables.
- Enough prose to explain why the numbers matter.
- At least one strong, specific methodological insight.
- At least one clinically actionable caution.
- No unsupported certainty.
