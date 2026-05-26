# Literature Appraisal Output Quality Style Guide

Supplementary cross-cutting quality targets. The route-specific requirements
(Study-Type Modules, Required Final Sections) live in `SKILL.md` and the
relevant `fragments/skill_*.md`; this guide intentionally does not repeat them.

`SKILL.md` is the source of truth. If this guide appears to conflict with
`SKILL.md` or a fragment, follow `SKILL.md` / the fragment.

## Core Output Contract

Every appraisal must:

1. Follow `SKILL.md` routing and SECTION-0 order, plus the route's fragment.
2. Preserve exact effect sizes, denominators, confidence intervals, margins,
   event counts, and subgroup sample sizes when available.
3. Avoid `p > 0.05 = no difference`; classify uncertainty with CI + MCID or
   fit-for-purpose equivalence / non-inferiority logic where relevant.
4. Identify where authors' clinical or policy claims exceed the evidence.
5. Provide alternative explanations and a final clinical-use verdict (the
   exact section labels and contents are defined per-route in the fragment).

Preferred tone: precise, clinically opinionated, evidence-grounded, and
skeptical without being performative. Use concrete numbers instead of vague
adjectives.

## Cross-Cutting Critique Patterns

Always look for, regardless of route:

- Claim / evidence mismatch.
- Hidden assumptions.
- Outcome hierarchy mismatch.
- Composite endpoints with unequal patient importance.
- Multiplicity without adjustment.
- Subgroup over-interpretation without interaction testing.
- Sponsor / manufacturer role, in-kind product supply, and commercial narrative
  alignment.
- Prior commitment bias from the same research group repeatedly publishing
  from the same dataset.
- Lack of independent replication.
- External validity: geography, race / ethnicity, center expertise, operator
  volume, standard-of-care level.

## Quality Bar

The output should feel like an expert journal-club note, not a generic
evidence summary. A high-quality appraisal usually contains:

- Tables only when they preserve numeric precision or enable comparison;
  do not add decorative or low-information tables.
- Enough prose to explain why the numbers matter.
- At least one strong, specific methodological insight.
- At least one clinically actionable caution.
- No unsupported certainty.
