# Classical Rectangle Public Validation — Frozen Protocol

Status: implementation complete; provider acquisition pending
Date: 2026-07-17
Execution boundary: local research only; non-executable

## Decision

Run the already-frozen rectangle detector and its two predeclared Last Full
Day stop-buffer variants on a fresh five-year Public daily-bar snapshot. This
is a **config-only hypothesis replay after data plumbing**, not a retune and not
a new entry rule. Calibration, validation, and holdout keep the dates already
declared in `rectangle_daily_v1.yaml`:

- calibration through 2022-12-31;
- validation from 2023-01-01 through 2024-12-31;
- holdout from 2025-01-01 onward.

No detector threshold, exit rule, universe member, split boundary, or reported
variant may change after the Public outcomes are observed.

## What the Experiment Can Establish

The run can establish whether the frozen implementation shows an economic
signal in the 43-symbol cohort that was selected before this provider run. It
can compare calibration, validation, and holdout without letting Suman's chart
comments curate the economic population.

It cannot establish population alpha. The cohort contains symbols known and
available today rather than a point-in-time historical market universe. Public
also does not document its corporate-action adjustment policy. The acquisition
therefore checks known split transitions empirically and labels the result
`frozen_cohort_validation_not_population_alpha`.

## Immutable Inputs

- Detector config: `config/classical_patterns/rectangle_daily_v1.yaml`
- Frozen cohort: `config/classical_patterns/public_validation_universe_v1.json`
- Required semantic gate: `MalaRectangleSemanticSpecFreezeV1` with status
  `frozen`, no trade-worthiness fields, and no economic filtering authority
- Provider request: `EQUITY/{symbol}/FIVE_YEARS/ONE_DAY`
- Population rule: every causal signal emitted by the frozen enumerator
- Variants: the stop buffers already declared by the config

The universe file is hash-bound to the detector config. The provider dataset
retains canonical raw responses, a current instrument-catalogue snapshot,
normalized Parquet bars, per-file hashes, content hashes, coverage gaps, and
known-split continuity checks. Access tokens are minted in memory and are not
written to the dataset.

## Fail-Closed Gates

Acquisition or replay stops when any of the following is true:

- the Git tree is dirty;
- the universe config hash differs from the detector config;
- a symbol has no bars or less than 98% expected NYSE-session coverage;
- a provider bar lands on an unexpected session;
- the current catalogue does not contain exactly one equity identity for a
  cohort symbol;
- a known split transition exceeds its declared continuity tolerance;
- a raw, normalized, universe, dataset, or semantic-freeze hash changes;
- the semantic freeze permits review-based economic filtering.

## Reproduction

The credential is sourced from the existing Public setup only for the process.
It must not be copied into Mala files or printed.

```bash
uv run --frozen python -m src.research.classical_patterns.runner acquire-public-daily \
  --universe config/classical_patterns/public_validation_universe_v1.json \
  --output-dir research/results/playbooks/classical_pattern_lab/public_validation_round_1/dataset_public_43_v1

uv run --frozen python -m src.research.classical_patterns.runner run-public-daily \
  --dataset-dir research/results/playbooks/classical_pattern_lab/public_validation_round_1/dataset_public_43_v1 \
  --semantic-freeze research/results/playbooks/classical_pattern_lab/semantic_round_3/review_batch_v3_base_r1b/source_fidelity_v3/decisions/mala_rectangle_semantic_freeze_v1.json \
  --run-id rectangle-public-43-v1 \
  --output-dir research/results/playbooks/classical_pattern_lab/public_validation_round_1/economic_public_43_v1
```

## Interpretation Rule

Read validation and holdout before calibration when deciding whether the
pattern merits another round. A favorable calibration cell that fails to
persist is not alpha. A favorable holdout with too few closed trades is
insufficient evidence. All directions and both variants remain visible; no
winning slice may be promoted alone without a new, predeclared experiment.

The next decision is one of:

1. **kill** — validation and holdout do not support positive net expectancy;
2. **replicate** — a predeclared effect persists but needs a broader licensed
   point-in-time universe or a second provider;
3. **new hypothesis** — a specific semantic or mechanical change is proposed
   as version 2, with fresh calibration and an untouched future holdout.

None of these decisions authorizes Bhiksha, options translation, shadow, or
live trading.
