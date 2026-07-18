# Classical Pattern Lab — Semantic Review Round 1

Status: Round 2 reviewed; human trade-curation gate retired
Date: 2026-07-17
Playbook: `classical-rectangle-breakout-daily@1`
Readiness: semantics frozen; Public frozen-cohort validation implementation ready; not executable

## What This Round Proves

This round asks one narrow question before any backtest claim:

> At the close shown on each chart, did the deterministic machine identify a
> genuine horizontal rectangle, faithful boundaries, a completed breakout,
> and the correct Last Full Day risk reference?

The packet contains ten 2022 signals: five long and five short, spread across
ten symbols and all four quarters. The cohort is deliberately enriched for
positive detector signals. It can measure candidate precision and anchor
fidelity; it cannot measure false negatives or recall.

## Data Boundary

The source window is 2021-09-01 through 2022-12-31 so January 2022 signals have
prehistory. Review eligibility is 2022-01-01 through 2022-12-31.

- 23 audited symbols.
- All 251 expected 2022 NYSE sessions present for every cohort symbol.
- Zero duplicate timestamps or invalid/null/non-finite OHLCV failures.
- Calendar-aware handling retains the six valid 2022–2024 13:00 ET closes that
  the earlier fixed 300-bar rule discarded.
- Provider and adjustment provenance remain unverified at file level.
- The cache is a present-day symbol collection, not a point-in-time universe.

Therefore the cache is suitable for this local blind semantic pilot, but not
for alpha, robustness, or promotion claims. NYSE's published calendar defines
regular early closes at 13:00 ET: <https://www.nyse.com/trade/hours-calendars>.

## Outcome-Hidden Contract

The generator imports the causal enumerator and daily bars only. It does not
read lifecycle outcomes, trades, P&L, MFE/MAE, economic reports, or the prior
backtest receipt.

Each card contains:

- bars ending exactly at the close-confirmed signal;
- central boundaries and the tolerance envelope;
- causal touch markers;
- highlighted breakout bar and Last Full Day;
- geometry facts available at the cutoff;
- an editable response row keyed by config, card, chart, and source hashes.

The review chart deliberately omits structural objective, subsequent bars,
trade fills, and all economic results. Sampling uses direction, lookback,
geometry band, as-of diagnostic band, symbol spread, and a fixed hash seed—no
future result.

## Current Review Packet

Generated root:

```text
research/results/playbooks/classical_pattern_lab/semantic_round_1/
  readiness/
    DATA_READINESS.md
    data_readiness.json
  review_batch_2022_v1/
    REVIEW_INDEX.md
    batch_manifest.json
    batch_receipt.json
    review_responses.template.csv
    review_responses.csv
    cards/
    charts/
```

Only `review_responses.csv` is editable. The immutable template remains hashed
so the response sheet can always be restored without changing the batch.

## Regeneration

```bash
symbols="SPY,QQQ,IWM,DIA,GLD,TLT,AAPL,MSFT,NVDA,AMZN,META,TSLA,JPM,XOM,UNH,WMT,ABNB,AMAT,AVGO,CRM,GOOGL,GS,XLE"
batch_root="research/results/playbooks/classical_pattern_lab/semantic_round_1/review_batch_repro_$(date -u +%Y%m%dT%H%M%SZ)"

uv run --frozen python -m src.research.classical_patterns.runner audit-cache \
  --symbols "$symbols" \
  --start 2021-09-01 \
  --end 2022-12-31 \
  --output-dir research/results/playbooks/classical_pattern_lab/semantic_round_1/readiness

uv run --frozen python -m src.research.classical_patterns.runner semantic-batch \
  --symbols "$symbols" \
  --start 2021-09-01 \
  --end 2022-12-31 \
  --eligibility-start 2022-01-01 \
  --eligibility-end 2022-12-31 \
  --readiness-json research/results/playbooks/classical_pattern_lab/semantic_round_1/readiness/data_readiness.json \
  --batch-id rectangle-semantic-calibration-v1-r1 \
  --batch-size 12 \
  --output-dir "$batch_root"

uv run --frozen python -m src.research.classical_patterns.runner verify-semantic-batch \
  --batch-dir "$batch_root"
```

The request is 12 cards, but the full eligible 2022 positive population is ten;
the generator reports the shortfall instead of silently importing another split.
Generation refuses a nonempty output directory, so every round receives a new
batch root rather than inheriting stale cards from an earlier attempt.

## Human Review

The preferred operator surface is one self-contained Lathi Bus card in Obsidian,
matching the earlier Playbook Tag Adjudication workflow. Render it with:

```bash
uv run --frozen python -m src.research.classical_patterns.runner render-obsidian-review \
  --batch-dir research/results/playbooks/classical_pattern_lab/semantic_round_1/review_batch_2022_v1 \
  --output research/results/playbooks/classical_pattern_lab/semantic_round_1/obsidian/classical_rectangle_adjudication_round_1.md
```

The projection embeds every SVG as a base64 data URI, so Lathi Bus publishes a
single note with no attachment dependency. Silence on a chart means agree; add
a pointy-bracket correction only when the machine read is wrong. Uncommented
charts remain accepted if the overall decision is revise.

The structured CSV remains the machine-side ingestion contract. It may also be
used directly by an operator or populated from a collected Obsidian response:

- `decision`: `accept`, `revise`, `reject`, or `ambiguous`;
- four fidelity fields: `yes`, `no`, `revise`, or `ambiguous`;
- corrections and reason codes when needed;
- reviewer, review timestamp, and both hidden/future attestations as `true`.

Allowed reason codes are `not_rectangle`, `insufficient_touch_structure`,
`trend_not_balance`, `boundary_misplaced`, `breakout_not_confirmed`,
`direction_wrong`, `lfd_misidentified`, `pattern_morphed`,
`chart_data_suspect`, `insufficient_context`, and `other`; separate multiple
codes with commas. Accepted rows require all four fidelity fields to be `yes`,
and revisions must include a correction or reason code.

Ingestion is append-only and idempotent:

```bash
uv run --frozen python -m src.research.classical_patterns.runner ingest-semantic-responses \
  --batch-dir research/results/playbooks/classical_pattern_lab/semantic_round_1/review_batch_2022_v1
```

It rejects stale hashes, unknown cards/enums, conflicting repeat decisions, or
missing hidden/future attestations. The resulting semantic scorecard contains
no economic fields.

## Next Decision

- If the ten examples are mostly faithful, add deterministic negative/near-miss
  observations before measuring recall.
- If boundaries or Last Full Day are systematically wrong, revise the doctrine
  and config under version 2, then create a new untouched semantic round.
- Do not run an economic holdout until semantic rules and a licensed
  point-in-time data manifest are frozen.

## Round 2 — Class-Hidden Calibration Packet

Round 2 is a separate, non-executable projection schema
(`ClassicalPatternSemanticCalibrationBatchV2`). It keeps the frozen
`classical-rectangle-breakout-daily@1` detector and every Round 1 artifact
unchanged. The new packet asks whether a reviewer can distinguish genuine
rectangles from close-but-not-tradeable and clearly non-rectangle as-of cases
without being told the detector's prior conclusion.

The private, hashed manifest contains three exact causal cohorts:

- `confirmed_signal`: one representative close-confirmed detector signal;
- `qualified_no_trigger`: a qualified base without a close-confirmed trigger;
- `rejected_geometry`: a case rejected for insufficient confirmed boundary
  touches.

The public cards and SVGs do not contain those labels, direction, boundaries,
rejection reasons, overlays, or a machine verdict. They show neutral raw daily
OHLC bars for the exact causal candidate window through the evaluation cutoff
and report its actual 21/41/61 displayed bars. This keeps the review target
aligned with the hidden detector case without claiming that the fixed window is
a natural base. The final pale band means
**evaluation cutoff**, not necessarily a breakout. Each response independently
records `strict_rectangle_validity` (`valid|invalid|ambiguous`) and
`as_of_trade_worthiness` (`trade|watch|no_trade|ambiguous`), keyed by batch,
card, reviewer ID, and review pass.

Cards use only current/past bars. The manifest retains source-slice hashes,
exact class counts, causal diagnostics for confirmed signals, and explicit
null diagnostics for geometry-rejected cases. It never reads lifecycle,
outcomes, trades, P&L, or backtest receipts. V2 refuses a class shortage rather
than silently substituting a different class or split.

Confirmed-signal diagnostics define anchor span as inclusive sessions and keep
two distinct prior-close counts: `prior_central_rail_excursion_count` is a
same-direction close beyond the central boundary, while
`prior_full_trigger_close_count` requires the exact directional confirmation
threshold (tolerance edge plus configured breakout buffer). A central-rail
excursion is therefore not retroactively called a trigger.

Example regeneration (the defaults request 6/6/6 cards):

```bash
uv run --frozen python -m src.research.classical_patterns.runner semantic-calibration-batch-v2 \
  --symbols "$symbols" \
  --start 2021-09-01 \
  --end 2022-12-31 \
  --eligibility-start 2022-01-01 \
  --eligibility-end 2022-12-31 \
  --readiness-json research/results/playbooks/classical_pattern_lab/semantic_round_1/readiness/data_readiness.json \
  --exclude-manifest research/results/playbooks/classical_pattern_lab/semantic_round_1/review_batch_2022_v1/batch_manifest.json \
  --batch-id rectangle-semantic-calibration-v2-r1 \
  --output-dir research/results/playbooks/classical_pattern_lab/semantic_round_2/review_batch_v2

uv run --frozen python -m src.research.classical_patterns.runner verify-semantic-calibration-batch-v2 \
  --batch-dir research/results/playbooks/classical_pattern_lab/semantic_round_2/review_batch_v2

uv run --frozen python -m src.research.classical_patterns.runner ingest-semantic-calibration-responses-v2 \
  --batch-dir research/results/playbooks/classical_pattern_lab/semantic_round_2/review_batch_v2
```

The generator permits configurable class counts with
`--confirmed-signal-count`, `--qualified-no-trigger-count`, and
`--rejected-geometry-count`; all three must be positive and each requested
count must be available after deterministic symbol/date/lookback de-duplication.
Use a fresh batch root for every round.

V2 ingestion is append-only and idempotent. Its identity is
`batch_id + card_id + reviewer_id + review_pass`, allowing independent second
passes without overwriting a prior reviewer/pass response. The resulting
scorecard remains semantic-only.

`--exclude-manifest` may be supplied again for each prior Round 1 or Round 2
manifest. V2 loads `signal_id` values from V1 and `source_id` values from V2
before sampling, then records manifest content hashes plus excluded source
identities and their hashes in its private exclusion contract—never an absolute
input path. Verification rejects an
unsupported or malformed prior manifest and proves selected public cards have
zero overlap with the exclusion set.

## Round 2 Disposition — Role Correction

The two blind Sol Ultra passes were useful, but the final Obsidian card asked
Suman the wrong question. Suman does not trade this style and is testing a
public Peter Brandt-inspired hypothesis; his answer to "would you trade this?"
cannot establish either source fidelity or future profitability.

The collected human response contained two impressions—AMAT `trade` and JPM
`ambiguous`—and no overall decision. These are retained as operator notes, not
canonical labels. Silence on WMT, META, TSLA, and GOOGL is not missing work and
will not be interpreted as acceptance.

V2 remains a valid historical calibration artifact, including its
`as_of_trade_worthiness` field, but that field is deprecated for doctrine and
economic selection. No response from V2 may add, remove, or weight an event in
the backtest population.

### Authority model for the next round

- **Peter-source fidelity:** audited against a frozen, cited public-source
  rubric by independent outcome-blind reviewers.
- **Entry state:** computed by the deterministic detector from the frozen
  geometry and trigger contract.
- **Exit state and P&L:** computed by the deterministic lifecycle and trade
  simulator with predeclared variants, costs, and same-bar policy.
- **Alpha claim:** decided only from complete-population calibration,
  validation, untouched holdout, and robustness evidence.
- **Suman's role:** sponsor and governance authority—choose whether to continue,
  refine, stop, or later authorize a capital-facing packet after evidence is
  available.

Future review packets therefore ask only source-answerable questions about the
frozen Mala rectangle operationalization and expose source silence explicitly.
They must not call the deterministic geometry a Peter-defined rectangle or ask
Suman to predict whether an individual chart should make money.

## Round 3 — Source-Fidelity Overlay and Semantic Freeze

Round 3 is an additive V3 overlay on a verified, outcome-hidden V2 batch. It
does not alter V1 or V2 serialization. The authoritative rubric is
`research/playbooks/classical_rectangle_source_rubric_v1.md`; each generated
review directory contains a byte-identical `FROZEN_RUBRIC.md` copy and binds it
by SHA-256.

The V3 response asks only for:

- `mala_rectangle_state`;
- `lfd_assessment` and an optional `lfd_date`;
- frozen Mala-spec reason codes;
- source-ambiguity codes and an optional note.

There is no trade-worthiness, action, expected-return, or economic-selection
field. V3 generates its own sanitized card wrappers around the already-hashed
as-of SVGs. Verification rejects historical V2 review language in those cards,
rubric/card/chart tampering, stale identities, unknown enums, missing
attestations, or conflicting reviewer/pass records.

The aggregate `MalaRectangleSemanticSpecFreezeV1` gate requires two distinct
complete reviewers, predeclared agreement and indeterminate limits, and a
minimum match rate in each hidden calibration cohort. Its receipt contains no
accepted-card or signal allowlist and grants no authority to filter economics.
This is a freeze of **Mala rectangle v1 semantics**, not certification of Peter
Brandt's private method or results.

Runner commands:

```bash
uv run --frozen python -m src.research.classical_patterns.runner \
  init-source-fidelity-review-v3 \
  --batch-dir "$batch_dir" \
  --rubric research/playbooks/classical_rectangle_source_rubric_v1.md

uv run --frozen python -m src.research.classical_patterns.runner \
  verify-source-fidelity-review-v3 --batch-dir "$batch_dir"

uv run --frozen python -m src.research.classical_patterns.runner \
  ingest-source-fidelity-responses-v3 --batch-dir "$batch_dir" \
  --responses-csv "$responses_csv"

uv run --frozen python -m src.research.classical_patterns.runner \
  freeze-source-fidelity-v3 --batch-dir "$batch_dir" \
  --detector-git-commit "$detector_commit"
```

Semantic freeze and economic readiness remain separate gates. The retained
local cache may support a clearly labeled fixture/calibration plumbing run, but
the current `ClassicalPatternDataReadinessV1` explicitly blocks an alpha claim
until adjustment provenance, point-in-time universe membership, and delisting
coverage are independently proven.

### Current economic gate readback — 2026-07-17

The latest retained-cache audit covers 43 symbols from 2021-09-01 through
2022-12-30 and reports:

- `adjustment_provenance=unverified_provider_adjusted`;
- `economic_research_status=blocked_unverified_adjustment_and_point_in_time_universe`;
- no validation or untouched holdout beyond the configured calibration end;
- no date-effective security-master, membership, corporate-action, or
  delisting receipt.

A read-only daily-aggregate entitlement probe against the configured provider
also returned HTTP 403 `NOT_AUTHORIZED`; no secret was printed or persisted.
Therefore the autonomous path is to run only non-claiming engineering economics
from the retained cache after semantic freeze. Claim-grade D5 remains blocked
until a licensed, immutable bar manifest and point-in-time universe evidence
are available.

### Round 3 result

Two pristine Sol Ultra reviewers independently covered all nine sanitized cards
and exact as-of charts. They agreed on 8/9 `mala_rectangle_state` calls and 8/9
joint LFD assessment/date calls. The retained disagreement is CRM on 2022-03-29:
one reviewer saw a fresh long completion while the other rejected the geometry
as trend/reversal without sufficient repeated upper tests.

The aggregate gate passed without resolving that card by vote:

- confirmed-signal cohort match: `6/6` review rows;
- qualified-no-trigger cohort match: `6/6`;
- rejected-geometry cohort match: `5/6`;
- indeterminate fraction: `0`;
- economic filtering: `false`.

The resulting `MalaRectangleSemanticSpecFreezeV1` is bound to clean commit
`c3eccbe933c79bafe3adf1e3ad85451e43cba376`. It freezes the tested Mala
operationalization while retaining the CRM disagreement and source-silence
codes; it is not an accepted-chart allowlist.

### Economic round 1 — engineering/calibration only

The complete retained-cache population was replayed across all 43 audited
symbols from 2021-09-01 through 2022-12-30. The run reconciled 38,178 scanned
windows, 21 qualifying candidates, 20 representative signals, and both exact
stop-buffer variants for every signal (40 rows). All population identities and
artifact hashes passed. The run is `readiness=fixture_shadow`,
`executable=false`, and contains calibration rows only.

| Direction | LFD buffer | Result rows | Closed | Win rate | Average net R | Profit factor |
|---|---:|---:|---:|---:|---:|---:|
| Long | 0.00 ATR | 9 | 9 | 44.4% | -0.341 | 0.395 |
| Long | 0.10 ATR | 9 | 9 | 44.4% | -0.355 | 0.370 |
| Short | 0.00 ATR | 11 | 10 | 50.0% | -0.068 | 0.866 |
| Short | 0.10 ATR | 11 | 10 | 60.0% | +0.056 | 1.137 |

Across both directions, average net R was `-0.197` for the unbuffered variant
and `-0.139` for the 0.10 ATR variant. The narrow positive short/buffered cell
contains only ten closed calibration trades and is not alpha evidence. The
honest current read is: no broad positive edge appears in this small retained
calibration sample, and claim-grade validation remains blocked by data
provenance and point-in-time-universe evidence.

### Public frozen-cohort validation — complete

A fresh Public `FIVE_YEARS/ONE_DAY` snapshot supplied 100% expected-session
coverage for all 43 frozen symbols from 2021-07-19 through 2026-07-16. The
dataset retained raw provider payloads, normalized Parquet bars, a 22,894-row
current equity catalogue snapshot, file/content hashes, and three passing known
split-continuity checks. Its evidence grade remains
`frozen_cohort_validation_not_population_alpha` because current-symbol
selection is not point-in-time and Public's adjustment policy is undocumented.

The unchanged semantic freeze and detector produced 85 signals. Validation
shorts were positive (`+0.162 R` and `+0.118 R`) but reversed in holdout
(`-0.388 R` and `-0.394 R`). Validation longs were negative; holdout longs were
approximately flat. Combined holdout expectancy was negative for both variants
(`-0.147 R`, `-0.163 R`), and all validation/holdout directional 95% bootstrap
intervals crossed zero. No positive directional cell replicated.

The economic verdict is `no_replicated_alpha`. This closes rectangle v1 as a
negative or insufficient result for the frozen cohort; the consumed holdout
must not be used to retune it. The next economic experiment requires a new,
versioned hypothesis or broader point-in-time data used only for unchanged
replication. Nothing in this result is executable.
