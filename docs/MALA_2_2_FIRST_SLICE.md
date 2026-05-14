# Mala 2.2 First Slice

**Status:** working design contract
**Owner:** Suman + Codex
**Vision source:** `docs/MALA_VISION_v2.2.md`
**First playbook surface:** `research/playbooks/mean_reversion_at_extremes_v0.md`

This document defines the first proof slice for Mala 2.2.

Mala 2.2 is not a cleaner name for the current Mala surface. It is a different
operating model:

```text
Current Mala:
  strategy discovery -> M1-M5 promotion -> evidence handoff -> shadow/live review

Mala 2.2:
  trader thesis -> historical/context evidence -> visual review -> rule packet draft
  -> receipt/load artifact -> shadow-only verification
```

The first slice should prove that this new loop is useful before any broad
architecture is built.

---

## Core Boundary

Reuse Mala v2 as research infrastructure. Do not inherit Mala v2 as the
operating model.

That means:

- use the cache, feature transforms, split discipline, reports, and simulator
- do not treat M1-M5 promotion as the product
- do not use Strategy_Catalog semantics as the control plane
- do not let a passing historical result imply Bhiksha readiness
- do not build a broad playbook registry before one playbook earns it

The first slice is successful only if it produces a concrete operator-facing
answer to one trading question.

---

## First Trading Question

Proposed default:

```text
When Suman has an intraday fade bias because IWM or QQQ looks overextended,
can Mala show whether similar historical moments were favorable, partial,
outside, or not enough evidence?
```

This starts with **Mean Reversion at Extremes** because it is close to the
actual chart intuition that motivated Mala 2.2. It is also a useful stress test:
the play must be recognizable visually, not just statistically.

### Proposed Defaults For Review

These are defaults, not final decisions.

| Decision | Proposed default | Why |
| --- | --- | --- |
| Playbook | `mean_reversion_at_extremes` | Directly maps to the overextension/fade intuition. |
| Symbols | `IWM`, `QQQ` | IWM is the motivating example; QQQ gives a clean index/tech contrast. |
| Direction | both long fades and short fades | Avoid assuming overextension only means "short a rip." |
| Horizon | intraday, same session | Keeps the first proof visual and bounded. |
| Data | underlying bars only | Options overlay is a later feasibility gate. |
| Runtime | none | First proof is a research/review surface, not Bhiksha integration. |
| Output | receipt + chart marks + conditional surface CSVs | Lets Suman inspect both the numbers and whether events look like the play. |

### Questions For Suman

Bring Suman in before implementation if any of these remain unresolved:

- Is the first horizon intraday snapback, multi-hour pullback, or next-day reversion?
- For the first pass, should "extreme" mean distance from VWAP, ATR-normalized move, z-score, prior range extension, or a small combination?
- Is the target "back to VWAP," "partial retrace," "back to moving average," or simply "not more continuation"?
- Which visual confirmation matters most: wick/reclaim, failure to extend, volume fading/climax, or slope/velocity rollover?
- Should QQQ be the second symbol, or should SPY be used as the cleaner index baseline?

---

## What To Reuse

### Data And Storage

Reuse:

- `src.chronos.storage.LocalStorage`
- cached underlying minute bars under `data/<TICKER>/<YYYY-MM-DD>.parquet`
- ET time helpers in `src.time_utils`
- the existing local/oldmac data boundary

Do not build:

- a new market-data cache
- a new provider abstraction
- option-chain ingestion for the first pass

### Feature Infrastructure

Reuse where it matches the play:

- Newton price/volume transforms from `src.newton.engine`
- velocity, acceleration, jerk
- VWAP/VPOC helpers only if they are part of the declared feature list
- daily/market regime tags as context, not as rescue explanations
- structural helpers added in this branch only as diagnostics, not as proof

Build only if needed:

- distance-from-VWAP features
- ATR-normalized extension
- prior-session/range extension
- event labels for chart inspection

### Evaluation Discipline

Reuse the discipline, not the exact promotion gates:

- calibration vs. holdout split
- sample-size reporting
- effect-size reporting
- explicit tested-feature accounting
- cost/stress thinking where relevant
- no feature added after seeing results unless the report marks it as a new experiment

Do not reuse:

- broad search over many strategies and symbols
- M1-M5 as automatic promotion law
- `CATALOG_SELECTED.csv` as the product surface
- recommendation tiers as execution authorization

### Exit And Outcome Simulation

Reuse:

- `src.oracle.trade_simulator`
- underlying-anchored exit policy interfaces
- fixed percent reward/risk and time-stop policies
- the thesis-exit artifact pattern from `src.research.exit_optimizer`

Build:

- playbook-specific outcome labels:
  - reverted to target
  - failed to revert
  - accepted beyond extreme
  - timed out
- simple invalidation-family metrics for the first pass

### Provider And Execution Readiness

Reuse later:

- M6 provider validation ideas
- Bhiksha capability manifest pattern
- handoff packet provenance pattern

Do not use in the first slice:

- live authorization
- active_strategy mutation
- Google Sheet publication
- Bhiksha runtime loading

---

## What To Build First

Build a small playbook evidence generator, not a strategy.

Proposed command shape:

```bash
python -m src.research.playbook_surface \
  mean-reversion-at-extremes \
  --symbols IWM,QQQ \
  --start 2021-05-13 \
  --end 2026-05-13 \
  --out-dir data/results/playbooks/mean_reversion_at_extremes/<run_ts>
```

Expected first output:

```text
data/results/playbooks/mean_reversion_at_extremes/<run_ts>/
  RECEIPT.md
  conditional_surface_by_symbol.csv
  feature_bins_by_symbol.csv
  event_marks_for_chart.csv
  sample_events.csv
  config.json
```

### Receipt Requirements

`RECEIPT.md` must answer:

- What was tested?
- Which symbols and dates were included?
- Which features were predeclared?
- How many events were found?
- What regions looked favorable, partial, outside, or insufficient?
- Did holdout agree with calibration?
- What examples should Suman inspect visually?
- What did the run not test?
- What is the next operator decision?

### Chart Marks Requirements

`event_marks_for_chart.csv` should make it easy to inspect events in
thinkorswim or another charting surface.

Minimum columns:

- `symbol`
- `event_timestamp`
- `direction`
- `event_type`
- `entry_reference_price`
- `target_reference_price`
- `invalidation_reference_price`
- `feature_summary`
- `outcome_label`

---

## What Not To Build Yet

Do not build:

- a global playbook registry
- a rule-packet compiler
- Bhiksha adapter changes
- option overlay simulator
- Google Sheet control tower changes
- automatic current-market scanner
- agent-proposed trade intake
- live or shadow authorization

These are not rejected forever. They are deferred until the first evidence
surface proves that Suman can recognize and use the playbook output.

---

## Review Gate

The first slice is ready for Suman review when there is one completed local run
with:

- a receipt
- conditional surface CSVs
- chart marks
- 10-20 sample events worth inspecting
- a clear recommendation:
  - continue
  - retune the play definition
  - change symbols/horizon
  - kill the slice

The first slice is not ready if the output only says "positive expectancy" or
"M1 passed." That would mean we rebuilt the old surface.

---

## Success Criteria

The first slice succeeds if Suman can answer:

```text
Do these historical events look like the trade I actually mean?
Do the favorable/partial/outside regions match my intuition enough to continue?
Did the evidence change how I would size, skip, or rewrite this play?
```

If yes, Mala 2.2 has earned the next build step.

If no, stop and revise the play definition before adding infrastructure.
