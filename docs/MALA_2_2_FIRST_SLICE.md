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

## Playbook Family Split

Mean Reversion at Extremes is a family, not a single horizon.

For Mala 2.2, split it into two separately evaluated playbooks:

1. **Mean Reversion at Extremes - Intraday**
   - The trade is formed and managed inside the same session.
   - The setup is early-session only.
   - The first proof slice belongs here.

2. **Mean Reversion at Extremes - Multi-Day**
   - The trade expects reversion over the next couple of days or few days.
   - It may use daily/4h context, anchored VWAP, prior ranges, and event drift.
   - This is explicitly deferred until the intraday surface is understood.

Do not mix the two horizons in one backtest. The entry logic, outcome window,
failure modes, and useful indicators are different enough that combining them
would blur the evidence.

---

## First Trading Question

Proposed default:

```text
When Suman has an early-session intraday fade/reversal bias because IWM or QQQ
has reached an extreme, can Mala show whether similar historical moments were
favorable, partial, outside, or not enough evidence?
```

This starts with **Mean Reversion at Extremes - Intraday** because it is close
to the actual chart intuition that motivated Mala 2.2. It is also a useful
stress test: the play must be recognizable as an early-session reversal setup,
not just a statistically convenient label.

### Proposed Defaults For Review

These are defaults, not final decisions.

| Decision | Proposed default | Why |
| --- | --- | --- |
| Playbook | `mean_reversion_at_extremes` | Directly maps to the overextension/fade intuition. |
| Symbols | `IWM`, `QQQ` | IWM is the motivating example; QQQ gives a clean index/tech contrast. |
| Direction | both long reversals and short reversals | Avoid assuming overextension only means "short a rip." |
| Horizon | intraday, same session | Keeps the first proof bounded. |
| Data | underlying bars only | Options overlay is a later feasibility gate. |
| Runtime | none | First proof is a research/review surface, not Bhiksha integration. |
| Output | receipt + conditional surface CSVs + sample events | Plotting in thinkorswim/TradingView comes after the surface has plausible parameters. |

### Questions For Suman

Bring Suman in before implementation if any of these remain unresolved:

- Confirm whether "before 9/9:10 CST" means before 9:00-9:10 Central time,
  which is 10:00-10:10 ET during daylight saving time.
- Confirm whether the first version should require the entry trigger before
  10:00 ET or allow a wider 10:10 ET cutoff.
- Confirm whether the trend/context gate should be the existing VWMA/VMA
  stack 8/21/34, an operator-read override, or both.
- Confirm whether the first reversal range should be 5-minute, 15-minute, or
  a parameter surface across both.
- Confirm whether the initial stop candidate should be the reversal-bar low,
  the reversal-bar midpoint, or both as tested variants.

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
- VWMA/VMA stack features where they match the 8/21/34 context read
- VWAP/VPOC helpers only if they are part of the declared feature list
- daily/market regime tags as context, not as rescue explanations
- structural helpers added in this branch only as diagnostics, not as proof

Build only if needed:

- distance-from-VWAP features
- ATR-normalized extension
- prior-session/range extension
- 5-minute and 15-minute reversal-range labels
- reversal-range breakout labels
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
  - breakout followed through
  - reverted to target
  - failed to revert after trigger
  - accepted beyond the reversal extreme
  - timed out
- invalidation-family metrics:
  - reversal-bar low or midpoint breached
  - reversal range lost after breakout
  - VWMA/VMA stack no longer supports the trade
  - price accepts away from the intended reversion path
  - time stop before enough favorable movement

Profit-taking should be treated as a surface to evaluate, not assumed. Initial
candidate exits:

- fixed R multiple from the tested stop
- return to VWAP
- return to a short moving average / VWMA reference
- partial retrace of the opening extreme
- time stop if the move does not validate quickly

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
  mean-reversion-at-extremes-intraday \
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
  sample_events.csv
  config.json
```

### Receipt Requirements

`RECEIPT.md` must answer:

- What was tested?
- Which symbols and dates were included?
- Which features were predeclared?
- Which papers/notes informed the candidate feature list?
- How many events were found?
- What regions looked favorable, partial, outside, or insufficient?
- Did holdout agree with calibration?
- Which examples should Suman inspect later if the surface is promising?
- What did the run not test?
- What is the next operator decision?

### Paper And Indicator Research Requirements

Before implementation, do a short literature pass focused on intraday reversal,
opening overreaction, and technical indicators that map to this specific setup.

The research pass should not import every common indicator. Suman's current
operator language does **not** use RSI as a primary decision input, so RSI can
only enter as a Tier 2 candidate if there is a clear reason to test it.

Initial paper/note leads:

- Grant, Wolf, and Yu (2005), [intraday index futures reversals after large
  opening price changes](https://doi.org/10.1016/j.jbankfin.2004.04.006).
- Heston, Korajczyk, and Sadka (2010), [intraday return patterns and short-term
  reversal from temporary liquidity imbalances](https://arxiv.org/abs/1005.3535).
- Gao, Han, Li, and Zhou (2017), [intraday momentum from first half-hour
  returns](https://ssrn.com/abstract=2440866). This is a warning:
  early-session direction can also continue, so the surface must distinguish
  reversal from continuation.
- The operator note `Six Key Technical Indicators Explained.docx`, especially
  EMA/VWMA stack, Bollinger-style volatility extremes, ADX/chop filtering, and
  VWAP/anchored VWAP as context.

The research pass should produce a small candidate feature list, with each
feature tagged as:

- `operator_tier_1`
- `paper_tier_2`
- `context_only`
- `defer`

### Later Chart Marks Requirements

Do not start with thinkorswim or TradingView plotting. After the parameter
surface has plausible candidates, produce `event_marks_for_chart.csv` for
visual review.

Minimum eventual columns:

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
- thinkorswim/TradingView plotting before the parameter surface is worth
  inspecting

These are not rejected forever. They are deferred until the first evidence
surface proves that Suman can recognize and use the playbook output.

---

## Review Gate

The first slice is ready for Suman review when there is one completed local run
with:

- a receipt
- conditional surface CSVs
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
