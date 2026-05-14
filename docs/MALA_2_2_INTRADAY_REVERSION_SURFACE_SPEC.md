# Mala 2.2 Intraday Reversion Surface Spec

**Status:** build contract
**Owner:** Suman + Codex
**Vision source:** `docs/MALA_VISION_v2.2.md`
**First-slice source:** `docs/MALA_2_2_FIRST_SLICE.md`
**Playbook source:** `research/playbooks/mean_reversion_at_extremes_v0.md`

This is the first build target for Mala 2.2.

The goal is not to create a new autonomous strategy. The goal is to build an
evidence surface for a trader-supplied play:

```text
I want to fade/reverse an early-session extreme because I expect reversion
toward a reference level.
```

Mala owns the parameter search. Suman owns the play definition.

---

## Product Question

When Suman walks in with:

```text
I want to short IWM because it feels overextended.
```

Mala should answer:

```text
For IWM and this playbook, historical evidence was strongest in this parameter
region. Today's conditions match / partially match / do not match. Proceed,
proceed smaller, or skip.
```

The first implementation only needs to produce historical surfaces and sample
events. Current-day matching can be a follow-up once the historical surface is
credible.

---

## Command Surface

Proposed command:

```bash
python -m src.research.playbook_surface \
  mean-reversion-at-extremes-intraday \
  --symbols IWM,QQQ \
  --start 2021-05-13 \
  --end 2026-05-13 \
  --out-dir data/results/playbooks/mean_reversion_at_extremes/<run_ts>
```

Initial flags:

| Flag | Required | Meaning |
| --- | --- | --- |
| `playbook` | yes | Must be `mean-reversion-at-extremes-intraday` for v1. |
| `--symbols` | yes | Comma-separated symbols. Default review set is `IWM,QQQ`. |
| `--start` | yes | Start date for historical evaluation. |
| `--end` | yes | End date for historical evaluation. |
| `--out-dir` | no | Output directory. If omitted, create timestamped run dir. |
| `--max-events-per-bin` | no | Cap sample events in receipt/details. |

No Google Sheets, Bhiksha, or runtime mutation flags belong here.

---

## Inputs

Use existing Mala infrastructure:

- minute bars from `src.chronos.storage.LocalStorage`
- ET session helpers from `src.time_utils`
- feature transforms from `src.newton.engine` where reusable
- local output under `data/results/playbooks/`

The first version is underlying-only. No option overlay, no broker fills, no
provider replay.

---

## Search Surface

These values are not manually selected by Suman. They are candidate families
for the system to search.

### Entry Window

Candidate cutoff values:

- `10:00 ET`
- `10:10 ET`
- optionally `10:15 ET` if sample size is too thin

Interpretation: the event trigger must occur before the cutoff.

### Stage / Context

Candidate filters:

- no stage filter
- bullish/accumulation proxy using 8/21/34 VMA or VWMA stack
- broad market context filter using SPY/QQQ trend if available

The operator's chart read remains the high-prior concept. These are proxies to
test, not replacements for the trader's judgment.

### Extension Measures

Candidate feature families:

- ATR-normalized distance from recent reference
- z-score extension over short windows
- distance from VWAP
- distance from short moving average / VWMA
- prior-session or opening-range extension

Do not add RSI as Tier 1. RSI can only enter as Tier 2 if the literature pass
gives a specific reason and the receipt reports it as such.

### Reversal Trigger

Candidate trigger families:

- 5-minute reversal range breakout
- 15-minute reversal range breakout
- failure-to-extend followed by reclaim/breakout
- slope/velocity rollover, if it maps cleanly to the play

### Stop / Invalidation Candidates

Candidate stop and invalidation families:

- reversal-bar low/high
- reversal-bar midpoint
- loss of reversal range
- VWMA/VMA stack flip
- no favorable movement within a time window
- acceptance beyond the extreme

### Exit Candidates

Candidate exits:

- fixed R multiple from tested stop
- return to VWAP
- return to short moving average / VWMA reference
- partial retrace of the early-session extreme
- time stop
- end-of-day flat

The first result should report which exit family best matched the play. It
should not pretend the exit is solved.

---

## Output Contract

Each run writes:

```text
data/results/playbooks/mean_reversion_at_extremes/<run_ts>/
  RECEIPT.md
  conditional_surface_by_symbol.csv
  feature_bins_by_symbol.csv
  sample_events.csv
  config.json
```

### `config.json`

Must include:

- playbook id
- symbols
- date range
- feature families tested
- entry/trigger/stop/exit candidate values
- calibration/holdout split
- generated timestamp

### `conditional_surface_by_symbol.csv`

One row per symbol/direction/parameter region.

Minimum columns:

- `symbol`
- `direction`
- `entry_cutoff_et`
- `stage_filter`
- `extension_family`
- `extension_bin`
- `reversal_range_minutes`
- `stop_family`
- `exit_family`
- `sample_count`
- `calibration_count`
- `holdout_count`
- `calibration_expectancy_r`
- `holdout_expectancy_r`
- `calibration_win_rate`
- `holdout_win_rate`
- `match_grade`
- `evidence_note`

`match_grade` vocabulary:

- `favorable`
- `partial`
- `outside`
- `insufficient`

### `feature_bins_by_symbol.csv`

One row per symbol/feature/bin.

Minimum columns:

- `symbol`
- `direction`
- `feature`
- `bin_label`
- `bin_min`
- `bin_max`
- `sample_count`
- `expectancy_r`
- `win_rate`
- `holdout_expectancy_r`
- `holdout_win_rate`

### `sample_events.csv`

Concrete examples for later chart review.

Minimum columns:

- `symbol`
- `direction`
- `event_timestamp`
- `entry_reference_price`
- `extension_summary`
- `stage_summary`
- `trigger_summary`
- `stop_reference_price`
- `exit_reference_price`
- `exit_family`
- `outcome_label`
- `pnl_r`

Do not build thinkorswim or TradingView plotting in the first implementation.
This file is the bridge to visual review later.

### `RECEIPT.md`

Must answer:

- What was tested?
- Which symbols/dates were included?
- Which candidate feature families were searched?
- Which literature/operator notes informed the feature list?
- Where did evidence cluster?
- Where was evidence thin or contradictory?
- Did holdout agree with calibration?
- Which sample events should Suman inspect later?
- What did the run not test?
- What is the next decision: continue, refine, or kill?

---

## Research Notes To Carry In

Use these as starting points, not proof:

- Grant, Wolf, and Yu (2005): intraday index futures reversals after large
  opening price changes.
- Heston, Korajczyk, and Sadka (2010): intraday return patterns and short-term
  reversal from temporary liquidity imbalances.
- Gao, Han, Li, and Zhou (2017): intraday momentum from first half-hour returns.
  This is a caution that early-session moves can continue instead of reverse.
- `Six Key Technical Indicators Explained.docx`: relevant concepts are
  EMA/VWMA stack, volatility extremes, ADX/chop filtering, and VWAP/anchored
  VWAP context. RSI is not a Tier 1 operator feature.

The implementation should record the tested feature families explicitly so
later experiments do not silently expand the search space.

---

## Non-Goals

Do not build:

- Strategy_Catalog writes
- `active_strategy` writes
- Google Sheet publication
- Bhiksha runtime adapter
- option overlay
- global playbook registry
- current-day scanner
- visual plotting

---

## Validation

Minimum tests:

- event generation respects ET entry cutoffs
- 5-minute and 15-minute reversal range labels are deterministic
- stop/invalidation labels are deterministic
- output CSVs have stable schemas
- `RECEIPT.md` is written and names tested feature families
- empty/thin data produces `insufficient`, not a false favorable grade

Minimum smoke:

```bash
python -m src.research.playbook_surface \
  mean-reversion-at-extremes-intraday \
  --symbols IWM,QQQ \
  --start 2024-01-02 \
  --end 2024-03-29 \
  --out-dir /tmp/mala_2_2_intraday_reversion_smoke
```

The smoke is not proof. It only verifies that the artifact pipeline works.

---

## Review Gate

Bring Suman back when the first run can show:

- top favorable regions by symbol/direction
- clear unfavorable/outside regions
- sample events
- a short receipt that says whether this looks like a real playbook surface

Do not ask Suman to pick numeric thresholds that the system can search. Ask
only if the play semantics are ambiguous.
