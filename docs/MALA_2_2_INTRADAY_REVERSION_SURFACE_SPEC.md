# Mala 2.2 Intraday Reversion Surface Spec

**Status:** build contract
**Owner:** Suman + Codex
**Vision source:** `docs/MALA_VISION_v2.2.md`
**First-slice source:** `docs/MALA_2_2_FIRST_SLICE.md`
**Family source:** `research/playbooks/mean_reversion_at_extremes_v0.md`
**Playbook source:** `research/playbooks/mean_reversion_at_extremes_intraday_v1.md`
**Implementation state:** Phase 1 spine implemented.

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

## Reusable Infrastructure

Reuse existing Mala/Newton surfaces where they fit the play:

- `src.chronos.storage.LocalStorage` for historical minute bars
- `src.time_utils` for market-session handling
- `src.newton.transforms` for velocity, acceleration, and jerk-style features
- Newton relative-volume transforms for trigger-volume confirmation
- `src.newton.market_impulse` / `src.strategy.market_impulse` for 8/21/34
  VWMA stack and `impulse_regime_5m`
- `src.strategy.elastic_band_reversion` as a reference for z-score, VPOC, and
  kinematic reversion feature wiring
- VPOC/auction-proxy helpers where the feature is available and reliable
- `src.oracle.metrics` for MFE/MAE-style excursion evidence
- `src.oracle.trade_simulator` and exit-policy classes where bar-by-bar
  simulation is needed
- existing `data/results/` style local artifact output

New build required:

- broader parameter search runs and review
- calibration of thresholds after the first real surface review
- current-day matcher after the historical surface is credible
- visual plotting after sample events identify candidate regions

Implemented:

- playbook-surface CLI and run config
- registered playbook strategy for the v1 grid
- Newton feature additions for opening VWAP, prior-close ATR distance, and
  `gap_state`
- reversal-range event construction
- stop, invalidation, and exit evaluation in R units
- calibration/holdout reporting
- receipt and CSV writers

---

## Architecture Decision

`src.research.playbook_surface` should be a thin orchestration and artifact
writer, not a generic dumping ground for every future playbook.

Scaling model:

- Newton owns reusable features.
- Strategy owns playbook-specific event logic and `search_spec` / parameter
  surface declaration.
- Oracle mostly remains stable and owns excursion, reward-risk, trade
  simulation, and exit-policy math.
- Research owns the CLI, run loop, calibration/holdout slicing, and output
  contract.

This lets new playbooks scale by adding strategy/search surfaces and only adding
Newton features when a reusable market concept is missing.

---

## Search Surface

These values are not manually selected by Suman. They are candidate families
for the system to search.

### Time Window

The operator describes the opening-drive window in Central time. The repo should
evaluate and report the equivalent Eastern-time market windows.

| Operator window | Repo window |
| --- | --- |
| `08:30-08:45 CT` | `09:30-09:45 ET` |
| `08:30-09:00 CT` | `09:30-10:00 ET` |
| `08:30-09:15 CT` | `09:30-10:15 ET` |
| `08:30-10:00 CT` | `09:30-11:00 ET` |

The event trigger must occur inside the candidate window.

### Stretch / Extreme

Candidate feature families:

- `z_score_from_opening_vwap`
- `atr_distance_from_prior_close`
- `z_score_from_vpoc_4h`
- `atr_distance_from_reference`
- `velocity_1m`
- `velocity_5m` / `velocity_5`
- `velocity_15m` / `velocity_15`

Initial z-score thresholds:

- `1.5`
- `2.0`
- `2.5`
- `3.0`
- `3.5`

Initial ATR thresholds for prior-close distance:

- `0.75`
- `1.0`
- `1.25`
- `1.5`
- `2.0`
- tail bins: `2.5`, `3.0`

Velocity should be evaluated as a conditioning dimension, not a single hard
filter:

- no velocity filter
- moderate/non-climactic velocity
- violent/climactic velocity

This lets Mala test whether the play works best after a fast rubber-band move or
after slower unsupported extension.

### Stage / Context

Candidate filters:

- no stage filter
- `impulse_regime_5m = bullish`
- `impulse_regime_5m = bearish`
- `impulse_regime_5m = neutral`
- 8/21/34 VMA/VWMA stack bullish
- 8/21/34 VMA/VWMA stack bearish
- 8/21/34 VMA/VWMA stack mixed
- `gap_state = gap_up_large`
- `gap_state = gap_up_small`
- `gap_state = flat`
- `gap_state = gap_down_small`
- `gap_state = gap_down_large`
- broad market context using SPY/QQQ trend if available

Key question: does the fade work better as a rubber-band snap in a still-bullish
stage, or as continuation after the 5-minute context has already rolled over?

Do not add RSI as Tier 1. RSI can only enter as Tier 2 if the literature pass
gives a specific reason and the receipt reports it as such.

### Reversal Trigger

Candidate trigger families:

- 5-minute reversal range breakout
- 15-minute reversal range breakout
- 1 confirming bar
- 2 confirming bars
- `jerk_1m` / `jerk_5m` exhaustion direction
- reversal-bar relative volume: no filter, `> 1.0`, `> 1.25`, `> 1.5`
- failure-to-extend followed by reclaim/breakout

### Stop / Invalidation Candidates

Candidate stop and invalidation families:

- reversal-bar low/high
- reversal-bar midpoint
- loss of reversal range
- VWMA/VMA stack flip
- no favorable movement within `10`, `20`, or `30` minutes
- acceptance beyond the extreme
- `immediate_entry_bar_failure` for the head-fake case

### Exit Candidates

Candidate exits:

- fixed `0.5R`, `1.0R`, `1.5R`, `2.0R`
- return to opening/session VWAP
- return to VPOC/reference if available
- return to short moving average / VWMA reference
- `25%`, `50%`, `75%` retrace of the early-session extreme
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
- `gap_state_filter`
- `extension_family`
- `extension_bin`
- `reversal_range_minutes`
- `volume_confirmation_filter`
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
- `gap_state`
- `trigger_summary`
- `volume_confirmation_summary`
- `stop_reference_price`
- `exit_reference_price`
- `exit_family`
- `outcome_label`
- `pnl_r`

Oracle already owns MFE/MAE-style excursion evidence. The playbook runner should
reuse or reference Oracle-derived excursion artifacts rather than redefining
that metric family in the playbook contract. If MFE/MAE columns are included in
`sample_events.csv`, they must be derived from Oracle calculations.

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
