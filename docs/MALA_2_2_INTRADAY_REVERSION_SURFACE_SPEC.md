# Mala 2.2 Intraday Reversion Surface Spec

**Status:** implemented / historical build contract as of 2026-05-17. This
documents the original surface build; current promotion and automation status
lives in `MALA_BHIKSHA_REFACTOR_ARCHITECTURE.md` and
`PLAYBOOK_AUTOMATION_GATES.md`.

**Owner:** Suman + Codex
**Vision source:** `docs/MALA_VISION_v2.2.md`
**First-slice source:** `docs/MALA_2_2_FIRST_SLICE.md`
**Family source:** `research/playbooks/mean_reversion_at_extremes_v0.md`
**Playbook source:** `research/playbooks/mean_reversion_at_extremes_intraday_v1.md`
**Implementation state:** surface spine, consultation path, packet path, parity,
and `P1-P7` promotion gate evaluator implemented. Current blocker for learning
is not locked validation; the next step is Bhiksha shadow feedback and Mala
feedback ingestion.

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
  --out-dir research/results/playbooks/mean_reversion_at_extremes/<run_ts>
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
- local output under `research/results/playbooks/`

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
  VWMA stack and trader-facing 1m `market_pulse_stage`
- `src.strategy.elastic_band_reversion` as a reference for z-score, VPOC, and
  kinematic reversion feature wiring
- VPOC/auction-proxy helpers where the feature is available and reliable
- `src.oracle.metrics` for MFE/MAE-style excursion evidence
- `src.oracle.trade_simulator` and exit-policy classes where bar-by-bar
  simulation is needed
- existing `research/results/` style local artifact output

New build required:

- broader parameter search runs and review
- calibration of thresholds after the first real surface review
- current-day matcher after the historical surface is credible
- visual plotting after sample events identify candidate regions

Implemented:

- playbook-surface CLI and run config
- registered playbook strategy for the v1 grid
- Newton feature additions for RTH opening VWAP, prior-RTH-close ATR distance,
  `gap_state_rth_open`, and RTH relative volume
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
- Query owns the operator-facing timestamp question through
  `src.research.playbook_surface_query`: a generic shell plus one registered
  adapter for this first playbook. Future playbooks should add adapters instead
  of forking the query command.
- Query supports two modes:
  - `state-management`: the preferred analyst-desk mode. It ignores rule firing
    as the primary product question, retrieves nearest historical analogs for
    the current state and requested bias, then reports empirical forward
    outcomes and management rows.
  - `signal`: a sparse rule-firing debug mode for checking whether the timestamp
    matches an active playbook entry.

This lets new playbooks scale by adding strategy/search surfaces and only adding
Newton features when a reusable market concept is missing.

---

## Relationship To M1-M5

Mala 2.2 should reuse the old research machinery without inheriting the old
promotion model.

The old M1-M5 path was designed for autonomous strategy promotion. It tried to
break a fixed strategy before allowing it to flow toward canonical
`Mala_Evidence_v1`, operator authorization, and eventually Bhiksha runtime
support.

This playbook surface is different. It maps where a trader-supplied play is
historically favorable, partial, outside, or insufficient. It should not publish
to `Mala_Evidence_v1`, `active_strategy`, or Bhiksha. Legacy
`Strategy_Catalog` paths are compatibility and migration history, not the
current playbook handoff target.

### Discovery Surface

`playbook_surface.py` absorbs the M1/M4 idea for the exploratory phase:

- calibration expectancy and win rate show where directional edge appears
- holdout expectancy and win rate sit next to calibration for each parameter
  region
- thin or contradictory regions are graded `insufficient` or `outside`
- the output is a map, not a single promoted candidate

The current repo's M3 concept is walk-forward/OOS stability rather than a pure
regime gate. In Mala 2.2, stage/regime features such as 1m `market_pulse_stage` and
`gap_state_rth_open` are primarily search dimensions. If the play only works in accumulation
contexts, that should become a mapped constraint, not a reason to kill the whole
playbook.

### Human Review

After the surface run, Suman reviews:

- `RECEIPT.md`
- `conditional_surface_by_symbol.csv`
- `feature_bins_by_symbol.csv`
- `sample_events.csv`

The chart review answers whether the math captured the intended feel. A region
with attractive numbers but bad chart semantics should not become a locked
packet.

### Locked Packet Stress

Only after chart review should a region become a locked execution packet. That
packet can reuse the old M2/M5 mechanics in a targeted way:

- M2-style cost/spread/slippage stress asks whether the edge survives realistic
  friction
- M5-style execution stress and Monte Carlo ask whether the locked packet is
  robust enough for controlled execution

These stress checks are evidence for a locked playbook packet. They are not
authorization to publish an autonomous strategy.

### Human-In-The-Loop Execution

The later operator flow is:

```text
Suman has a bias -> current-day matcher checks locked constraints ->
PROCEED / PARTIAL / SKIP -> Suman authorizes -> Bhiksha manages the
predeclared stop/target for that authorized trade.
```

Live matching, Bhiksha integration, option overlay, and plotting are later
steps. The first proof remains the historical surface plus sample-event chart
review.

### Analyst Desk Pivot

The consultation surface must not be a thin wrapper over entry-rule firing. A
timestamp query should answer:

```text
I am looking at this state with this bias. What did the closest historical
analogs do next, and what management choices would have helped?
```

The preferred query mode is therefore `state-management`, not `signal`.

It returns:

- a trader-readable current-state summary
- nearest historical analog count and similarity
- the similarity recipe, including feature scales, weights, and stage/gap
  mismatch penalties
- forward MFE/MAE over `5`, `10`, `15`, `30`, `60`, and session-close
  horizons
- reversion, continuation, and chop mix
- empirical management rows for quick scalp thresholds, VWAP retraces, and VWAP
  return
- omission of management rows whose target is below the tradable floor
  `max(0.10 * daily_rth_atr_14, 0.10% * price)`, so tiny VWAP retraces do not get
  promoted as edge
- honest desk reads such as `strong_reversion_lean`, `reversion_lean`,
  `mixed_cohort`, `continuation_lean`, `strong_continuation_risk`, or
  `too_thin`
- an append-only `consultation_log.csv` row so replay/live questions become a
  forward-shadow journal

The consultation log should record what the desk reported, then leave
`selected_exit`, `taken`, and actual outcome columns empty for the trader or
post-close updater. The query path should not auto-pick the exit row.

The generic journal contract and close/update CLI are defined in
`docs/MALA_2_2_CONSULTATION_JOURNAL.md`.

`wait_no_trigger` is not the primary live desk product. It remains useful for
debugging the sparse event constructor, but the trader-facing consultation
should browse historical analogs.

### Options Layer Boundary

The query packet is currently underlying-first. If Suman asks "what if I still
enter?", Mala can return an underlying entry, stop, target, invalidation, and
evidence context. It must not pretend that this directly solves options
execution.

The later options layer should sit on top of the locked or queried underlying
thesis and translate:

- underlying entry/stop/target into option-contract selection
- delta, expiry, spread/liquidity, and theta exposure into expected option PnL
- underlying thesis invalidation into option stop/adjustment rules
- position sizing from underlying R into contract-level risk

Until that layer exists, options guidance should be framed as "underlying thesis
management only."

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

- `z_score_from_opening_vwap_rth`
- `atr_distance_from_prior_rth_close`
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
- `market_pulse_stage = bullish`
- `market_pulse_stage = accumulation`
- `market_pulse_stage = distribution`
- `market_pulse_stage = bearish`
- `gap_state_rth_open = gap_up_large`
- `gap_state_rth_open = gap_up_small`
- `gap_state_rth_open = flat`
- `gap_state_rth_open = gap_down_small`
- `gap_state_rth_open = gap_down_large`
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
- MarketPulse flip into continuation
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
- `market_pulse_flip`: long exits when 1m `market_pulse_stage` flips to
  `bearish`; short exits when it flips to `bullish`
- time stop
- end-of-day flat

The first result should report which exit family best matched the play. It
should not pretend the exit is solved.

---

## Output Contract

Each run writes:

```text
research/results/playbooks/mean_reversion_at_extremes/<run_ts>/
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
- `criteria_failed_count`
- `criteria_failed`
- `evidence_note`

`match_grade` vocabulary:

- `favorable`
- `near_favorable`
- `partial`
- `outside`
- `insufficient`

`near_favorable` means exactly one strict criterion missed while calibration or
holdout expectancy remained positive. It is a chart-review lead, not proof.
`criteria_failed` must name the failed bounds so a trader can tell a one-bound
miss from a way-off partial result.

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
- `event_timestamp` in UTC
- `event_timestamp_et` for chart lookup
- `entry_reference_price`
- `extension_summary`
- `stage_summary`
- `gap_state` derived from `gap_state_rth_open`
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

Operator-query smoke after a surface exists:

```bash
python -m src.research.playbook_surface_query \
  --run-dir research/results/playbooks/mean_reversion_at_extremes/<run_ts> \
  --symbol QQQ \
  --direction short \
  --timestamp "2026-05-11 09:45 America/New_York"
```

This should write a timestamp-specific `QUERY_REVIEW.md` and
`query_result.json`. The verdict is not live authorization; it is the surface's
answer to the trader's stated bias at one historical or current bar.

---

## Review Gate

Bring Suman back when the first run can show:

- candidate regions by symbol/direction, ranked by review taxonomy rather than
  raw holdout expectancy
- clear unfavorable/outside regions
- sample events
- a short receipt that says whether this looks like a real playbook surface

The receipt must not publish a simple "top holdout expectancy" leaderboard. That
rebuilds the old M1-style artifact and over-promotes tail-payoff or holdout-only
pockets. Use `surface_review/SURFACE_REVIEW.md` for the trader-facing taxonomy.

Do not ask Suman to pick numeric thresholds that the system can search. Ask
only if the play semantics are ambiguous.
