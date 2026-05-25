# Intraday Mean Reversion at Extremes - Playbook Search Spec v1

**Status:** Phase 1 spine implemented
**Owner:** Suman + Codex
**Family spec:** `research/playbooks/mean_reversion_at_extremes_v0.md`
**Build spec:** `docs/MALA_2_2_INTRADAY_REVERSION_SURFACE_SPEC.md`
**Implementation state:** Newton features, strategy search surface, and thin
playbook runner are implemented.

---

## Core Thesis

This playbook is not:

```text
Find me a profitable strategy.
```

It is:

```text
Map the historical terrain for this specific trader play.
```

The trader brings a bias:

```text
IWM/QQQ is overextended early in the session. I want to fade the extreme and
capture reversion toward a reference level.
```

Mala answers:

```text
For this symbol, direction, and playbook, the historical evidence was strongest
in these parameter regions. Today's state is favorable / partial / outside /
insufficient.
```

---

## Scope

- **Playbook id:** `mean-reversion-at-extremes-intraday`
- **Symbols:** `IWM`, `QQQ`
- **Directions:** upside fade / short reversion and downside fade / long
  reversion
- **Primary horizon:** same-session move after the opening drive
- **Data:** underlying minute bars only
- **Options overlay:** deferred
- **Bhiksha/runtime:** deferred
- **Visual review:** TradingView/thinkorswim after the parameter surface
  produces candidates worth inspecting. In-repo static chart renderers should
  not become a parallel charting surface.

---

## Ownership Boundary

Suman owns:

- the play category
- the symbol and direction bias
- the chart-language intuition: early stretch, accumulation/bullish or stage
  context, exhaustion, reversal range, reversion toward a reference
- the final discretionary decision after Mala reports the surface

Mala owns:

- the parameter grid
- threshold discovery
- historical event construction
- stop, invalidation, and exit surface evaluation
- calibration/holdout reporting
- concrete sample events for later chart review

Suman should not have to choose one cutoff, one reversal window, or one stop
location before research. Those are search dimensions.

---

## Time Convention

The operator language uses Central time. Because U.S. equity market data is
normally indexed in Eastern time inside the repo, every result should report ET
while preserving the Central-time meaning in the receipt.

Initial time windows:

| Operator window | Repo window | Meaning |
| --- | --- | --- |
| `08:30-08:45 CT` | `09:30-09:45 ET` | first 15 minutes after cash open |
| `08:30-09:00 CT` | `09:30-10:00 ET` | first 30 minutes |
| `08:30-09:15 CT` | `09:30-10:15 ET` | opening-drive plus first reversal window |
| `08:30-10:00 CT` | `09:30-11:00 ET` | broad morning window for comparison |

The user's phrase "before 9:00/9:10 CST" should be treated as the Central
operator cutoff concept, not as a hard literal timezone rule. In May, Chicago is
on daylight time; docs should use `CT` where possible.

---

## Search Grid

### 1. Stretch: Defining the Extreme

Purpose: quantify "this looks overextended" without replacing the trader's
chart read.

Candidate feature families:

| Feature family | Initial candidates | Notes |
| --- | --- | --- |
| `z_score_from_opening_vwap_rth` | thresholds `1.5, 2.0, 2.5, 3.0, 3.5` | preferred opening-drive reference; starts at the regular-session open and ignores premarket |
| `atr_distance_from_prior_rth_close` | thresholds `0.75, 1.0, 1.25, 1.5, 2.0`; tail bins `2.5, 3.0` | captures RTH-open gap plus morning displacement without premarket VWAP contamination |
| `z_score_from_vpoc_4h` | thresholds `1.5, 2.0, 2.5, 3.0, 3.5` | uses existing auction/VPOC direction where reliable |
| `atr_distance_from_reference` | binned by symbol and direction | keeps surface comparable across IWM/QQQ |
| `velocity_1m` | percentile or ATR-normalized bins | distinguishes grind from impulse |
| `velocity_5m` / `velocity_5` | percentile or ATR-normalized bins | smoother version of the same question |
| `velocity_15m` / `velocity_15` | percentile or ATR-normalized bins | broader opening-drive pressure |

Search interaction:

- stretch with violent/climactic velocity
- stretch with moderate/non-climactic velocity
- stretch without a velocity filter

The key question is not only "was it stretched?" It is whether the edge lives in
a rubber-band snap after a fast move, or in a slower extension that becomes
unsupported.

`z_score_from_session_vwap` can be a later companion feature, but v1 should not
need both opening VWAP and session VWAP unless implementation is nearly free.

### 2. Context: Defining the Stage

Purpose: test whether the fade works because the market is in an exhaustion
state, a trend-continuation state, or a mixed/transition state.

Candidate filters:

| Feature family | Initial candidates | Notes |
| --- | --- | --- |
| `market_pulse_stage` | `bullish`, `accumulation`, `distribution`, `bearish`, `no_filter` | Suman's 1m MarketPulse stage vocabulary from VWMA 8/21/34 plus VMA location |
| `gap_state_rth_open` | large gap up, small gap up, flat, small gap down, large gap down, no filter | separates true intraday stretch from RTH gap-plus-continuation days |
| broad-market context | aligned, opposed, neutral, no filter | optional if SPY/QQQ context is available cleanly |

The important search question:

```text
Does shorting an overextended QQQ work best when the 5m stack is already
bearish, or when it is still deeply bullish and vulnerable to a rubber-band
snap?
```

Both cases should be tested. The initial spec should not assume that
"bullish/accumulation" is always the best historical filter.

### 3. Trigger: Defining Exhaustion and Confirmation

Purpose: avoid stepping in front of a trend that has not yet shown failure.

Candidate feature families:

| Feature family | Initial candidates | Notes |
| --- | --- | --- |
| `reversal_range_minutes` | `5`, `15` | compare the operator's two visual ranges |
| `reversal_bar_breakout` | required after reversal range | entry confirmation candidate |
| `confirming_bars` | `1`, `2` | tests whether waiting helps or gives up edge |
| `jerk_1m` / `jerk_5m` | sign/direction bins | exhaustion/deceleration proxy |
| `reversal_bar_relative_volume_rth` | no filter, `> 1.0`, `> 1.25`, `> 1.5` | tests whether RTH-volume-backed reversal bars improve holdout expectancy; baseline is rolling RTH-only bars, not a session-reset opening-volume baseline |
| `failure_to_extend_reclaim` | yes/no | candidate if easy to define from bars |

Directional interpretation:

- Upside fade / short reversion: positive stretch, positive velocity, then
  negative jerk or failure to continue higher.
- Downside fade / long reversion: negative stretch, negative velocity, then
  positive jerk or failure to continue lower.

The first implementation can derive `jerk_5m` by resampling or aggregating the
existing kinematic features if native 5-minute jerk is not already present.

### 4. Invalidation and Stop Surface

Purpose: find where the thesis is historically broken, not just where the
initial stop would have sat.

Candidate stop families:

| Family | Initial candidates |
| --- | --- |
| reversal-bar extreme | low for long reversion, high for short reversion |
| reversal-bar midpoint | midpoint of reversal bar/range |
| reversal range loss | loss of confirmed reversal range |
| acceptance beyond extreme | 1 or 2 bars accepting beyond the prior extreme |
| immediate entry-bar failure | direction-specific close back through the entry bar after breakout |

Candidate invalidation families:

- no favorable movement after `10`, `20`, or `30` minutes
- new extreme after confirmation
- MarketPulse flips into continuation
- price accepts beyond the stretch reference instead of rejecting it
- max adverse excursion exceeds the tested stop family

The output should say which invalidation family actually protected expectancy.
It should not hard-code the user's current crude stop idea as final truth.

`immediate_entry_bar_failure` is the tight "head fake" leash. For long
reversion, it means the breakout/reclaim bar immediately closes back below the
entry bar reference; for short reversion, it is the symmetric close back above
the entry bar reference. The implementation should make the exact reference
explicit.

### 5. Exit Surface

Purpose: discover what validates the reversion and what should be monetized.

Candidate exits:

| Exit family | Initial candidates |
| --- | --- |
| fixed R | `0.5R`, `1.0R`, `1.5R`, `2.0R` |
| return to VWAP/reference | opening VWAP, session VWAP, VPOC/reference if available |
| partial retrace | `25%`, `50%`, `75%` retrace of the early extreme |
| short MA/VWMA return | return to 8/21/34 reference family |
| MarketPulse flip | long exits when 1m `market_pulse_stage` flips to `bearish`; short exits when it flips to `bullish` |
| time stop | morning cutoff, midday cutoff, end-of-day flat |

The exit surface is the most uncertain part of this playbook. Phase 1 should
report exit-family stability and fragility instead of forcing a single answer.

---

## Decision Surface Output

The eventual operator-facing answer should be shaped like:

```text
Trader bias: Short IWM reversion
Current state:
  stretch: +2.7 z-score from opening VWAP
  context: 5m impulse regime bullish
  time: 08:52 CT / 09:52 ET
  trigger: downside reversal range breakout with exhaustion

Mala verdict:
  PROCEED / PARTIAL / SKIP / INSUFFICIENT

Evidence:
  historically favorable region: z-score > 2.5, 09:45-10:10 ET trigger,
  bullish/mixed 5m context, confirmed 5m reversal range
  holdout expectancy: +x.xxR
  holdout win rate: yy%
  sample count: n
```

The first build only needs the historical surface and sample events. Live
current-state matching is a follow-up once the surface is credible.

Visual review should happen on the trader chart surface, not in a synthetic
static renderer. Use the generated playbook run plus cached bars to prepare a
TradingView overlay packet:

```bash
python -m src.research.playbook_visual_review \
  --run-dir research/results/playbooks/mean_reversion_at_extremes/<run_ts> \
  --symbol QQQ \
  --start 2026-05-04 \
  --end 2026-05-08 \
  --tv-symbol NASDAQ:QQQ
```

This keeps Mala responsible for research events and uses TradingView only for
human chart inspection. The packet writes `event_review.csv`,
`event_groups.csv`, a Pine overlay, and optional MCP apply/drawing scripts.
The Pine overlay is preferred because it preserves the real chart context
without deleting existing TradingView drawings.

---

## Existing Infra To Leverage

- `src.chronos.storage.LocalStorage` for historical minute bars
- `src.time_utils` for session/calendar handling
- `src.newton.transforms` for velocity, acceleration, and jerk-style features
- existing Newton relative-volume transforms for trigger-volume confirmation
- `src.newton.market_impulse` and `src.strategy.market_impulse` for
  MarketPulse stack concepts
- `src.strategy.elastic_band_reversion` as a reference for z-score, VPOC, and
  kinematic reversion feature wiring
- `src.newton.vpoc_daily` and auction-proxy work where the VPOC feature is
  reliable enough for research
- `src.oracle.metrics` for MFE/MAE-style excursion evidence
- `src.oracle.trade_simulator` and exit-policy machinery where bar-by-bar
  trade simulation is needed
- existing research artifact pattern under `research/results/`

---

## Implemented Entry Points

- Newton features:
  - `opening_vwap_rth`
  - `prior_rth_close_atr`
  - `prior_rth_close`
  - `daily_rth_atr_14`
  - `atr_distance_from_prior_rth_close`
  - `gap_state_rth_open`
  - `relative_volume_rth:<period>` as a rolling RTH-only baseline, not a
    session-reset opening-volume baseline
- Strategy: `src.strategy.intraday_mean_reversion.IntradayMeanReversionStrategy`
- Registry name: `Intraday Mean Reversion at Extremes`
- Runner:

```bash
python -m src.research.playbook_surface \
  mean-reversion-at-extremes-intraday \
  --symbols IWM,QQQ \
  --start 2021-05-13 \
  --end 2026-05-13 \
  --out-dir research/results/playbooks/mean_reversion_at_extremes/<run_ts>
```

Operator query:

```bash
python -m src.research.playbook_surface_query \
  --run-dir research/results/playbooks/mean_reversion_at_extremes/<run_ts> \
  --symbol QQQ \
  --direction short \
  --timestamp "2026-05-11 09:45 America/New_York"
```

The query command is playbook-aware, not a separate strategy. It loads the
surface run, asks the registered playbook adapter how to interpret the
timestamp state, and writes `QUERY_REVIEW.md` plus `query_result.json` under
`surface_queries/`.

Recommended analyst-desk query:

```bash
python -m src.research.playbook_surface_query \
  --run-dir research/results/playbooks/mean_reversion_at_extremes/<run_ts> \
  --symbol IWM \
  --direction short \
  --timestamp "2026-04-21 08:50 America/Chicago" \
  --mode state-management
```

`state-management` is the trader-facing consultation mode. It does not ask
whether an exact entry rule fired. It finds the nearest historical analogs for
the current state and requested bias, then reports forward MFE/MAE, reversion
versus continuation mix across 5m/10m/15m/30m/60m/session-close horizons, and
a management menu with quick scalp thresholds, VWAP retraces, and VWAP return.
The management menu leads with `survived`, meaning the target was reached before
a symmetric adverse move of the same size. `captured` is secondary context, not
the headline. Rows whose target is below the tradable floor
`max(0.10 * daily_rth_atr_14, 0.10% * price)` are omitted, because a one- or
two-cent VWAP retrace is bar noise, not an actionable options scalp. This is the
preferred desk answer when Suman brings the timestamp and wants to know what
usually happened next.
Same-minute target/stop ambiguity is conservative: the target must print before
the symmetric adverse move to count as survived.

Each state-management query appends one row to `consultation_log.csv` in the
run directory. The trader can later fill in taken/not-taken and actual outcome
columns, turning replay/live consultations into the forward-shadow journal for
this playbook. The log intentionally does not auto-pick a suggested exit; the
system reports the menu and the trader records the selected management row.
For historical review, use
`python -m src.research.playbook_consultation_log replay-close ...`; the trader
supplies `taken` and `selected_exit`, and Mala fills actual outcome fields from
cached bars. For live/manual review, use
`python -m src.research.playbook_consultation_log close ...` to update rows
after the trade. The generic journal vision lives in
`docs/MALA_2_2_CONSULTATION_JOURNAL.md`.

For live/replay compression, create a deterministic policy card from the query:

```bash
python -m src.research.playbook_policy_card \
  --query-json research/results/playbooks/mean_reversion_at_extremes/<run_ts>/surface_queries/<query_id>/query_result.json \
  --update-log
```

This card is not an LLM agent. It applies explicit thresholds, picks a usable
management row if one exists, and records the prefilled row in the journal.
Future external-context agents can add caveats, but they should not silently
overwrite the deterministic policy.

The card also includes a compact `STATE` and `ANALOG` read. `STATE` reports
where the queried timestamp ranks against prior same-symbol, same-bias,
same-entry-window history for VWAP stretch, prior-close ATR stretch, and
velocity. `ANALOG` reports whether the nearest historical cohort is tight,
workable, loose, or thin based on similarity quality.

Default operator policy:

```text
research/playbooks/operator_policies/mean_reversion_intraday_operator_v1.yaml
```

The query and policy-card JSON artifacts embed the policy id, version, source
path, and full config so the desk read is auditable after the fact.

Options overlay note: the packet is still underlying-first. A later options
layer must translate the underlying entry, stop, target, and invalidation into
contract selection, delta/expiry/spread constraints, option-PnL stops, and
position sizing. Do not treat underlying R as option R until that layer exists.

---

## Module Ownership

`src.research.playbook_surface` should stay a thin orchestration/reporting
runner. It should not own playbook-specific math or hard-coded grids.

Scalable split:

- **Newton:** reusable market features such as opening VWAP, prior close
  distance, gap state, velocity/jerk variants, relative volume, VPOC, and VWMA
  stack features.
- **Strategy:** playbook-specific event construction and `search_spec`
  declaration. This intraday reversion playbook should likely become its own
  strategy class rather than a pile of logic inside the research runner.
- **Oracle:** mostly unchanged; owns MFE/MAE, reward-risk, trade simulation,
  and exit-policy evaluation.
- **Research:** loads the playbook/strategy, asks it for its search surface,
  runs the bounded grid, and writes the conditional-surface artifacts. Surface
  rows should preserve the main conditioning dimensions, including gap state
  and volume-confirmation filter, rather than collapsing them into a note.
- **Query:** `src.research.playbook_surface_query` is a generic operator-query
  shell with a small playbook adapter registry. The shell owns artifact loading,
  timestamp selection, verdict formatting, and report writing. The adapter owns
  playbook-specific state language and management-packet translation.

This keeps Mala 2.2 from becoming tied to legacy promotion semantics while still
reusing the parts of Mala v2 that are already strong.

---

## Remaining Build Required

- run the full IWM/QQQ surface on the 2021-05-13 through current local cache
- inspect whether favorable regions are broad or fragile
- choose sample events for chart review
- treat `near_favorable` rows as chart-review leads only: they missed exactly
  one strict bound and must show that failed criterion in the review pack
- do not rank review candidates by raw holdout expectancy; use the candidate
  taxonomy so tail-payoff and holdout-only pockets do not masquerade as clean
  reversion
- compare the new `market_pulse_stage` axis against `no_filter` before concluding
  that Suman's stage read does or does not matter
- include `market_pulse_flip` as a normal exit family in the surface, not as a
  side experiment, so stage can be evaluated for trade management as well as
  entry filtering
- split catastrophic `risk_stop` from thesis-state `invalidation` before the
  playbook grows past first chart-review leads
- add an entry-quality dimension if chart review confirms late triggers:
  bound the trigger bar's stretch after the reversal so Mala can distinguish
  early reversal entries from snaps that already crossed too far through the
  reference level
- use timestamp queries to compare Mala's verdict with the trader's chart read
  before locking any execution packet
- lock one candidate packet only after chart semantics match the intended play
- add a targeted locked-packet stress runner that reuses M2/M5-style friction
  and execution-stress mechanics without publishing to old live/autonomous
  surfaces

Do not send broad playbook-search output into M1-M5 as if it were an autonomous
strategy promotion candidate. The old gates become targeted evidence checks only
after a human-reviewed packet is locked.

---

## First Acceptance Criteria

Phase 1 is useful if it can answer:

- Which stretch definition is strongest for IWM and QQQ?
- Does gap state change the playbook's validity?
- Does the edge require climactic velocity or work better without it?
- Does volume confirmation improve holdout expectancy or just reduce sample
  size?
- Does the favorable context look like bullish rubber-band snap, accumulation
  reversal, distribution fade, bearish continuation, or no stable stage?
- Does the 5-minute or 15-minute reversal range carry more evidence?
- Does waiting for 2 confirming bars improve expectancy or just reduce edge?
- Which stop/invalidation family avoids the worst failures?
- Which exit family is stable enough to review visually?
- Are favorable regions broad, or just one fragile parameter pocket?

Phase 1 is not useful if it only returns one optimized setting without showing
nearby parameter regions and sample counts.

---

## Do Not Build Yet

- no live trading verdict engine
- no Bhiksha adapter
- no option overlay
- no Google Sheet publication
- no TradingView/thinkorswim plotter
- no broad global playbook registry beyond the small query-adapter registry
- no autonomous scanner
