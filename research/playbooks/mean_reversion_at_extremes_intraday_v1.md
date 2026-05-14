# Intraday Mean Reversion at Extremes - Playbook Search Spec v1

**Status:** Phase 0 search contract
**Owner:** Suman + Codex
**Family spec:** `research/playbooks/mean_reversion_at_extremes_v0.md`
**Build spec:** `docs/MALA_2_2_INTRADAY_REVERSION_SURFACE_SPEC.md`
**Implementation state:** no code yet

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
- **Visual plotting:** deferred until the parameter surface produces candidates
  worth inspecting

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
| `z_score_from_opening_vwap` | thresholds `1.5, 2.0, 2.5, 3.0, 3.5` | preferred opening-drive reference if available |
| `z_score_from_session_vwap` | thresholds `1.5, 2.0, 2.5, 3.0, 3.5` | fallback/companion to opening VWAP |
| `z_score_from_vpoc_4h` | thresholds `1.5, 2.0, 2.5, 3.0, 3.5` | uses existing auction/VPOC direction where reliable |
| `atr_distance_from_reference` | binned by symbol and direction | keeps surface comparable across IWM/QQQ |
| `velocity_1m` | percentile or ATR-normalized bins | distinguishes grind from impulse |
| `velocity_5m` | percentile or ATR-normalized bins | smoother version of the same question |

Search interaction:

- stretch with violent/climactic velocity
- stretch with moderate/non-climactic velocity
- stretch without a velocity filter

The key question is not only "was it stretched?" It is whether the edge lives in
a rubber-band snap after a fast move, or in a slower extension that becomes
unsupported.

### 2. Context: Defining the Stage

Purpose: test whether the fade works because the market is in an exhaustion
state, a trend-continuation state, or a mixed/transition state.

Candidate filters:

| Feature family | Initial candidates | Notes |
| --- | --- | --- |
| `impulse_regime_5m` | `bullish`, `bearish`, `neutral`, `no_filter` | reusable Market Impulse vocabulary |
| `vwma_stack_8_21_34` or `vma_stack_8_21_34` | bullish stack, bearish stack, mixed, no filter | approximates Suman's stage read |
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

Candidate invalidation families:

- no favorable movement after `10`, `20`, or `30` minutes
- new extreme after confirmation
- VWMA/VMA stack flips into continuation
- price accepts beyond the stretch reference instead of rejecting it
- max adverse excursion exceeds the tested stop family

The output should say which invalidation family actually protected expectancy.
It should not hard-code the user's current crude stop idea as final truth.

### 5. Exit Surface

Purpose: discover what validates the reversion and what should be monetized.

Candidate exits:

| Exit family | Initial candidates |
| --- | --- |
| fixed R | `0.5R`, `1.0R`, `1.5R`, `2.0R` |
| return to VWAP/reference | opening VWAP, session VWAP, VPOC/reference if available |
| partial retrace | `25%`, `50%`, `75%` retrace of the early extreme |
| short MA/VWMA return | return to 8/21/34 reference family |
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

---

## Existing Infra To Leverage

- `src.chronos.storage.LocalStorage` for historical minute bars
- `src.time_utils` for session/calendar handling
- `src.newton.transforms` for velocity, acceleration, and jerk-style features
- `src.newton.market_impulse` and `src.strategy.market_impulse` for Market
  Impulse / VWMA stack concepts
- `src.strategy.elastic_band_reversion` as a reference for z-score, VPOC, and
  kinematic reversion feature wiring
- `src.newton.vpoc_daily` and auction-proxy work where the VPOC feature is
  reliable enough for research
- existing research artifact pattern under `data/results/`

---

## New Build Required

- a playbook-surface CLI, likely `python -m src.research.playbook_surface`
- a playbook spec loader or hard-coded v1 grid module for this first slice
- feature adapters that normalize naming across existing Newton/strategy
  outputs
- event construction for reversal-range breakout candidates
- calibration/holdout split logic for parameter-surface reporting
- stop, invalidation, and exit evaluators in R units
- receipt and CSV writers:
  - `RECEIPT.md`
  - `conditional_surface_by_symbol.csv`
  - `feature_bins_by_symbol.csv`
  - `sample_events.csv`
  - `config.json`

---

## First Acceptance Criteria

Phase 1 is useful if it can answer:

- Which stretch definition is strongest for IWM and QQQ?
- Does the edge require climactic velocity or work better without it?
- Does the favorable context look like bullish rubber-band snap, bearish
  continuation, neutral transition, or no stable regime?
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
- no global playbook registry
- no autonomous scanner
