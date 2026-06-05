# Intraday Mean Reversion Extremes Strategy Lane Brief

Status: local M1 failed under realized playbook-path scoring.

This brief applies `STRATEGY_DEEP_IMPLEMENTATION_CHECKLIST.md` to the existing
intraday mean-reversion-at-extremes playbook and strategy surface.

## Objective

Convert the current playbook-derived surface into a normal Mala strategy lane
without disturbing the playbook consultation lane.

The intended path is:

```text
playbook thesis
-> deterministic strategy family
-> hypothesis/M-gate execution
-> M6/M7 evidence
-> local Mala_Evidence_v1 row(s)
-> Bhiksha shadow feasibility
-> operator approval before any external mutation
```

## Existing Assets

- Source playbook:
  `research/playbooks/mean_reversion_at_extremes_intraday_v1.md`
- Build/spec doc:
  `docs/MALA_2_2_INTRADAY_REVERSION_SURFACE_SPEC.md`
- Strategy class:
  `src.strategy.intraday_mean_reversion.IntradayMeanReversionStrategy`
- Factory display name:
  `Intraday Mean Reversion at Extremes`
- Playbook id:
  `mean-reversion-at-extremes-intraday`
- Current IWM/QQQ run:
  `research/results/playbooks/mean_reversion_at_extremes/20260515T_clean_rth_iwm_qqq_surface64`
- Current run symlink:
  `research/results/playbooks/mean_reversion_at_extremes/current`
- Current receipt facts:
  - `64` configs tested
  - `21,127` events
  - `256` conditional surface rows
  - `1` favorable row
  - `78` partial rows
  - `68` outside rows
  - `109` insufficient rows

The current playbook receipt explicitly says it is not a
`Mala_Evidence_v1`, `active_strategy`, or live authorization write. This lane
keeps that boundary and creates a separate strategy-promotion path.

## Proposed Strategy Identity

- Display name: `Intraday Mean Reversion at Extremes`
- Canonical strategy key: `intraday_mean_reversion_extremes`
- Source playbook: `mean-reversion-at-extremes-intraday`
- First symbol scope: `IWM,QQQ`
- First directions: long and short reversion
- Initial readiness target: `fixture_shadow`
- Publication target: local `Mala_Evidence_v1` artifacts only until approved

Resolved implementation note:

- `src/research/strategy_keys.py` now maps
  `Intraday Mean Reversion at Extremes` to
  `intraday_mean_reversion_extremes`.

## Strategy Translation

### Required Features

- timestamp and OHLCV bars
- RTH opening VWAP
- prior RTH close
- RTH ATR and prior-close ATR distance
- RTH gap state
- MarketPulse stage
- velocity and optional jerk
- optional RTH relative volume
- optional VPOC when using the VPOC stretch source

### Entry Trigger

The executable entry is:

```text
early-session stretch beyond threshold
-> reversal range is established
-> price breaks back through the reversal range
-> optional velocity, jerk, stage, gap, and volume filters pass
```

Long reversion fades downside stretch. Short reversion fades upside stretch.

### Initial Search Dimensions

- stretch source:
  `opening_vwap_rth`, `prior_rth_close_atr`, `vpoc_4h`
- stretch threshold:
  VWAP/VPOC z-score and prior-close ATR bins
- entry cutoff:
  `09:45`, `10:00`, `10:15`, `11:00`
- reversal range:
  `5`, `15` minutes
- confirmation:
  `1`, `2` bars
- velocity filter:
  no filter, aligned, climactic, non-climactic
- stage filter:
  no filter, bullish, accumulation, distribution, bearish
- gap state:
  no filter plus RTH gap buckets
- volume confirmation:
  no filter, `1.0`, `1.25`, `1.5` RTH relative-volume threshold
- stop family:
  reversal extreme, reversal midpoint, immediate entry-bar failure
- exit family:
  fixed R, VWAP return, partial retrace, MarketPulse flip, time stop

## Human-Only Judgment To Resolve

The current playbook assumes chart-review language:

- early stretch should look like a real extreme, not ordinary noise;
- reversal range should visually match the intended exhaustion/reclaim shape;
- favorable math with ugly chart semantics should not promote;
- stage context may matter, but the current surface did not prove it.

For automation, these cannot remain hidden requirements. They must become one of:

- deterministic filters;
- chart-review provenance only;
- explicit unresolved risks;
- future feature work.

## Current Evidence Read

The restored IWM/QQQ playbook run is useful, but not promotable by itself.

Reasons:

- only one favorable conditional row in the current receipt;
- the receipt explicitly calls for multiple-comparisons review before promotion;
- stop and thesis invalidation are still merged into one stop axis;
- options overlay is deferred;
- provider parity is evidence-plus-compile validation, not a strategy-lane
  M7 run for `Mala_Evidence_v1`;
- current output is a conditional surface, not hypothesis-agent evidence.

This means the current run should seed the first locked strategy candidate, not
be published directly.

## First Autonomous Implementation Slice

Before asking Suman to audit trading philosophy, Codex safely proved the target
local strategy contract enters the normal strategy lane:

1. Add explicit canonical strategy-key mapping.
2. Add a pending hypothesis file for the first bounded IWM/QQQ strategy-lane
   run.
3. Verify `research.search_space` can build bounded configs for this strategy.
4. Add balanced strategy-owned search configs so the first 64 configs cover the
   declared playbook-like surface.
5. Score M1/M2 with realized playbook stop/exit paths when a strategy declares
   playbook path columns.
6. Derive the Mala handoff signal window from
   `entry_window_start` / `entry_window_end`.
7. Add M1 failure diagnostics so terminal M1 failures retain config, frame,
   signal, and valid-path evidence.

No Google Sheet publication, oldmac sync, Bhiksha restart, or live runtime
mutation belongs in this slice.

## Human Audit Gate

Before broad M-gate execution, Suman should approve or revise:

- whether `intraday_mean_reversion_extremes` is the canonical key;
- whether the first run should include both IWM and QQQ;
- whether long and short should run together or be split;
- whether the declared target surface is faithful enough to the playbook or
  missing a required disqualifier/exit/invalidation distinction;
- whether chart-semantics rejection should be encoded now or treated as
  post-run review.

## Initial Recommendation

Start with a target-complete local strategy-lane integration pass:

- use the existing strategy class;
- fix the canonical key;
- create one pending hypothesis for IWM/QQQ;
- dry-run only;
- inspect whether the current hypothesis/M-stage machinery can consume the
  playbook strategy without special-casing;
- then run the declared target surface and let failures drive hardening,
  splitting, or boundary minimization.

## Local Integration Readback

Completed on branch `codex/intraday-mean-reversion-strategy-lane`:

- Added explicit canonical key mapping:
  `Intraday Mean Reversion at Extremes` ->
  `intraday_mean_reversion_extremes`.
- Added pending hypothesis:
  `research/hypotheses/intraday-mean-reversion-extremes-iwm-qqq.md`.
- Added a regression test for the canonical handoff key.
- Added focused tests for balanced search coverage, realized playbook-path
  scoring, and the strategy signal window.
- Focused tests passed:
  `35 passed`.
- Exact-surface parity is now regression-tested:
  `build_search_configs("Intraday Mean Reversion at Extremes", mode="discovery", max_configs=64)`
  matches the playbook's `balanced_axis_sweep_v1` 64-config surface.
- Hypothesis dry-run passed:
  - id: `intraday-mean-reversion-extremes-iwm-qqq`
  - strategy: `Intraday Mean Reversion at Extremes`
  - tickers: `IWM`, `QQQ`
  - max stage: `M2`
  - mode: `discovery`
  - configs: `64`
  - start stage: `M1`
- Local M1 run completed:
  - run:
    `data/results/hypothesis_runs/intraday-mean-reversion-extremes-iwm-qqq/2026-06-05T131625`
  - artifacts:
    `M1_detail.csv`, `M1_aggregate.csv`,
    `M1_FAILURE_DIAGNOSTICS.md`, `M1_failure_diagnostics.csv`,
    `RUN_SUMMARY.md`, `run_results.xlsx`
  - detail rows: `144`
  - aggregate rows: `65`
  - top rows: `0`
  - diagnostic signal rows: `128`
  - total diagnostic signals: `8,724`
  - valid simulated playbook paths: `7,269`
  - diagnostic errors: `0`
  - best aggregate row:
    `IWM combined vpoc_4h threshold=1.5 fixed_1r`,
    `169` OOS signals, `5` OOS windows,
    `avg_test_exp_r=-0.16568`, `pct_positive_oos_windows=0.20`

This proves the existing playbook strategy can enter the normal strategy-lane
runner without special-casing, and that the stop/exit surface is now scored as
realized playbook R instead of only generic forward MFE/MAE.

It also says the current IWM/QQQ strategy translation should not publish to
`Mala_Evidence_v1`: it failed M1 with no positive after-cost aggregate configs.
This is a useful strategic answer, not a runtime failure.

Current readiness: `locally_integrated_m1_failed`.

## Playbook Parity And M1 Failure Diagnosis

The strategy lane now considers the same bounded optimization surface as the
current playbook receipt, but it does not consider the same event population or
apply the same validation rule.

Surface parity:

- playbook receipt: `64` configs from `balanced_axis_sweep_v1`;
- strategy lane discovery surface: `64` configs;
- exact config-set parity: `true`;
- regression guard:
  `tests/test_search_space.py::test_intraday_mean_reversion_surface64_matches_playbook_surface`;
- M1 failure diagnostics now emit the same SHA1-style `config_id` used by the
  playbook receipt, so rows like `f09fcdd6b5` can be compared directly.

Event-population difference:

- playbook receipt range: `2021-05-13 -> 2026-05-12`;
- playbook receipt events: `21,127`;
- M1 calibration range: `2024-01-02 -> 2025-11-30`;
- M1 raw diagnostic signals across 64 configs x 2 tickers: `8,724`;
- M1 valid simulated playbook paths: `7,269`;
- M1 then splits those paths into five rolling `6m train / 3m test` windows
  and requires at least `15` train and `15` test signals per
  ticker/config/direction row.

Output difference:

- playbook output is a conditional surface and chart-review map;
- playbook favorable row:
  `f09fcdd6b5`, `IWM short`, `prior_rth_close_atr > 1.0`,
  `stage=no_filter`, `gap=no_filter`, `stop=reversal_extreme`,
  `exit=fixed_1r`, `n=114`, calibration `+0.1997R`, holdout `+0.1818R`;
- M1 output is after-cost rolling walk-forward evidence;
- latest M1 output had no positive aggregate rows and no M1 top rows.

Why the best playbook row did not appear in M1:

- `f09fcdd6b5` was included in the exact M1 run.
- In the M1 calibration frame for IWM it generated `112` raw signals:
  `61` long and `51` short.
- After playbook path simulation, IWM had only `59` valid paths:
  `35` long and `24` short.
- Full-span IWM short still had positive raw expectancy (`+0.2000R`), but the
  8 bps M1 cost converted to about `0.2346R`, leaving only about `+0.0154R`
  net over the whole calibration span.
- No rolling short fold had both train and test counts at or above `15`.
  The short fold counts were:
  `4/6`, `8/3`, `9/2`, `5/4`, and `6/0` for train/test.
- Therefore M1 produced no detail or aggregate row for the favorable playbook
  candidate. It was not rejected as a bad row after ranking; it was too sparse
  for M1's rolling evidence contract.

The second playbook lead, `e7b617137a`, was also included. It was even thinner:
IWM produced `49` raw signals and `27` valid paths in the M1 calibration frame;
its short side had `11` valid paths and also never reached the rolling
train/test minimum.

Interpretation:

- The first M1 failure was partly contaminated by a two-config sampler mismatch.
  That is now fixed and tested.
- The latest M1 failure is not a surface-parity bug.
- The failure is mainly a validation-contract mismatch: the playbook surface is
  a longer-range, no-cost, conditional chart-review lead generator, while M1 is
  a shorter-range, after-cost, rolling OOS gate.
- The playbook's "right signals" did not become the strategy lane's "right
  evidence" because the favorable row is sparse, concentrated, and only
  marginal after costs in the M1 calibration frame.

## Decision Implication

The current translation is not a candidate for Bhiksha adoption or evidence
publication. The next legitimate directions are:

- write a materially different thesis, probably adding deterministic versions
  of the playbook's chart-quality disqualifiers before reopening the lane;
- split the hypothesis by symbol/direction/session slice only if Suman believes
  the combined surface is hiding a specific playbook-intended subcase;
- introduce a playbook-packet validation stage separate from M1 if the desired
  question is "does this chart-review lead deserve more manual/visual work?"
  rather than "does this row already satisfy strategy-lane OOS evidence?";
- keep the playbook lane as consultation/provenance and do not force this
  thesis through the strategy lane unless the thesis changes.
