# Hypothesis: Intraday Mean Reversion Extremes - IWM/QQQ Strategy Lane

## Config
- id: `intraday-mean-reversion-extremes-iwm-qqq`
- state: `kill`
- decision: `kill`
- symbol_scope: `IWM, QQQ`
- strategy: `Intraday Mean Reversion at Extremes`
- max_stage: `M2`
- max_configs: `64`
- last_run: `2026-06-05T18:21:02+0000`

## Thesis
The existing mean-reversion-at-extremes playbook can be translated into a
deterministic strategy-family lane: fade early-session IWM/QQQ stretches after
a confirmed reversal range break, then let Mala discover whether any parameter
region survives the normal strategy evidence path.

## Rules
- Entry: early-session upside or downside stretch beyond the configured
  reference threshold.
- Confirmation: price breaks back through a 5- or 15-minute reversal range.
- Filters: search stretch source, stage, gap state, velocity, jerk, and RTH
  relative-volume confirmation.
- Direction: long reversion after downside stretch and short reversion after
  upside stretch.
- Exit: search the existing stop and exit families before locking a thesis exit.

## Notes
- Source playbook: `research/playbooks/mean_reversion_at_extremes_intraday_v1.md`
- Current playbook run:
  `research/results/playbooks/mean_reversion_at_extremes/current`
- This hypothesis is a local strategy-lane integration probe. It must not
  publish to Google Sheets, mutate `active_strategy`, sync oldmac, or change
  Bhiksha runtime state without explicit approval.
- First max stage is `M2` to expose strategy-lane compatibility before any full
  promotion attempt. Search cap is `64` to match the current balanced playbook
  surface instead of truncating the target surface.

## Agent Report
### Run
`2026-06-05T131625` — strategy: `Intraday Mean Reversion at Extremes`

### Stages Executed
`M1`

### Notes
- M1 FAIL: no positive configs found
- M1 diagnostics: M1_FAILURE_DIAGNOSTICS.md

### Decision
`kill`

### Artifacts
`/Users/suman/code/mala_v2/data/results/hypothesis_runs/intraday-mean-reversion-extremes-iwm-qqq/2026-06-05T131625`
