# Hypothesis: Expand30 W1-B4-P1 PANW Jerk-Pivot

## Config
- id: `expand30-w1-b4-p1-panw-jerk-pivot`
- state: `retune`
- decision: `retune`
- symbol_scope: `PANW`
- strategy: `Jerk-Pivot Momentum (tight)`
- max_stage: `M1`
- last_run: `2026-04-22T13:20:28+0000`

## Thesis
PANW may extend the jerk-pivot family into tactical security software if local jerk inflections near VPOC can isolate tradeable momentum continuation or exhaustion on a liquid single-name software profile.

## Rules
- Entry: use `Jerk-Pivot Momentum (tight)` discovery over the existing VPOC proximity, jerk lookback, kinematic periods, and volume-filter surface.
- Direction: allow both long and short paths; let the declared strategy search surface determine survivors.
- Filters: use the existing optional volume gate and standard time filter already encoded by the strategy.
- Exit: use the standard mala_v2 M1 evaluation only.

## Notes
- Packet: `W1-B4-P1`
- Strategy family transfer prior: `Jerk-Pivot`
- Feasibility tag: `config-only`
- Bounded to dry-run then `--max-stage M1` only.

## Agent Report
### Run
`2026-04-22T081910` — strategy: `Jerk-Pivot Momentum (tight)`

### Stages Executed
`M1`

### Notes
- M1 FAIL: pct_pos=40%<60%

### Decision
`retune`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b4-p1-panw-jerk-pivot/2026-04-22T081910`
