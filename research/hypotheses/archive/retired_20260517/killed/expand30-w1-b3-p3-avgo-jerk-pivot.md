# Hypothesis: Expand30 W1-B3-P3 AVGO Jerk-Pivot

## Config
- id: `expand30-w1-b3-p3-avgo-jerk-pivot`
- state: `kill`
- decision: `kill`
- symbol_scope: `AVGO`
- strategy: `Jerk-Pivot Momentum (tight)`
- max_stage: `M1`
- last_run: `2026-04-22T14:13:55+0000`

## Thesis
AVGO may express the same jerk-pivot transfer through fast semiconductor momentum bursts, where jerk sign changes near VPOC capture intraday continuation or exhaustion on a liquid high-beta name.

## Rules
- Entry: use `Jerk-Pivot Momentum (tight)` discovery over the existing VPOC proximity, jerk lookback, kinematic periods, and volume-filter surface.
- Direction: allow both long and short paths; let the declared strategy search surface determine survivors.
- Filters: use the existing optional volume gate and standard time filter already encoded by the strategy.
- Exit: use the standard mala_v2 M1 evaluation only.

## Notes
- Packet: `W1-B3-P3`
- Strategy family transfer prior: `Jerk-Pivot`
- Feasibility tag: `config-only`
- Bounded to dry-run then `--max-stage M1` only.

## Agent Report
### Run
`2026-04-22T091122` — strategy: `Jerk-Pivot Momentum (tight)`

### Stages Executed
`M2 → M3 → M4`

### Notes
- M2: 3 candidates promoted
- M3: 45 detail rows
- M4: 0 promoted

### Decision
`kill`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b3-p3-avgo-jerk-pivot/2026-04-22T091122`
