# Hypothesis: Expand30 W1-B2-P3 AVGO Opening Drive

## Config
- id: `expand30-w1-b2-p3-avgo-opening-drive`
- state: `completed`
- decision: `promote`
- symbol_scope: `AVGO`
- strategy: `Opening Drive Classifier`
- max_stage: `M5`
- last_run: `2026-05-17T12:08:10+0000`

## Thesis
AVGO may carry the established opening-drive continuation behavior seen in the current basket, especially when the first auction imbalance resolves into a directional continuation rather than an early fade.

## Rules
- Entry: use `Opening Drive Classifier` discovery over the standard opening-window and entry-window surface.
- Direction: allow both long and short continuation/failure paths; let the strategy search surface determine survivors.
- Filters: evaluate breakout buffer, drive threshold, kinematic periods, volume, directional mass, jerk confirmation, and optional 5-minute regime alignment from the existing strategy surface.
- Exit: use the standard mala_v2 M1 evaluation only.

## Notes
- Packet: `W1-B2-P3`
- Strategy family transfer prior: `Opening Drive`
- Feasibility tag: `config-only`
- Bounded to dry-run then `--max-stage M1` only.

## Agent Report
### Run
`2026-05-17T070754` — strategy: `Opening Drive Classifier`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.1171  signals=162  windows=5
- M2: 4 candidates promoted
- M3: 60 detail rows
- M4: 2 promoted
- M5: 8 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/data/results/hypothesis_runs/expand30-w1-b2-p3-avgo-opening-drive/2026-05-17T070754`
