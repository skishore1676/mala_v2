# Hypothesis: Expand30 W1-B2-P3 MU Opening Drive

## Config
- id: `expand30-w1-b2-p3-mu-opening-drive`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU`
- strategy: `Opening Drive Classifier`
- max_stage: `M5`
- last_run: `2026-05-17T12:08:25+0000`

## Thesis
MU may express the same opening-drive continuation behavior observed in prior opening-drive work, with semi-specific intraday momentum carrying beyond the opening window when the initial drive is real.

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
`2026-05-17T070810` — strategy: `Opening Drive Classifier`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=60%  exp_r=+0.0607  signals=164  windows=5
- M2: 2 candidates promoted
- M3: 30 detail rows
- M4: 2 promoted
- M5: 8 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/data/results/hypothesis_runs/expand30-w1-b2-p3-mu-opening-drive/2026-05-17T070810`
