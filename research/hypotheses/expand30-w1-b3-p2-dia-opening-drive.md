# Hypothesis: Expand30 W1-B3-P2 DIA Opening Drive

## Config
- id: `expand30-w1-b3-p2-dia-opening-drive`
- state: `retune`
- decision: `retune`
- symbol_scope: `DIA`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-22T14:39:41+0000`

## Thesis
DIA may inherit an opening-drive continuation edge when broad-index auction pressure sets a clear early directional move that continues instead of reverting quickly.

## Rules
- Entry: use `Opening Drive Classifier` discovery over the standard opening-window and entry-window surface.
- Direction: allow both long and short continuation/failure paths; let the strategy search surface determine survivors.
- Filters: evaluate breakout buffer, drive threshold, kinematic periods, volume, directional mass, jerk confirmation, and optional 5-minute regime alignment from the existing strategy surface.
- Exit: use the standard mala_v2 M1 evaluation only.

## Notes
- Packet: `W1-B3-P2`
- Strategy family transfer prior: `Opening Drive`
- Feasibility tag: `config-only`
- Bounded to dry-run then `--max-stage M1` only.

## Agent Report
### Run
`2026-04-22T093920` — strategy: `Opening Drive Classifier`

### Stages Executed
`M2`

### Notes
- M2: 0 candidates promoted

### Decision
`retune`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b3-p2-dia-opening-drive/2026-04-22T093920`
