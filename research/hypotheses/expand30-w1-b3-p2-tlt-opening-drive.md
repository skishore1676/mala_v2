# Hypothesis: Expand30 W1-B3-P2 TLT Opening Drive

## Config
- id: `expand30-w1-b3-p2-tlt-opening-drive`
- state: `retune`
- decision: `retune`
- symbol_scope: `TLT`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-25T19:11:11+0000`

## Thesis
TLT may carry a tradable opening-drive continuation or failure pattern even as a macro ETF, especially when the first auction sets a directional move that persists through the early session.

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
`2026-04-25T141018` — strategy: `Opening Drive Classifier`

### Stages Executed
`M1`

### Notes
- M1 FAIL: signals=18<50; windows=1<3

### Decision
`retune`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b3-p2-tlt-opening-drive/2026-04-25T141018`
