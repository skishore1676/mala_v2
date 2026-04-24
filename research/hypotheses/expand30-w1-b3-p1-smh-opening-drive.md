# Hypothesis: Expand30 W1-B3-P1 SMH Opening Drive

## Config
- id: `expand30-w1-b3-p1-smh-opening-drive`
- state: `completed`
- decision: `promote`
- symbol_scope: `SMH`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-22T14:25:38+0000`

## Thesis
SMH may carry the same opening-drive continuation behavior already observed in single-name semiconductor tests, with the sector ETF capturing broad early directional follow-through when the opening auction resolves cleanly.

## Rules
- Entry: use `Opening Drive Classifier` discovery over the standard opening-window and entry-window surface.
- Direction: allow both long and short continuation/failure paths; let the strategy search surface determine survivors.
- Filters: evaluate breakout buffer, drive threshold, kinematic periods, volume, directional mass, jerk confirmation, and optional 5-minute regime alignment from the existing strategy surface.
- Exit: use the standard mala_v2 M1 evaluation only.

## Notes
- Packet: `W1-B3-P1`
- Strategy family transfer prior: `Opening Drive`
- Feasibility tag: `config-only`
- Bounded to dry-run then `--max-stage M1` only.

## Agent Report
### Run
`2026-04-22T092236` — strategy: `Opening Drive Classifier`

### Stages Executed
`M2 → M3 → M4 → M5`

### Notes
- M2: 4 candidates promoted
- M3: 52 detail rows
- M4: 3 promoted
- M5: 12 execution mappings

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b3-p1-smh-opening-drive/2026-04-22T092236`
