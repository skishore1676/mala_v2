# Hypothesis: Expand30 W1-B3-P1 XLK Opening Drive

## Config
- id: `expand30-w1-b3-p1-xlk-opening-drive`
- state: `kill`
- decision: `kill`
- symbol_scope: `XLK`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-22T14:25:02+0000`

## Thesis
XLK may show transferable opening-drive continuation behavior at the sector level, though prior impulse evidence was thin enough that this needs a clean M1 read before any deeper continuation claim.

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
`M2 → M3 → M4`

### Notes
- M2: 1 candidates promoted
- M3: 6 detail rows
- M4: 0 promoted

### Decision
`kill`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b3-p1-xlk-opening-drive/2026-04-22T092236`
