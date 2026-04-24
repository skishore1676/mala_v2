# Hypothesis: Expand30 W1-B2-P3 MU Opening Drive

## Config
- id: `expand30-w1-b2-p3-mu-opening-drive`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-22T14:09:58+0000`

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
`2026-04-22T090751` — strategy: `Opening Drive Classifier`

### Stages Executed
`M2 → M3 → M4 → M5`

### Notes
- M2: 4 candidates promoted
- M3: 51 detail rows
- M4: 2 promoted
- M5: 8 execution mappings

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b2-p3-mu-opening-drive/2026-04-22T090751`
