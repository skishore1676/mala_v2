# Hypothesis: Expand30 GOOGL Opening Drive

## Config
- id: `expand30-w1-b2-p2-googl-opening-drive`
- state: `kill`
- decision: `kill`
- symbol_scope: `GOOGL`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-22T13:55:22+0000`

## Thesis
Apply the existing Opening Drive Classifier to GOOGL as a config-only transfer test for same-session continuation after an opening directional drive. The honest mapping is continuation-only using current opening-drive, acceleration, optional jerk, directional-mass, and volume filters already exposed by mala_v2.

## Rules
- Detect an opening directional drive using the strategy's opening window.
- Allow continuation entries only, not failure-reversal entries.
- Permit long and short continuation paths.
- Let the engine search the declared Opening Drive parameter surface and stop at M1.

## Notes
- Packet: `W1-B2-P2`
- Queue row: `Q013`
- Strategy family label in campaign artifacts: `Opening Drive`
- Nearest mala_v2 registry strategy: `Opening Drive Classifier`

## Agent Report
### Run
`2026-04-22T085342` — strategy: `Opening Drive Classifier`

### Stages Executed
`M2 → M3 → M4`

### Notes
- M2: 1 candidates promoted
- M3: 9 detail rows
- M4: 0 promoted

### Decision
`kill`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b2-p2-googl-opening-drive/2026-04-22T085342`
