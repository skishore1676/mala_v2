# Hypothesis: Expand30 AMZN Opening Drive

## Config
- id: `expand30-w1-b2-p2-amzn-opening-drive`
- state: `completed`
- decision: `promote`
- symbol_scope: `AMZN`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-22T13:53:34+0000`

## Thesis
Apply the existing Opening Drive Classifier to AMZN as a config-only transfer test for same-session continuation after an opening directional drive. The honest mapping is continuation-only using current opening-drive, acceleration, optional jerk, directional-mass, and volume filters already exposed by mala_v2.

## Rules
- Detect an opening directional drive using the strategy's opening window.
- Allow continuation entries only, not failure-reversal entries.
- Permit long and short continuation paths.
- Let the engine search the declared Opening Drive parameter surface and stop at M1.

## Notes
- Packet: `W1-B2-P2`
- Queue row: `Q012`
- Strategy family label in campaign artifacts: `Opening Drive`
- Nearest mala_v2 registry strategy: `Opening Drive Classifier`

## Agent Report
### Run
`2026-04-22T085239` — strategy: `Opening Drive Classifier`

### Stages Executed
`M2 → M3 → M4 → M5`

### Notes
- M2: 1 candidates promoted
- M3: 15 detail rows
- M4: 1 promoted
- M5: 4 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b2-p2-amzn-opening-drive/2026-04-22T085239`
