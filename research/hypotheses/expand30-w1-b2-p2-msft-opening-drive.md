# Hypothesis: Expand30 MSFT Opening Drive

## Config
- id: `expand30-w1-b2-p2-msft-opening-drive`
- state: `retune`
- decision: `retune`
- symbol_scope: `MSFT`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-24T23:36:10+0000`

## Thesis
Apply the existing Opening Drive Classifier to MSFT as a config-only transfer test for same-session continuation after an opening directional drive. The honest mapping is continuation-only using current opening-drive, acceleration, optional jerk, directional-mass, and volume filters already exposed by mala_v2.

## Rules
- Detect an opening directional drive using the strategy's opening window.
- Allow continuation entries only, not failure-reversal entries.
- Permit long and short continuation paths.
- Let the engine search the declared Opening Drive parameter surface and stop at M1.

## Notes
- Packet: `W1-B2-P2`
- Queue row: `Q011`
- Strategy family label in campaign artifacts: `Opening Drive`
- Nearest mala_v2 registry strategy: `Opening Drive Classifier`

## Agent Report
### Run
`2026-04-24T183520` — strategy: `Opening Drive Classifier`

### Stages Executed
`M1`

### Notes
- M1 FAIL: signals=15<50; windows=1<3

### Decision
`retune`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b2-p2-msft-opening-drive/2026-04-24T183520`
