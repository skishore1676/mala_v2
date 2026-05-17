# Hypothesis: Opening Drive v2 Current Basket Discovery

## Config
- id: `opening-drive-v2-current-basket-discovery`
- state: `retune`
- decision: `retune`
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Opening Drive v2 (Short Continue)`
- max_stage: `M5`
- max_configs: `48`
- last_run: `2026-04-13T00:04:12+0000`

## Thesis
Opening Drive v2 narrows the opening-drive idea toward short continuation after a stronger early move. If the opening auction exhausts upward or confirms bearish continuation, the short-side continuation profile should survive costs better than the broader opening-drive classifier.

## Rules
- Entry: short continuation setup after the opening drive window.
- Direction: short-only behavior is expected from the strategy defaults; any long or combined survivor should be treated as a code/config surprise.
- Filters: use the strategy search surface for opening window, entry window, drive return, breakout buffer, volume, directional mass, jerk confirmation, and optional regime alignment.
- Exit: use MFE/MAE reward-risk gates through M1-M5, then evaluate per-candidate thesis exits after M5.

## Notes
- Feasibility tag: `config-only`
- This is a useful contrast against the broader Opening Drive Classifier.
- Market regime columns are observational evidence, not a gate.
- Use `CATALOG_SELECTED.csv` as the concise selected-candidate readout after M5.

## Agent Report
### Run
`2026-04-12T190235` — strategy: `Opening Drive v2 (Short Continue)`

### Stages Executed
`M1`

### Notes
- M1 FAIL: signals=47<50

### Decision
`retune`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/opening-drive-v2-current-basket-discovery/2026-04-12T190235`
