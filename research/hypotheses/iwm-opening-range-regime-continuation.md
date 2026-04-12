# Hypothesis: IWM Opening Range Regime Continuation

## Config
- id: `iwm-opening-range-regime-continuation`
- state: `kill`
- decision: `kill`
- symbol_scope: `IWM`
- strategy: `Opening Drive Classifier`
- max_stage: `M5`
- last_run: `2026-04-05T21:06:11+0000`

## Thesis
If IWM crosses the 15-minute market-open boundary in one direction, that move
should continue when it is aligned with the 5-minute directional regime.

## Rules
- Entry: cross of the 15-minute opening range high/low
- Direction: continuation only (long above range, short below)
- Filter: 5-minute regime must agree with trade direction
- Exit: at R:R targets (MFE/MAE based)

## Notes
- Nearest existing strategy: `Opening Drive Classifier` with `use_regime_filter=True` and `regime_timeframe=5m`
- Invalidation: no stable M1 edge, or regime alignment doesn't improve continuation thesis

## Agent Report
### Run
`2026-04-05T210611` — strategy: `Opening Drive Classifier`

### Stages Executed
`M1`

### Notes
- M1 FAIL: no positive configs found
- No positive exp_r found in any config. Recommend kill.

### Decision
`kill`

### Artifacts
`data/results/hypothesis_runs/iwm-opening-range-regime-continuation/2026-04-05T210611`
