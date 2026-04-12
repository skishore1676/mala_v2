# Hypothesis: Jerk-Pivot Momentum Current Basket Discovery

## Config
- id: `jerk-pivot-current-basket-discovery`
- state: `pending`
- decision: ``
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Jerk-Pivot Momentum (tight)`
- max_stage: `M5`
- max_configs: `48`
- last_run: ``

## Thesis
Jerk inflections near VPOC can identify moments where intraday momentum is re-accelerating from a high-participation reference level. If velocity, acceleration, and jerk align near VPOC, continuation should be strong enough to survive cost and execution stress.

## Rules
- Entry: jerk sign-change confirms velocity and acceleration alignment near VPOC.
- Direction: long and short are both evaluated by the strategy signal direction.
- Filters: use the strategy search surface for VPOC proximity, jerk lookback, kinematic lookback, volume filter, and volume multiplier.
- Exit: use MFE/MAE reward-risk gates through M1-M5, then evaluate per-candidate thesis exits after M5.

## Notes
- Feasibility tag: `config-only`
- Prior v1 discoveries around jerk/kinematics are leads only; this run must pass v2 gates on its own.
- Market regime columns are observational evidence, not a gate.
- Use `CATALOG_SELECTED.csv` as the concise selected-candidate readout after M5.

## Agent Report
Pending.
