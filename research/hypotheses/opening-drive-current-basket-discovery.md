# Hypothesis: Opening Drive Current Basket Discovery

## Config
- id: `opening-drive-current-basket-discovery`
- state: `pending`
- decision: ``
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Opening Drive Classifier`
- max_stage: `M5`
- max_configs: `48`
- last_run: ``

## Thesis
The opening range establishes the early auction imbalance. When price breaks away from that range with enough drive, volume, and kinematic confirmation, the move should continue long enough to create a tradable intraday options edge.

## Rules
- Entry: break or continuation away from the opening drive window.
- Direction: long and short are both evaluated by the strategy signal direction.
- Filters: use the strategy search surface for opening window, entry window, drive return, breakout buffer, volume, directional mass, jerk confirmation, and optional 5-minute regime alignment.
- Exit: use MFE/MAE reward-risk gates through M1-M5, then evaluate per-candidate thesis exits after M5.

## Notes
- Feasibility tag: `config-only`
- Prior IWM-only opening range work failed M1, but this full-basket replay should test whether the idea is symbol-specific rather than globally dead.
- Market regime columns are observational evidence, not a gate.
- Use `CATALOG_SELECTED.csv` as the concise selected-candidate readout after M5.

## Agent Report
Pending.
