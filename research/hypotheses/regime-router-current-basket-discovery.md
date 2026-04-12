# Hypothesis: Regime Router Current Basket Discovery

## Config
- id: `regime-router-current-basket-discovery`
- state: `pending`
- decision: ``
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Regime Router (Kinematic + Compression)`
- max_stage: `M5`
- max_configs: `16`
- last_run: ``

## Thesis
Momentum and compression-breakout logic should work in different intraday regimes. A router that sends trend regimes to Kinematic Ladder and compression regimes to Compression Breakout should reduce false positives compared with either standalone family.

## Rules
- Entry: route to Kinematic Ladder in trend regimes and Compression Breakout in compression regimes.
- Direction: long and short are both evaluated by the selected sub-strategy signal direction.
- Filters: use the router search surface for volatility and trend thresholds.
- Exit: use MFE/MAE reward-risk gates through M1-M5, then evaluate per-candidate thesis exits after M5.

## Notes
- Feasibility tag: `config-only`
- This is a meta-strategy replay; compare results against standalone Kinematic Ladder and Compression Breakout before trusting it.
- Market regime columns are observational evidence, not a gate.
- Use `CATALOG_SELECTED.csv` as the concise selected-candidate readout after M5.

## Agent Report
Pending.
