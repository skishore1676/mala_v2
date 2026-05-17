# Hypothesis: Regime Router Current Basket Discovery

## Config
- id: `regime-router-current-basket-discovery`
- state: `kill`
- decision: `kill`
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Regime Router (Kinematic + Compression)`
- max_stage: `M5`
- max_configs: `16`
- last_run: `2026-04-13T00:06:57+0000`

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
### Run
`2026-04-12T190642` — strategy: `Regime Router (Kinematic + Compression)`

### Stages Executed
`M1 → M2 → M3 → M4`

### Notes
- M1 PASS: pct_pos=100%  exp_r=+0.0443  signals=3958  windows=5
- M2: 1 candidates promoted
- M3: 15 detail rows
- M4: 0 promoted

### Decision
`kill`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/regime-router-current-basket-discovery/2026-04-12T190642`
