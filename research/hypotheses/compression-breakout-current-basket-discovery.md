# Hypothesis: Compression Breakout Current Basket Discovery

## Config
- id: `compression-breakout-current-basket-discovery`
- state: `pending`
- decision: ``
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Compression Expansion Breakout`
- max_stage: `M5`
- max_configs: `48`
- last_run: ``

## Thesis
Low realized volatility periods can precede expansion moves when trend bias and participation confirm. Breakouts from compressed ranges should produce durable intraday continuation only on symbols where post-compression expansion is clean enough to survive costs.

## Rules
- Entry: breakout above/below the prior range after volatility compression.
- Direction: long and short are both evaluated by the strategy signal direction.
- Filters: use the strategy search surface for compression window, breakout lookback, compression factor, velocity lookback, and volume filter.
- Exit: use MFE/MAE reward-risk gates through M1-M5, then evaluate per-candidate thesis exits after M5.

## Notes
- Feasibility tag: `config-only`
- Compare surviving symbols against Regime Router to see whether explicit routing adds value.
- Market regime columns are observational evidence, not a gate.
- Use `CATALOG_SELECTED.csv` as the concise selected-candidate readout after M5.

## Agent Report
Pending.
