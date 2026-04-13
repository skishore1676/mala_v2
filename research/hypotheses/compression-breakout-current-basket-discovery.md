# Hypothesis: Compression Breakout Current Basket Discovery

## Config
- id: `compression-breakout-current-basket-discovery`
- state: `completed`
- decision: `promote`
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Compression Expansion Breakout`
- max_stage: `M5`
- max_configs: `48`
- last_run: `2026-04-12T23:42:07+0000`

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
### Run
`2026-04-12T183837` — strategy: `Compression Expansion Breakout`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=100%  exp_r=+0.1043  signals=2722  windows=5
- M2: 12 candidates promoted
- M3: 180 detail rows
- M4: 8 promoted
- M5: 32 execution mappings

### Decision
`promote`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/compression-breakout-current-basket-discovery/2026-04-12T183837`
