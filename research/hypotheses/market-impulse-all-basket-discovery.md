# Hypothesis: Market Impulse — Full Basket Discovery

## Config
- id: `market-impulse-all-basket-discovery`
- state: `completed`
- decision: `promote`
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M5`
- last_run: `2026-04-12T20:32:13+0000`

## Thesis
Market Impulse captures the directional move that follows a confirmed
VWMA cross-and-reclaim at the open. When the short-term VWMA stack
aligns and price reclaims above (or below) the mid VWMA after a brief
pullback, the opening impulse tends to continue for 45–90 minutes.

This is a full-basket discovery run across all 9 locally-cached tickers
to find which symbols and directions carry reliable edge, and what
parameter regime (entry buffer, window, VWMA periods, regime timeframe)
is most durable across cost frictions and out-of-sample windows.

## Rules
- Entry: VWMA cross-and-reclaim confirmed at open (first 3–5 min buffer)
- Direction: long and short both evaluated
- Entry window: 45–90 minutes from open
- Filter: regime timeframe alignment (5m / 15m / 30m / 1h)
- VWMA stack: three-period configuration swept

## Notes
- Strategy declared `search_spec` with constraint: entry_buffer < entry_window
- Grid: entry_buffer [3,5] × entry_window [45,60,90] × regime_tf [5m,15m,30m,1h] × vwma [(5,13,21),(8,21,34),(10,20,40)]
- Prior catalog entries for NVDA long, SPY short, TSLA short exist in v1 — treat as leads, not validation
- First v2 run of this strategy at scale

## Agent Report
### Run
`2026-04-12T152932` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=100%  exp_r=+0.4284  signals=456  windows=5
- M2: 27 candidates promoted
- M3: 405 detail rows
- M4: 22 promoted
- M5: 88 execution mappings
- exit_opt: 7 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/market-impulse-all-basket-discovery/2026-04-12T152932`
