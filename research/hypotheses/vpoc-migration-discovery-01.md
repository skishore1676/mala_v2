## Config
- id:           `vpoc-migration-discovery-01`
- state:        `completed`
- decision:     `promote`
- symbol_scope: `SPY,QQQ,IWM,NVDA,TSLA,AMD,MU,SMH,PANW,TLT,XLE,AAPL`
- strategy:     `Compression Expansion Breakout`
- max_stage:    `M5`
- last_run:     `2026-05-17T12:31:55+0000`

## Thesis
Big multi-day moves may be preceded by compressed realized volatility while the
session VPOC migrates directionally. This first shadow-safe pass uses the
existing compression-breakout research rail plus the new daily VPOC helper as
an explicit feature-readout. Do not claim promotion until a dedicated strategy
surface passes the normal M1-M5 gates.

## Big Ideas Mapping
- idea: VPOC migration -> compression ignition
- feasibility: new-feature
- shadow boundary: research artifacts only; no live authorization

## Agent Report
### Run
`2026-05-17T072830` — strategy: `Compression Expansion Breakout`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=100%  exp_r=+0.0749  signals=1749  windows=5
- M2: 8 candidates promoted
- M3: 120 detail rows
- M4: 8 promoted
- M5: 32 execution mappings
- exit_opt: 2 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/research/results/hypothesis_runs/vpoc-migration-discovery-01/2026-05-17T072830`
