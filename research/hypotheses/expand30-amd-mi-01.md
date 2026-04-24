# Hypothesis: Expand30 AMD Market Impulse

## Config
- id: `expand30-amd-mi-01`
- state: `completed`
- decision: `promote`
- symbol_scope: `AMD`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-24T13:40:39+0000`

## Thesis
Run Expand30 single-ticker Market Impulse feasibility/M1 on AMD. This is a high-priority transfer candidate because AMD short is a recurring promoted Market Impulse edge and a clean nightly smoke-test complement to QQQ.

## Rules
- Entry uses the existing Market Impulse cross-and-reclaim logic.
- Longs require bullish higher-timeframe VWMA regime plus reclaim above VMA.
- Shorts require bearish higher-timeframe VWMA regime plus reclaim below VMA.
- Entry timing stays within the strategy-defined post-open impulse window.
- Evaluate only the stock Market Impulse declared search surface.
- Focus on short-side configurations as indicated by prior evidence.

## Notes
- Campaign: expand30 wave 2 bounded packet.
- Strategy family: Market Impulse.
- Prior strongest configs cluster around AMD short with ema3/ema5 + 90 trend windows and vwma 3,8,21 or 5,13,21.
- Objective: validate single-ticker M1 path through nightly research_lab.
- This is a bounded M1-only discovery run, no retune or later-stage continuation.

## Agent Report
### Run
`2026-04-24T083939` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.3094  signals=518  windows=5
- M2: 4 candidates promoted
- M3: 60 detail rows
- M4: 4 promoted
- M5: 16 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-amd-mi-01/2026-04-24T083939`
