# Hypothesis: Expand30 W1-B1-P2 MU Market Impulse

## Config
- id: `expand30-w1-b1-p2-mu-market-impulse`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-24T13:41:59+0000`

## Thesis
MU may exhibit transferable opening impulse continuation when early VWMA regime alignment and a same-session VMA reclaim appear together. The test is whether the existing Market Impulse search surface can find stable out-of-sample signal quality on this higher-beta semiconductor name without any code changes.

## Rules
- Entry only on Market Impulse cross-and-reclaim signals.
- Longs require bullish higher-timeframe impulse regime alignment.
- Shorts require bearish higher-timeframe impulse regime alignment.
- Entry timing is limited to the strategy-defined opening buffer and entry window.
- Evaluate only the declared Market Impulse search surface.

## Notes
- Campaign: Expand30 wave 1 packet W1-B1-P2.
- Scope is dry-run plus M1 only.
- No retune or later-stage continuation is authorized in this packet.

## Agent Report
### Run
`2026-04-24T084126` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M3 → M4 → M5`

### Notes
- M3: 60 detail rows
- M4: 4 promoted
- M5: 16 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b1-p2-mu-market-impulse/2026-04-24T084126`
