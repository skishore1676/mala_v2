# Hypothesis: Expand30 W1-B2-P1 DIA Market Impulse

## Config
- id: `expand30-w1-b2-p1-dia-market-impulse`
- state: `retune`
- decision: `retune`
- symbol_scope: `DIA`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-22T15:43:33+0000`

## Thesis
DIA is a liquid index ETF candidate for testing whether the existing Market Impulse cross-and-reclaim behavior transfers to broader index structure. The honest first pass is a bounded M1-only discovery run on the existing strategy surface.

## Rules
- Entry uses the existing Market Impulse cross-and-reclaim logic.
- Longs require bullish higher-timeframe impulse regime alignment.
- Shorts require bearish higher-timeframe impulse regime alignment.
- Entry timing stays within the strategy-defined opening buffer and entry window.
- Evaluate only the declared Market Impulse search surface.

## Notes
- Campaign: Expand30 wave 1 packet W1-B2-P1.
- Strategy family requested: Market Impulse.
- Scope is dry-run plus M1 only.
- No retune and no M2+ continuation in this packet.

## Agent Report
### Run
`2026-04-22T104311` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M2`

### Notes
- M2: 0 candidates promoted

### Decision
`retune`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b2-p1-dia-market-impulse/2026-04-22T104311`
