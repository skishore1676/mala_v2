# Hypothesis: Expand30 W1-B1-P2 AVGO Market Impulse

## Config
- id: `expand30-w1-b1-p2-avgo-market-impulse`
- state: `kill`
- decision: `kill`
- symbol_scope: `AVGO`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-24T13:41:25+0000`

## Thesis
AVGO may carry the same opening cross-and-reclaim continuation behavior already seen in liquid impulse names. If the early VWMA stack aligns and price reclaims the short-term VMA inside the opening window, directional continuation may persist long enough to clear the M1 walk-forward gate.

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
`2026-04-24T084105` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M3 → M4`

### Notes
- M3: 15 detail rows
- M4: 0 promoted

### Decision
`kill`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b1-p2-avgo-market-impulse/2026-04-24T084105`
