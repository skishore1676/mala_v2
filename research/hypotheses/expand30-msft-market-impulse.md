# Hypothesis: Expand30 MSFT Market Impulse

## Config
- id: `expand30-msft-market-impulse`
- state: `kill`
- decision: `kill`
- symbol_scope: `MSFT`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-24T18:30:55+0000`

## Thesis
MSFT is a liquid mega-cap candidate for transferring the existing Market Impulse cross-and-reclaim edge into a single-name open impulse test. The honest first pass is a bounded M1-only discovery run on the stock strategy surface, without retune or later-stage continuation.

## Rules
- Entry uses the existing Market Impulse cross-and-reclaim logic.
- Longs require bullish higher-timeframe VWMA regime plus reclaim above VMA.
- Shorts require bearish higher-timeframe VWMA regime plus reclaim below VMA.
- Entry timing stays within the strategy-defined post-open impulse window.
- Evaluate only the stock Market Impulse declared search surface.

## Notes
- Campaign: expand30 wave 1 packet W1-B1-P1.
- Strategy family requested: Market Impulse.
- Scope is dry-run plus M1 only.
- No retune and no M2+ continuation in this packet.

## Agent Report
### Run
`2026-04-24T133046` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M1`

### Notes
- M1 FAIL: no positive configs found

### Decision
`kill`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-msft-market-impulse/2026-04-24T133046`
