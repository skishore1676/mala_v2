# Hypothesis: Expand30 W1-B4-P3 TLT Elastic Band

## Config
- id: `expand30-w1-b4-p3-tlt-elastic-band`
- state: `retune`
- decision: `retune`
- symbol_scope: `TLT`
- strategy: `Elastic Band Reversion`
- max_stage: `M1`
- last_run: `2026-04-22T14:11:21+0000`

## Thesis
TLT may offer config-only Elastic Band mean-reversion opportunities when intraday price stretches far from 4-hour VPOC and short-term kinematics show exhaustion, making rates-sensitive ETF overextensions a plausible transfer test from the existing basket.

## Rules
- Entry: take Elastic Band signals only when price stretch versus 4-hour VPOC clears the strategy z-score threshold.
- Direction: long below value on downside stretch, short above value on upside stretch.
- Filters: let the declared Elastic Band search surface vary z-score threshold, z-score window, directional-mass gating, jerk confirmation, and kinematic lookback.
- Exit: use standard mala_v2 M1 evaluation only. No continuation beyond M1 in this packet.

## Notes
- Packet: `W1-B4-P3`
- Strategy family request: `Elastic Band`
- mala_v2 mapping: `Elastic Band Reversion`
- Feasibility tag: `config-only`
- Objective: test macro stretch/reversion transfer on TLT only through M1.

## Agent Report
### Run
`2026-04-22T090744` — strategy: `Elastic Band Reversion`

### Stages Executed
`M2`

### Notes
- M2: 0 candidates promoted

### Decision
`retune`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b4-p3-tlt-elastic-band/2026-04-22T090744`
