# Hypothesis: Expand30 W1-B4-P3 SOXX Elastic Band

## Config
- id: `expand30-w1-b4-p3-soxx-elastic-band`
- state: `kill`
- decision: `kill`
- symbol_scope: `SOXX`
- strategy: `Elastic Band Reversion`
- max_stage: `M1`
- last_run: `2026-04-24T13:47:43+0000`

## Thesis
SOXX may offer config-only Elastic Band mean-reversion opportunities when intraday semiconductor ETF moves stretch far from 4-hour VPOC and short-term kinematics show exhaustion, making sector overextensions a plausible transfer test from the existing basket.

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
- Objective: test semiconductor ETF stretch/reversion transfer on SOXX only through M1.

## Agent Report
### Run
`2026-04-24T084659` — strategy: `Elastic Band Reversion`

### Stages Executed
`M2 → M3 → M4`

### Notes
- M2: 1 candidates promoted
- M3: 8 detail rows
- M4: 0 promoted

### Decision
`kill`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b4-p3-soxx-elastic-band/2026-04-24T084659`
