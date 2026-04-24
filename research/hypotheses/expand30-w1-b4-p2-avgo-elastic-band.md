# Hypothesis: Expand30 W1-B4-P2 AVGO Elastic Band

## Config
- id: `expand30-w1-b4-p2-avgo-elastic-band`
- state: `completed`
- decision: `promote`
- symbol_scope: `AVGO`
- strategy: `Elastic Band Reversion`
- max_stage: `M1`
- last_run: `2026-04-22T13:56:56+0000`

## Thesis
AVGO may show repeatable intraday snap-back behavior after statistically large stretches away from 4-hour VPOC, making it a plausible semiconductor transfer target for the Elastic Band reversion family.

## Rules
- Entry: take Elastic Band reversion signals after price stretches far enough from 4-hour VPOC.
- Direction: long below value, short above value, using the strategy signal direction.
- Filters: let the declared Elastic Band search surface vary z-score threshold, z-score window, kinematic lookback, directional mass, and jerk confirmation.
- Exit: standard mala_v2 staged evaluation, but this packet stops at M1 only.

## Notes
- Feasibility tag: `config-only`
- Campaign packet: `W1-B4-P2`
- Strategy-family transfer prior: Elastic Band has prior evidence in the current basket, strongest on NVDA short stock_like only; this is a bounded transfer test, not proof.

## Agent Report
### Run
`2026-04-22T085304` — strategy: `Elastic Band Reversion`

### Stages Executed
`M2 → M3 → M4 → M5`

### Notes
- M2: 4 candidates promoted
- M3: 59 detail rows
- M4: 2 promoted
- M5: 8 execution mappings

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b4-p2-avgo-elastic-band/2026-04-22T085304`
