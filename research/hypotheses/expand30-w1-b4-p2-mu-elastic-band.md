# Hypothesis: Expand30 W1-B4-P2 MU Elastic Band

## Config
- id: `expand30-w1-b4-p2-mu-elastic-band`
- state: `retune`
- decision: `retune`
- symbol_scope: `MU`
- strategy: `Elastic Band Reversion`
- max_stage: `M1`
- last_run: `2026-04-24T13:44:14+0000`

## Thesis
MU may mean-revert after intraday overextensions away from 4-hour VPOC when short-term kinematics show exhaustion, making it a plausible high-beta semiconductor transfer target for Elastic Band.

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
`2026-04-24T084315` — strategy: `Elastic Band Reversion`

### Stages Executed
`M2`

### Notes
- M2: 0 candidates promoted

### Decision
`retune`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b4-p2-mu-elastic-band/2026-04-24T084315`
