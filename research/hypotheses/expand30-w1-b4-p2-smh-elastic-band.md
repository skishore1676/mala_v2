# Hypothesis: Expand30 W1-B4-P2 SMH Elastic Band

## Config
- id: `expand30-w1-b4-p2-smh-elastic-band`
- state: `completed`
- decision: `promote`
- symbol_scope: `SMH`
- strategy: `Elastic Band Reversion`
- max_stage: `M5`
- last_run: `2026-05-17T12:09:31+0000`

## Thesis
SMH may inherit a usable ETF-level mean-reversion profile when stretched far from 4-hour VPOC, providing a sector-level transfer test for Elastic Band in semiconductors.

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
`2026-05-17T070909` — strategy: `Elastic Band Reversion`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.0791  signals=1763  windows=5
- M2: 4 candidates promoted
- M3: 52 detail rows
- M4: 1 promoted
- M5: 4 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/data/results/hypothesis_runs/expand30-w1-b4-p2-smh-elastic-band/2026-05-17T070909`
