# Hypothesis: MI Second Touch — semis M1 pilot

## Config
- id: `mi-second-touch-semis-m1-20260501`
- state: `retune`
- decision: `retune`
- symbol_scope: `MU, AMD, SMH, SOXX`
- strategy: `MI Second Touch`
- max_stage: `M1`
- last_run: `2026-05-02T02:34:10+0000`

## Thesis
Delayed VMA reclaim in an existing impulse regime should distinguish real repair from immediate noisy pierce/reclaim behavior.

## Rules
- Run M1 only across the compact semiconductor universe; do not continue to M2 until reviewed.
- Avoid dormant ATR excursion assumptions; use the declared strategy search surface.

## Notes
- Overnight pilot created by Jarvis on 2026-05-01; ticker-agnostic descendant test.
- Review M1_top.csv, M1_aggregate.csv, and RUN_SUMMARY before promotion.

## Agent Report
### Run
`2026-05-01T213032` — strategy: `MI Second Touch`

### Stages Executed
`M1`

### Notes
- M1 FAIL: signals=34<50; windows=2<3

### Decision
`retune`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/mi-second-touch-semis-m1-20260501/2026-05-01T213032`
