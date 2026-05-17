# Hypothesis: Elastic Band NVDA Long Catalog Replay

## Config
- id: `elastic-band-nvda-long-catalog-replay`
- state: `completed`
- decision: `promote`
- symbol_scope: `NVDA`
- strategy: `Elastic Band z=3.0/w=120+dm`
- max_stage: `M5`
- search_mode: `fixed`
- direction_scope: `long`
- max_configs: `1`
- last_run: `2026-04-12T13:54:54+0000`

## Thesis
Replay the v1 catalog row for NVDA long exactly enough to verify whether mala_v2 can reproduce the prior Elastic Band survivor before trusting broad discovery results.

## Rules
- Entry: fixed Elastic Band Reversion config from the catalog row.
- Direction: long only.
- Exit: standard M1-M5 workbench gates, followed by Strategy_Catalog write only if M5 promotes.

## Notes
- v1 catalog lead: `NVDA long`, `Elastic Band z=3.0/w=120+dm`.
- Treat this as a replay/regression hypothesis, not a broad search.

## Agent Report
### Run
`2026-04-12T085437` — strategy: `Elastic Band z=3.0/w=120+dm`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.0765  signals=229  windows=5
- M2: 1 candidates promoted
- M3: 15 detail rows
- M4: 1 promoted
- M5: 4 execution mappings
- exit_opt: fixed_rr_underlying:0.0035x1.00

### Decision
`promote`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/elastic-band-nvda-long-catalog-replay/2026-04-12T085437`
