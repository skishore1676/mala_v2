# Hypothesis: SPY Opening Drive M1 Smoke Test

## Config
- id: `spy-opening-drive-m1-smoke`
- state: `retune`
- decision: `retune`
- symbol_scope: `SPY`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-12T12:15:10+0000`

## Thesis
SPY tends to continue its opening drive direction after the first 15-20 minutes
when the 5-minute regime agrees. Testing a fast M1 pass to confirm the data
pipeline and gate machinery work end-to-end.

## Rules
- Entry: after a clear 15-minute opening drive (positive or negative)
- Filter: 5-minute regime must be directionally aligned
- Max stage: M1 only (smoke test)

## Notes
- Feasibility tag: `config-only`
- Purpose: verify the mala_v2 workbench runs clean before deeper research

## Agent Report
### Run
`2026-04-12T071502` — strategy: `Opening Drive Classifier`

### Stages Executed
`M1`

### Notes
- M1 FAIL: signals=15<50; windows=1<3

### Decision
`retune`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/spy-opening-drive-m1-smoke/2026-04-12T071502`
