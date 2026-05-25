# Hypothesis: MI High Close Reclaim Semiconductors M1

## Config
- id: `mi-desc-high-close-semiconductors-m1`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU, AMD, SMH, SOXX`
- strategy: `MI High Close Reclaim`
- max_stage: `M5`
- max_configs: `64`
- last_run: `2026-05-17T12:19:17+0000`

## Thesis
Market Impulse high-close reclaim behavior in liquid semiconductor names can
separate real continuation from noisy first-hour whipsaw.

## Rules
- Use the declared MI High Close Reclaim search surface.
- Keep both long and short directions in the sweep.
- Re-optimize exits for short-term option handling.

## Notes
- Reconstructed from prior M5 artifacts so this candidate can participate in
  the full Mala evidence rework sweep.

## Agent Report
### Run
`2026-05-17T071804` — strategy: `MI High Close Reclaim`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.3732  signals=127  windows=5
- M2: 8 candidates promoted
- M3: 102 detail rows
- M4: 3 promoted
- M5: 12 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/research/results/hypothesis_runs/mi-desc-high-close-semiconductors-m1/2026-05-17T071804`
