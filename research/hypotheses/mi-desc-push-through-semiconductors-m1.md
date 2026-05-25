# Hypothesis: MI Push Through Semiconductors M1

## Config
- id: `mi-desc-push-through-semiconductors-m1`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU, AMD, SMH, SOXX`
- strategy: `MI Push Through`
- max_stage: `M5`
- max_configs: `64`
- last_run: `2026-05-17T12:21:27+0000`

## Thesis
Market Impulse push-through confirmation in semiconductor names can reduce
false reclaims by requiring continuation after the initial impulse.

## Rules
- Use the declared MI Push Through search surface.
- Keep both long and short directions in the sweep.
- Re-optimize exits for short-term option handling.

## Notes
- Reconstructed from prior M5 artifacts so this candidate can participate in
  the full Mala evidence rework sweep.

## Agent Report
### Run
`2026-05-17T071917` — strategy: `MI Push Through`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.5001  signals=106  windows=5
- M2: 12 candidates promoted
- M3: 176 detail rows
- M4: 9 promoted
- M5: 36 execution mappings
- exit_opt: 3 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/research/results/hypothesis_runs/mi-desc-push-through-semiconductors-m1/2026-05-17T071917`
