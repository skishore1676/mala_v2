# Hypothesis: MI Second Touch Semiconductors M1

## Config
- id: `mi-desc-second-touch-semiconductors-m1`
- state: `kill`
- decision: `kill`
- symbol_scope: `MU, AMD, SMH, SOXX`
- strategy: `MI Second Touch`
- max_stage: `M5`
- max_configs: `64`
- last_run: `2026-05-17T12:23:27+0000`

## Thesis
Market Impulse second-touch behavior in liquid semiconductor names can produce
short-side follow-through after a delayed reclaim failure near the open.

## Rules
- Use the declared MI Second Touch search surface.
- Keep both long and short directions in the sweep, but require candidates to
  re-earn shadow status through M5 before they remain evidence.
- Re-optimize exits for short-term option handling.

## Notes
- Reconstructed from prior M5 artifacts so this candidate can participate in
  the full Mala evidence rework sweep.

## Agent Report
### Run
`2026-05-17T072127` — strategy: `MI Second Touch`

### Stages Executed
`M1 → M2 → M3 → M4`

### Notes
- M1 PASS: pct_pos=100%  exp_r=+0.3398  signals=121  windows=5
- M2: 10 candidates promoted
- M3: 94 detail rows
- M4: 0 promoted

### Decision
`kill`

### Artifacts
`/Users/suman/code/mala_v2/data/results/hypothesis_runs/mi-desc-second-touch-semiconductors-m1/2026-05-17T072127`
