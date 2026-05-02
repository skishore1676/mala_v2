# Hypothesis: Expand30 W1-B3-P2 DIA Opening Drive

## Config
- id: `expand30-w1-b3-p2-dia-opening-drive`
- state: `kill`
- decision: `kill`
- symbol_scope: `DIA`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: `2026-04-25T18:55:11+0000`

## Thesis
DIA may inherit an opening-drive continuation edge when broad-index auction pressure sets a clear early directional move that continues instead of reverting quickly.

## Rules
- Entry: use `Opening Drive Classifier` discovery over the standard opening-window and entry-window surface.
- Direction: allow both long and short continuation/failure paths; let the strategy search surface determine survivors.
- Filters: evaluate breakout buffer, drive threshold, kinematic periods, volume, directional mass, jerk confirmation, and optional 5-minute regime alignment from the existing strategy surface.
- Exit: use the standard mala_v2 M1 evaluation only.

## Notes
- Packet: `W1-B3-P2`
- Strategy family transfer prior: `Opening Drive`
- Feasibility tag: `config-only`
- Bounded to dry-run then `--max-stage M1` only.

## Agent Report
- decision: `kill`
- updated_at: `2026-04-25T18:55:11+0000`
- source: `research_runner kill-approved`
- reason: operator approved kill from Research_Control
