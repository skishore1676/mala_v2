# Hypothesis: SPY/IWM Elastic Band overextension flash-drop risk

## Config
- id: `spy-iwm-elastic-band-overextension-flash-drop-20260511`
- state: `completed`
- decision: `promote`
- symbol_scope: `SPY,IWM`
- strategy: `Elastic Band Reversion`
- max_stage: `M5`
- direction_scope: `short`
- last_run: `2026-05-17T12:28:29+0000`

## Thesis
Test whether SPY/IWM intraday upside overextension relative to VPOC, confirmed by kinematic exhaustion, produces elevated downside excursion risk. Same-day 30/60/EOD short-side forward MFE/MAE is config-only; next-session / 2D / 3D downside labels are in scope for the research question but blocked unless the current runner exposes safe session-aware multi-day labels.

## Rules
- Use Elastic Band Reversion declared search surface; inspect short-side M1 evidence first.
- Treat forward 30/60/EOD MFE/MAE as same-day flash-drop-risk proxies only; do not claim next-day / 2D / 3D evidence unless session-aware multi-day labels are implemented and present in artifacts.

## Notes
- Feasibility: config-only, medium confidence per Research Ops design artifact 2026-05-11.
- Human-approved by Suman via Jarvis: OK, let's go ahead; bounded to local M1 only.
- Scope update 2026-05-11 19:12 CDT: do not limit the research question to same-day/EOD. Current artifact interpretation must separately mark next-session / multi-day flash-drop labels as BLOCKED/new-feature if the runner cannot produce them safely.

## Agent Report
### Run
`2026-05-17T072736` — strategy: `Elastic Band Reversion`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.1860  signals=339  windows=5
- M2: 1 candidates promoted
- M3: 15 detail rows
- M4: 1 promoted
- M5: 4 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/research/results/hypothesis_runs/spy-iwm-elastic-band-overextension-flash-drop-20260511/2026-05-17T072736`
