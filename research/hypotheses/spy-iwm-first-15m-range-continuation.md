# Hypothesis: SPY IWM First 15m Range Continuation

## Config
- id: `spy-iwm-first-15m-range-continuation`
- state: `kill`
- decision: `kill`
- symbol_scope: `SPY,IWM`
- strategy: `Opening Drive Classifier`
- max_stage: `M1`
- last_run: ``

## Thesis
SPY and IWM first-15-minute opening drives that break and continue in the same direction can produce positive M1 expectancy using the existing Opening Drive Classifier continuation path, without requiring EMA-stack or VMA-specific logic.

## Rules
- Opening range is the first 15 minutes after the cash open.
- Trade continuation only, not failure.
- Longs require an upward opening drive and a breakout above the opening high.
- Shorts require a downward opening drive and a breakdown below the opening low.
- Keep existing confirmation filters limited to volume, directional mass, acceleration, and optional jerk confirmation.

## Notes
- Contract source: `/Users/sunny/.openclaw/workspace/agents/research_lab/artifacts/2026-04-21-first-15m-range-m1-spec.md`
- Fixed intent: `opening_window_minutes=15`, `entry_end_offset_minutes=90`, `enable_continue=true`, `enable_fail=false`, `allow_long=true`, `allow_short=true`, `use_volume_filter=true`, `volume_multiplier=1.2`, `use_directional_mass=true`, `kinematic_periods_back=1`, `use_regime_filter=false`, `regime_timeframe=5m`
- Sweep intent: `entry_start_offset_minutes in [20,25]`, `min_drive_return_pct in [0.0015,0.0020]`, `breakout_buffer_pct in [0.0,0.0005]`, `use_jerk_confirmation in [true,false]`
- Similar prior hypothesis `iwm-opening-range-regime-continuation` was killed under regime-filtered IWM-only settings; this run tests a different pooled-symbol, no-regime-filter continuation path.

## Agent Report
### Run
`2026-04-21T074318` — strategy: `Opening Drive Classifier`

### Stages Executed
`M1`

### Notes
- Executed the exact approved 16-config M1 sweep from the research_lab spec, not the broader default 32-config discovery surface.
- M1 FAIL: no configuration cleared standard M1 honestly.
- Closest row was IWM combined at `entry_start_offset_minutes=20`, `min_drive_return_pct=0.0015`, `breakout_buffer_pct=0.0`, `use_jerk_confirmation=false` with `oos_windows=3`, `oos_signals=52`, `pct_positive_oos_windows=33.3%`, `avg_test_exp_r=+0.0207`.
- Best raw expectancy rows were sparse IWM-only pockets with 1 OOS window and 15-17 signals, not stable evidence.

### Decision
`kill`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/spy-iwm-first-15m-range-continuation-custom-m1/2026-04-21T074318`
