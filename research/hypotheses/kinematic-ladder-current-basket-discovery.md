# Hypothesis: Kinematic Ladder Current Basket Discovery

## Config
- id: `kinematic-ladder-current-basket-discovery`
- state: `retune`
- decision: `retune`
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Kinematic Ladder`
- max_stage: `M5`
- max_configs: `48`
- last_run: `2026-04-12T23:37:29+0000`

## Thesis
Intraday continuation is strongest when higher-level velocity and acceleration establish regime bias and lower-level velocity, acceleration, and jerk confirm the trigger. The ladder should find symbol/direction pockets where momentum is persistent rather than noisy.

## Rules
- Entry: regime velocity/acceleration aligns with lower-timeframe kinematic trigger.
- Direction: long and short are both evaluated by the strategy signal direction.
- Filters: use the strategy search surface for regime window, acceleration window, kinematic lookback, volume filter, and volume multiplier.
- Exit: use MFE/MAE reward-risk gates through M1-M5, then evaluate per-candidate thesis exits after M5.

## Notes
- Feasibility tag: `config-only`
- This is the cleanest pure-kinematics replay and should be compared against Jerk-Pivot and Market Impulse.
- Market regime columns are observational evidence, not a gate.
- Use `CATALOG_SELECTED.csv` as the concise selected-candidate readout after M5.

## Agent Report
### Run
`2026-04-12T183516` — strategy: `Kinematic Ladder`

### Stages Executed
`M1`

### Notes
- M1 FAIL: signals=36<50; windows=2<3

### Decision
`retune`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/kinematic-ladder-current-basket-discovery/2026-04-12T183516`
