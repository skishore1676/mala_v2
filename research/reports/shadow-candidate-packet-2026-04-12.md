# Shadow Candidate Packet: Post-Guardrail Options Candidates

Generated: `2026-04-12`

Purpose: provide a human-reviewed handoff from mala_v2 research artifacts to the
Strategy_Catalog control surface. This report is the audit trail. The Google
Sheet remains the operational source of truth for Bhiksha.

Source evidence:

- Market Impulse guardrail run: `data/results/hypothesis_runs/market-impulse-all-basket-discovery/2026-04-12T201909`
- Jerk-Pivot guardrail run: `data/results/hypothesis_runs/jerk-pivot-current-basket-discovery/2026-04-12T203208`
- Comparison report: `research/reports/top-family-guardrail-comparison.md`

Guardrails in force:

- `entry_delay_bars=1`
- `min_hold_bars=2`
- `exit_evaluation_start_bar=1`
- `cooldown_bars_after_signal=5`

## Sheet Update Summary

| Catalog key | Recommended override | Why |
| --- | --- | --- |
| `jerk-pivot-current-basket-discovery__tsla_short` | `shadow` | strongest post-guardrail options candidate; exit sample is near usable but still thin |
| `market-impulse-all-basket-discovery__amd_short` | `shadow` | strongest Market Impulse survivor; good M5 sample and usable exit sample |
| `market-impulse-all-basket-discovery__iwm_long` | `watch_only` | high M5 probability but new candidate with thin holdout and exit samples |
| `jerk-pivot-current-basket-discovery__amd_short` | `watch_only` | improved after guardrails, but still shadow-tier and exit sample is thin |
| `market-impulse-all-basket-discovery__qqq_short` | `watch_only` | demoted sharply by guardrails; previous QQQ edge likely depended on rapid-fire behavior |

Do not set any of these to `active` yet. Use `active` only after Bhiksha shadow
observations confirm option fills, spreads, premium stop behavior, and timing.

## Candidate 1: TSLA Short, Jerk-Pivot

Recommended sheet status:

- `lifecycle_status`: keep as `candidate`
- `operator_status_override`: `shadow`
- `bionic_ready`: `false`

Catalog key:

`jerk-pivot-current-basket-discovery__tsla_short`

Research identity:

- Symbol: `TSLA`
- Direction: `short`
- Strategy: `Jerk-Pivot Momentum (tight)`
- Execution profile: `single_option`
- Params:
  - `jerk_lookback=10`
  - `kinematic_periods_back=1`
  - `use_volume_filter=true`
  - `volume_multiplier=1.3`
  - `vpoc_proximity_pct=0.0015`

Evidence:

- `mc_prob_positive_exp=0.9985`
- `mc_exp_r_p50=0.648349`
- `base_exp_r=0.7867`
- `holdout_trades=45`
- `holdout_win_rate=0.6222`
- Selected exit: `fixed_rr_underlying:0.0050x1.75`
- Exit optimization trades: `38`
- Exit expectancy: `+1.0611`
- Exit profit factor: `2.2105`

Operator notes to paste:

```text
Post-guardrail top options candidate. TSLA short Jerk-Pivot remained robust after entry_delay=1, min_hold=2, cooldown=5. single_option mc_prob=0.9985, mc_p50=+0.6483, holdout_trades=45. Exit policy fixed_rr_underlying 0.0050x1.75, exit_trade_count=38, PF=2.21. Shadow first; do not activate until Bhiksha confirms option fills/slippage/premium stops for 40-60 shadow observations.
```

Promotion condition:

- At least `40-60` Bhiksha shadow observations.
- Option spreads/fills remain acceptable.
- Premium stop and fixed-RR behavior are executable without repeated manual overrides.
- No large degradation versus expected direction/timing.

## Candidate 2: AMD Short, Market Impulse

Recommended sheet status:

- `lifecycle_status`: keep as `candidate`
- `operator_status_override`: `shadow`
- `bionic_ready`: `false`

Catalog key:

`market-impulse-all-basket-discovery__amd_short`

Research identity:

- Symbol: `AMD`
- Direction: `short`
- Strategy: `Market Impulse (Cross & Reclaim)`
- Execution profile: `single_option`
- Params:
  - `entry_buffer_minutes=5`
  - `entry_window_minutes=90`
  - `regime_timeframe=1h`
  - `vwma_periods=5,13,21`

Evidence:

- `mc_prob_positive_exp=0.99825`
- `mc_exp_r_p50=0.408227`
- `base_exp_r=0.5564`
- `holdout_trades=110`
- `holdout_win_rate=0.5455`
- Selected exit: `fixed_rr_underlying:0.0075x2.00`
- Exit optimization trades: `48`
- Exit expectancy: `+0.2258`
- Exit profit factor: `1.2489`

Operator notes to paste:

```text
Post-guardrail strongest Market Impulse survivor. AMD short 1h-regime config remained robust after QQQ/30m variants were demoted. single_option mc_prob=0.99825, mc_p50=+0.4082, holdout_trades=110. Exit policy fixed_rr_underlying 0.0075x2.00, exit_trade_count=48. Shadow first; strongest MI candidate after 1-minute guardrails.
```

Promotion condition:

- At least `40-60` Bhiksha shadow observations.
- AMD option liquidity/fills remain stable during the entry window.
- Fixed-RR exit can be mapped cleanly to option premium handling.
- No evidence that the 1h regime condition is stale or lagging live behavior.

## Watch Only: IWM Long, Market Impulse

Recommended sheet status:

- `lifecycle_status`: keep as `candidate`
- `operator_status_override`: `watch_only`
- `bionic_ready`: `false`

Catalog key:

`market-impulse-all-basket-discovery__iwm_long`

Research identity:

- Symbol: `IWM`
- Direction: `long`
- Strategy: `Market Impulse (Cross & Reclaim)`
- Execution profile: `single_option`
- Params:
  - `entry_buffer_minutes=5`
  - `entry_window_minutes=45`
  - `regime_timeframe=15m`
  - `vwma_periods=5,13,21`

Evidence:

- `mc_prob_positive_exp=0.97825`
- `mc_exp_r_p50=0.429951`
- `base_exp_r=0.5731`
- `holdout_trades=49`
- `holdout_win_rate=0.5510`
- Selected exit: `fixed_rr_underlying:0.0050x2.00`
- Exit optimization trades: `24`
- Exit expectancy: `+0.5584`
- Exit profit factor: `2.0033`

Operator notes to paste:

```text
New post-guardrail Market Impulse candidate. IWM long did not exist in the pre-guardrail baseline. single_option mc_prob=0.97825 and mc_p50=+0.4300, but holdout_trades=49 and exit_trade_count=24 are thin. Keep watch_only until more shadow/holdout evidence accumulates; do not activate from current sample.
```

Promotion condition:

- Reconfirm in a later rerun or with at least `40` exit-quality observations.
- Option liquidity must support clean long-call entry/exit in the 45-minute window.
- No contradictory regime concentration appears in new data.

## Watch Only: AMD Short, Jerk-Pivot

Recommended sheet status:

- `lifecycle_status`: keep as `candidate`
- `operator_status_override`: `watch_only`
- `bionic_ready`: `false`

Catalog key:

`jerk-pivot-current-basket-discovery__amd_short`

Research identity:

- Symbol: `AMD`
- Direction: `short`
- Strategy: `Jerk-Pivot Momentum (tight)`
- Execution profile: `single_option`
- Params:
  - `jerk_lookback=12`
  - `kinematic_periods_back=3`
  - `use_volume_filter=true`
  - `volume_multiplier=1.0`
  - `vpoc_proximity_pct=0.0015`

Evidence:

- `mc_prob_positive_exp=0.82025`
- `mc_exp_r_p50=0.227348`
- `base_exp_r=0.3771`
- `holdout_trades=35`
- `holdout_win_rate=0.4857`
- Selected exit: `fixed_rr_underlying:0.0025x1.25`
- Exit optimization trades: `34`
- Exit expectancy: `+0.1010`
- Exit profit factor: `1.4050`

Operator notes to paste:

```text
Improved after 1-minute guardrails, likely because cooldown removed noisy duplicate entries. Still only shadow-tier: single_option mc_prob=0.82025, holdout_trades=35, exit_trade_count=34. Keep watch_only; revisit after more data or after TSLA/AMD primary shadows are stable.
```

Promotion condition:

- Needs a fresh rerun or accumulated shadow data showing mc_prob-equivalent behavior above `0.90`.
- Exit trade count should exceed `40`.
- Must add diversification beyond AMD Market Impulse rather than duplicate the same AMD short exposure.

## Watch Only / Paused: QQQ Short, Market Impulse

Recommended sheet status:

- `lifecycle_status`: keep as `candidate` if already present
- `operator_status_override`: `watch_only` or `paused`
- `bionic_ready`: `false`

Catalog key:

`market-impulse-all-basket-discovery__qqq_short`

Research identity:

- Symbol: `QQQ`
- Direction: `short`
- Strategy: `Market Impulse (Cross & Reclaim)`
- Execution profile: `single_option`
- Params:
  - `entry_buffer_minutes=3`
  - `entry_window_minutes=60`
  - `regime_timeframe=1h`
  - `vwma_periods=10,20,40`

Evidence:

- `mc_prob_positive_exp=0.82875`
- `mc_exp_r_p50=0.186666`
- `base_exp_r=0.3351`
- `holdout_trades=53`
- `holdout_win_rate=0.4717`
- Selected exit: `fixed_rr_underlying:0.0075x2.00`
- Exit optimization trades: `17`
- Exit expectancy: `+1.1034`
- Exit profit factor: `1.5539`

Operator notes to paste:

```text
Demoted by 1-minute guardrails. Pre-guardrail QQQ short Market Impulse looked strong, but after entry_delay=1/min_hold=2/cooldown=5 it fell to single_option mc_prob=0.82875 and only one config survived as shadow-tier. Exit opt has only 17 trades and is not reliable. Keep watch_only/paused; do not activate without a new M1 retune and fresh M4/M5 pass.
```

Promotion condition:

- Requires a separate follow-up hypothesis, not mutation of this completed run.
- Must pass M4/M5 again under guardrails.
- Exit optimization needs a materially larger sample.

## Do Not Promote From This Packet

Do not promote these current selected rows:

- `market-impulse-all-basket-discovery__aapl_short`: shadow-tier in CSV but exit sample is thin and this was absent in baseline.
- `market-impulse-all-basket-discovery__meta_short`: below catalog floor after guardrails.
- `market-impulse-all-basket-discovery__pltr_short`: weak/negative M5.
- `market-impulse-all-basket-discovery__spy_long`: below catalog floor after guardrails.
- `market-impulse-all-basket-discovery__tsla_short`: TSLA is better covered by Jerk-Pivot.
- `jerk-pivot-current-basket-discovery__nvda_short`: weak M5.

## Operational Note

Bhiksha should treat `operator_status_override` as the human control. The
workbench can write research candidates, but it should not auto-activate live
execution. `active` should require human review after shadow evidence.
