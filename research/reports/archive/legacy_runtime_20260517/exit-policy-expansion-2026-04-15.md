# Exit Policy Expansion Experiment - 2026-04-15

## Scope

Added generic underlying-anchored thesis exits to the M5 exit optimizer and reran current winner families with the expanded grid.

New exit families:

- `hold_to_eod_underlying`
- `time_stop_underlying` at 11:30, 14:30, and 15:55 ET
- `atr_trailing_underlying` using 14/30-bar ATR variants
- `ma_trailing_underlying` using EMA 8/12/20/50 variants
- `ma_crossover_underlying` using EMA 8/20 and 12/50 variants
- Wider fixed R:R runner grids for momentum/breakout families

## Runs

| Strategy | Old run | New run | Notes |
| --- | --- | --- | --- |
| Market Impulse | `2026-04-12T201909` | `2026-04-15T225335` | Forced full M1-M5 rerun with expanded exits. |
| Jerk Pivot | `2026-04-12T203208` | `2026-04-15T225844` | Forced full M1-M5 rerun with expanded exits. |

## Market Impulse Result

| Candidate | Previous selected exit | Previous expectancy | New selected exit | New expectancy | Verdict |
| --- | ---: | ---: | --- | ---: | --- |
| AAPL short | `fixed_rr_underlying:0.0075x2.00` | 0.3883 | `time_stop_underlying:1555` | 0.7988 | Better, but still shadow tier. |
| AMD short | `fixed_rr_underlying:0.0075x2.00` | 0.2258 | `time_stop_underlying:1430` | 0.9132 | Material improvement. |
| IWM long | `fixed_rr_underlying:0.0050x2.00` | 0.5584 | `fixed_rr_underlying:0.0050x2.00` | 0.5584 | No change. |
| QQQ short | `fixed_rr_underlying:0.0075x2.00` | 1.1034 | `hold_to_eod_underlying` | 1.8455 | Material improvement. |

Interpretation: for Market Impulse, the useful discovery is not ATR/MA specifically. The edge improves when the exit optimizer allows the trade to breathe through wider/time-based exits instead of forcing the old 0.75%/2R cap.

## Jerk Pivot Result

| Candidate | Previous selected exit | Previous expectancy | New selected exit | New expectancy | Verdict |
| --- | ---: | ---: | --- | ---: | --- |
| AMD short | `fixed_rr_underlying:0.0025x1.25` | 0.1010 | `ma_crossover_underlying:ema_12_exit>ema_50_exit` | 0.1041 | Marginal improvement; treat as fragile. |
| TSLA short | `fixed_rr_underlying:0.0050x1.75` | 1.0611 | `fixed_rr_underlying:0.0100x3.00` | 2.1591 | Material improvement. |

Interpretation: TSLA Jerk Pivot clearly benefits from a runner-style exit. AMD's MA crossover win is too small to trust as a meaningful model improvement; it should not drive production behavior without another validation pass.

## Operational Notes

- The current hypothesis runner has no exit-only rerun mode, so these experiments reran full M1-M5 discovery.
- Because catalog credentials were available in the environment, the reruns also upserted Google Sheet Strategy_Catalog rows for passing catalog candidates.
- Before using these in Bhiksha, Bhiksha must understand the new `thesis_exit_policy` values and their params, especially `time_stop_underlying`, `hold_to_eod_underlying`, `atr_trailing_underlying`, `ma_trailing_underlying`, and `ma_crossover_underlying`.

## Recommendation

Keep the expanded exit grid, but use it conservatively:

- Accept runner/time-based improvements for Market Impulse AMD short, Market Impulse QQQ short, and Jerk Pivot TSLA short.
- Treat MA/ATR exits as candidate research tools for now, not default production exits.
- Add an exit-only optimizer command next so exit experiments can be run against existing M5 artifacts without rerunning discovery or touching the catalog unless explicitly requested.
