# Volume Mismatch Retune Findings

## Data Availability

- Generated at: `2026-05-03T15:59:13.082320+00:00`
- Hypothesis runs dir: `data/results/hypothesis_runs`
- Replay bar data dir: `data`
- Period: `2024-01-02` to `2026-02-28`
- Unique selected M5 candidates replayed: `28`
- Volume-affected candidates: `26`
- Output directory: `data/results/volume_mismatch_retune/20260503T_volume_derivative_m5_v2`

## Candidate Inventory

| family | rows | volume dependency |
|---|---:|---|
| compression_breakout | 2 | explicit_volume_gate, none_explicit |
| elastic_band_reversion | 3 | directional_mass_sign, vpoc_volume_dependence |
| jerk_pivot_momentum | 3 | explicit_volume_gate, vpoc_volume_dependence |
| market_impulse | 11 | volume_inside_vwma_regime |
| opening_drive_classifier | 9 | directional_mass_sign, none_explicit, volume_inside_vwma_regime |

## Provider Risk By Symbol

| symbol | affected rows | 1m risk | 3m risk | 5m risk |
|---|---:|---:|---:|---:|
| AAPL | 2 | medium | medium | medium |
| AMD | 4 | medium | low | low |
| AMZN | 1 | high | high | medium |
| AVGO | 2 | medium | medium | medium |
| IWM | 2 | high | high | high |
| META | 1 | medium | high | medium |
| MU | 1 | medium | medium | medium |
| NVDA | 3 | medium | medium | low |
| PLTR | 1 | medium | medium | medium |
| QQQ | 1 | high | high | high |
| SMH | 3 | medium | medium | medium |
| SPY | 1 | high | high | high |
| TSLA | 4 | medium | medium | medium |

## Replay Results

| family | scenario | rows | trades | avg expectancy | avg PF | trade overlap | trade count ratio |
|---|---|---:|---:|---:|---:|---:|---:|
| compression_breakout | all_volume_relvol_3m | 1 | 533 | 0.184655 | 1.085241 | 0.548327 | 0.990706 |
| compression_breakout | all_volume_relvol_5m | 1 | 523 | 0.23906 | 1.119252 | 0.405204 | 0.972119 |
| compression_breakout | baseline | 2 | 1079 | 0.222034 | 1.168725 | None | None |
| compression_breakout | explicit_gate_off | 1 | 541 | 0.204933 | 1.092578 | 0.607807 | 1.005576 |
| compression_breakout | gate_relative_volume_1m | 1 | 538 | 0.221877 | 1.103646 | 1.0 | 1.0 |
| compression_breakout | gate_relative_volume_3m | 1 | 521 | 0.267687 | 1.136419 | 0.349442 | 0.968401 |
| compression_breakout | gate_relative_volume_5m | 1 | 489 | 0.21138 | 1.116899 | 0.174721 | 0.908922 |
| elastic_band_reversion | all_volume_relvol_3m | 3 | 3931 | 0.025752 | 1.091413 | 0.516724 | 0.999833 |
| elastic_band_reversion | all_volume_relvol_5m | 3 | 3936 | 0.018086 | 1.065706 | 0.515227 | 1.001092 |
| elastic_band_reversion | baseline | 3 | 3930 | 0.018808 | 1.053709 | None | None |
| jerk_pivot_momentum | all_volume_relvol_3m | 3 | 1335 | 0.13478 | 1.129615 | 0.225906 | 0.984555 |
| jerk_pivot_momentum | all_volume_relvol_5m | 3 | 1381 | 0.111504 | 1.112981 | 0.195409 | 1.002035 |
| jerk_pivot_momentum | baseline | 3 | 1339 | 0.193207 | 1.26733 | None | None |
| jerk_pivot_momentum | explicit_gate_off | 3 | 2688 | 0.059148 | 1.129082 | 0.665139 | 2.141925 |
| jerk_pivot_momentum | gate_relative_volume_1m | 3 | 1339 | 0.193207 | 1.26733 | 1.0 | 1.0 |
| jerk_pivot_momentum | gate_relative_volume_3m | 3 | 1354 | 0.120682 | 1.19372 | 0.61059 | 0.976316 |
| jerk_pivot_momentum | gate_relative_volume_5m | 3 | 1354 | 0.075099 | 1.136159 | 0.510575 | 0.973172 |
| market_impulse | all_volume_relvol_3m | 11 | 2059 | 0.041765 | 1.082174 | 0.895782 | 0.996772 |
| market_impulse | all_volume_relvol_5m | 11 | 2060 | 0.037039 | 1.078554 | 0.894668 | 0.997602 |
| market_impulse | baseline | 11 | 2060 | 0.173428 | 1.163753 | None | None |
| opening_drive_classifier | all_volume_relvol_3m | 8 | 1756 | 0.266015 | 1.298737 | 0.90316 | 0.995605 |
| opening_drive_classifier | all_volume_relvol_5m | 8 | 1756 | 0.267736 | 1.300523 | 0.90047 | 0.995605 |
| opening_drive_classifier | baseline | 9 | 2017 | 0.275389 | 1.314794 | None | None |

## Rows That Survive

| catalog_key | symbol | family | variant | decision |
|---|---:|---|---|---|
| compression-breakout-current-basket-discovery__amd_short | AMD | compression_breakout | baseline | baseline_ok |
| compression-breakout-current-basket-discovery__tsla_short | TSLA | compression_breakout | baseline | baseline_ok |
| elastic-band-current-basket-discovery__nvda_short | NVDA | elastic_band_reversion | baseline | baseline_ok |
| expand30-w1-b4-p2-smh-elastic-band__smh_long | SMH | elastic_band_reversion | baseline | baseline_ok |
| jerk-pivot-current-basket-discovery__amd_short | AMD | jerk_pivot_momentum | baseline | baseline_ok |
| jerk-pivot-current-basket-discovery__nvda_short | NVDA | jerk_pivot_momentum | baseline | baseline_ok |
| jerk-pivot-current-basket-discovery__tsla_short | TSLA | jerk_pivot_momentum | baseline | baseline_ok |
| market-impulse-all-basket-discovery__aapl_short | AAPL | market_impulse | baseline | baseline_ok |
| expand30-amd-mi-01__amd_short | AMD | market_impulse | baseline | baseline_ok |
| market-impulse-all-basket-discovery__amd_short | AMD | market_impulse | baseline | baseline_ok |
| market-impulse-all-basket-discovery__iwm_long | IWM | market_impulse | baseline | baseline_ok |
| market-impulse-all-basket-discovery__meta_short | META | market_impulse | baseline | baseline_ok |
| expand30-w1-b1-p2-mu-market-impulse__mu_long | MU | market_impulse | baseline | baseline_ok |
| market-impulse-all-basket-discovery__pltr_short | PLTR | market_impulse | baseline | baseline_ok |
| market-impulse-all-basket-discovery__qqq_short | QQQ | market_impulse | baseline | baseline_ok |
| expand30-w1-b1-p3-market-impulse-smh__smh_short | SMH | market_impulse | baseline | baseline_ok |
| market-impulse-all-basket-discovery__spy_long | SPY | market_impulse | baseline | baseline_ok |
| opening-drive-current-basket-discovery__aapl_short | AAPL | opening_drive_classifier | baseline | baseline_ok |
| opening-drive-current-basket-discovery__amd_short | AMD | opening_drive_classifier | baseline | baseline_ok |
| expand30-w1-b2-p2-amzn-opening-drive__amzn_short | AMZN | opening_drive_classifier | baseline | baseline_ok |
| expand30-w1-b2-p3-avgo-opening-drive__avgo_long | AVGO | opening_drive_classifier | baseline | baseline_ok |
| opening-drive-current-basket-discovery__iwm_short | IWM | opening_drive_classifier | baseline | baseline_ok |
| expand30-w1-b2-p3-mu-opening-drive__mu_long | MU | opening_drive_classifier | baseline | baseline_ok |
| opening-drive-current-basket-discovery__nvda_long | NVDA | opening_drive_classifier | baseline | baseline_ok |
| expand30-w1-b3-p1-smh-opening-drive__smh_short | SMH | opening_drive_classifier | baseline | baseline_ok |
| opening-drive-current-basket-discovery__tsla_short | TSLA | opening_drive_classifier | baseline | baseline_ok |

## Rows To Avoid In Shadow

| catalog_key | symbol | family | reason |
|---|---:|---|---|
| expand30-w1-b4-p2-avgo-elastic-band__avgo_long | AVGO | elastic_band_reversion | avoid_in_shadow_until_retested |
| market-impulse-all-basket-discovery__tsla_short | TSLA | market_impulse | avoid_in_shadow_until_retested |

## Smallest Code Change Recommended

Partial. Do not replace `volume` globally. Add an explicit normalized aggregated volume feature, then let strategies opt into it for gates only.

Recommended derivative:

`relative_volume_sum_{N}_over_ma_{M} = rolling_sum(volume, N) / rolling_mean(rolling_sum(volume, N), M)`

Use `N=3` first where provider flip risk improves; use `N=5` only for symbols where 5m has lower flip risk. Keep Market Impulse VWMA/VPOC/directional-mass on raw provider volume for now, because replacing all volume weights with normalized relative volume materially changes the indicator rather than just stabilizing the gate.

Implementation note: this report added the Newton feature name
`relative_volume_sum_<N>_over_ma_<M>` for future retunes. No existing strategy
row has been changed to request it yet.

## Open Questions

- Provider parity is based on a short May 2026 sample; rerun it weekly during shadow.
- This replay is not a clean new M1-M5 promotion. Any changed variant should go through full staged gates.
- Bhiksha still needs the same normalized aggregated volume feature if Mala adopts it.

Verdict:

- Should Mala add a 3m/5m normalized volume feature? partial
- Which strategy families benefit? explicit-gate Jerk Pivot and, if future rows enable it, Compression/Opening Drive gates. Market Impulse benefits only if an explicit relative-volume gate is added; its VWMA regime should not be globally rewritten from this evidence.
- Which symbols remain unsafe? QQQ, IWM, and SPY remain high risk for volume-gated rows; AMD/NVDA/PLTR are more promising for smoothed gates.
- What should Bhiksha change, if anything? implement the same named feature and make runtime adapters fail closed when a Mala row requests it but Bhiksha cannot compute it.

Supporting CSVs:

- `data/results/volume_mismatch_retune/20260503T_volume_derivative_m5_v2/volume_mismatch_candidate_inventory.csv`
- `data/results/volume_mismatch_retune/20260503T_volume_derivative_m5_v2/volume_mismatch_replay_by_row.csv`
- `data/results/volume_mismatch_retune/20260503T_volume_derivative_m5_v2/volume_mismatch_candidate_decisions.csv`
