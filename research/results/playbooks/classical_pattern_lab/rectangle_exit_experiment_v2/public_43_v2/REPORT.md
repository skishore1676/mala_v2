# Rectangle Exit Experiment V2

## Decision

**no_out_of_sample_edge** — Move on from autonomous rectangle trading; optional discretionary alerts require prospective logging.

Optimization selected `rectangle_height_lfd_buffer_0p00atr`. Its OOS mean was `-0.205R` per signal with profit factor `0.65` and symbol-clustered 95% interval `[-0.535, +0.135]`.

## Design

- Frozen 20/40/60 signals plus 80-only new events; no 80-session replacement of prior representatives.
- Optimization: through 2024-12-31. OOS: 2025 onward. Boundary-crossing events are purged.
- Primary selection metric: mean net R per emitted signal; no-trades count as zero.
- Four variants: rectangle-height and daily Range Expansion analogue, each with raw and 0.10 ATR LFD stops.
- The selected procedure is evaluated once on OOS.

## Scorecard

| Period | Variant | Signals | Mean R/signal | PF | 95% symbol CI | Max DD R |
|---|---|---:|---:|---:|---:|---:|
| optimization | range_expansion_lfd_buffer_0p00atr | 63 | -0.058 | 0.88 | [-0.289, +0.163] | -6.03 |
| optimization | range_expansion_lfd_buffer_0p10atr | 63 | -0.111 | 0.78 | [-0.322, +0.094] | -8.28 |
| optimization | rectangle_height_lfd_buffer_0p00atr | 63 | -0.013 | 0.97 | [-0.255, +0.248] | -8.49 |
| optimization | rectangle_height_lfd_buffer_0p10atr | 63 | -0.015 | 0.97 | [-0.247, +0.231] | -7.61 |
| out_of_sample | range_expansion_lfd_buffer_0p00atr | 33 | -0.237 | 0.63 | [-0.601, +0.140] | -8.90 |
| out_of_sample | range_expansion_lfd_buffer_0p10atr | 33 | -0.223 | 0.65 | [-0.578, +0.149] | -8.42 |
| out_of_sample | rectangle_height_lfd_buffer_0p00atr | 33 | -0.205 | 0.65 | [-0.535, +0.135] | -8.38 |
| out_of_sample | rectangle_height_lfd_buffer_0p10atr | 33 | -0.213 | 0.63 | [-0.524, +0.109] | -8.58 |

## Paired Range Expansion Difference

| Period | Buffer ATR | Signals | Mean profile minus baseline R | Profile better | Baseline better |
|---|---:|---:|---:|---:|---:|
| optimization | 0.00 | 63 | -0.045 | 12 | 23 |
| optimization | 0.10 | 63 | -0.097 | 10 | 24 |
| out_of_sample | 0.00 | 33 | -0.033 | 6 | 10 |
| out_of_sample | 0.10 | 33 | -0.010 | 6 | 10 |

## Limitations

- The 43-symbol universe is a frozen current-symbol cohort, not a point-in-time market universe.
- Public provider adjustment continuity was checked empirically but its policy is undocumented.
- The 2025+ period was inspected in rectangle v1, so it is secondary OOS evidence rather than a pristine program-level holdout.
- The Range Expansion policy is a daily underlying analogue, not the minute-level option-premium profile.
- No result authorizes shadow or live trading.

This artifact is local historical research. It is not a trading recommendation, shadow authorization, or live approval.
