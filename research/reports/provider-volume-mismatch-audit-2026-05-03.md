# Provider Volume Mismatch Audit - 2026-05-03

## Scope

This audit covers the mismatch between Mala backtests, which use Polygon
historical minute bars, and Bhiksha runtime/shadow evaluation, which has used
Schwab minute bars for live data. The goal is to decide whether smoothing
1-minute volume to 3-minute or 5-minute volume makes runtime signals more
provider-invariant.

Primary artifact:

```text
data/results/provider_volume_parity/20260503T_volume_mismatch_baseline/PROVIDER_VOLUME_PARITY_REPORT.md
```

Source divergence data copied from oldmac:

```text
~/Documents/bhiksha/artifacts/provider_divergence/volume_sensitivity_20260501/
```

## Findings

Observed:
Raw 1-minute prices are effectively aligned, but raw 1-minute volume is not.
Across 13 symbols and 20,332 regular-session rows, the report shows average
per-symbol median absolute 1-minute volume disagreement of about 22%.

Inferred:
This is not just random 1-minute allocation noise. Rolling absolute volume over
3, 5, 10, or 20 minutes does not remove the disagreement because Schwab volume
is often persistently lower than Polygon for the same symbol/session.

Observed:
Normalized relative-volume gates are much closer than raw volume. The report
shows average per-symbol median relative-volume disagreement of about 6% on both
3-minute and 5-minute aggregates.

Inferred:
Volume logic should prefer normalized ratios over absolute volume thresholds.
However, threshold flips still matter: the 1.2x gate flip rate is about 7.2% on
3-minute aggregates and about 6.3% on 5-minute aggregates.

Observed:
AMD improves materially with aggregation. Its 1.2x relative-volume flip rate
drops from about 4.6% on 1-minute bars to about 1.8% on 3-minute aggregates and
about 2.7% on 5-minute aggregates.

Observed:
QQQ, IWM, and SPY do not improve with 3-minute or 5-minute aggregation. Their
relative-volume disagreement and gate flip rates remain elevated.

Inferred:
Smoothing should be symbol-aware or strategy-aware. A global switch from
1-minute to 3-minute/5-minute volume would help some candidates but can make
others worse.

Observed:
Directional-mass values are highly provider-sensitive because they multiply
internal bar strength by raw volume. VPOC percentage drift is generally small,
because VPOC is more sensitive to relative volume by price bucket than total
volume scale.

Inferred:
Strategies using the sign of directional mass may be less exposed than metrics
using its magnitude, but any threshold on mass magnitude should be treated as
provider-sensitive.

## Adapter Audit

Market Impulse:

- Mala supports optional `use_volume_filter` through `relative_volume_N`.
- Bhiksha's current Market Impulse runtime adapter does not request or evaluate
  `relative_volume_N`.
- Bhiksha supports only the cross-reclaim runtime variant. Descendant features
  such as `close_location` and `vma_excursion_pct` remain unsupported at runtime.
- Market Impulse still uses volume inside VWMA/regime construction, so provider
  volume can affect regime even when the explicit volume filter is disabled.

Jerk Pivot Momentum:

- Both sides support parameterized kinematics such as `velocity_3`, `accel_3`,
  and `jerk_3`.
- Both sides use `volume >= volume_multiplier * volume_ma_N` when the volume
  filter is enabled.
- This family should be audited with normalized gate flip rates by symbol before
  using volume-filtered candidates in shadow.

Opening Drive Classifier:

- Mala declares `accel_N` and `jerk_N` as required features.
- Bhiksha computes velocity, acceleration, and jerk internally from close using
  `kinematic_periods_back`, so the prior `accel_3` concern is not currently a
  missing-feature issue for this adapter.
- Opening Drive uses raw current volume versus opening-window mean. That is a
  normalized gate, but the threshold can still flip when provider-relative shape
  differs inside the session.

Elastic Band Reversion:

- Both sides depend on VPOC and optionally directional mass.
- VPOC drift appears much smaller than raw volume drift in the current provider
  divergence sample.
- Directional mass magnitude is provider-sensitive; sign-based use is less
  risky than magnitude thresholds.

## Recommendation

Do not switch every strategy to 3-minute or 5-minute volume globally.

Use this rule for the next research pass:

- Prefer candidates with no explicit volume gate while plumbing is being tested.
- For volume-gated candidates, require a provider parity check by symbol and
  threshold.
- Test 3-minute and 5-minute aggregated relative-volume variants only where the
  parity report shows lower gate flip rates than 1-minute.
- Treat QQQ, IWM, and SPY volume-gated rows as higher-risk until a separate
  provider-specific explanation exists.
- Treat AMD and NVDA as better candidates for smoothed relative-volume tests.

## Implementation Added

New command:

```bash
python -m src.research.research_ops provider-volume-parity \
  --divergence-dir ../bhiksha/artifacts/provider_divergence \
  --session regular
```

New same-bar replay diagnostics:

`bhiksha-signal-ev --same-bar-replay` now records feature-level replay
comparisons when Bhiksha signal events include runtime features. The signal CSV
includes:

```text
mala_same_bar_feature_compared
mala_same_bar_feature_mismatch_count
mala_same_bar_feature_max_pct_diff
mala_same_bar_feature_worst
mala_same_bar_feature_diffs
```

Use these columns to separate provider mismatch from strategy adapter mismatch
on actual shadow events.

Handoff guardrail:

Mala now marks Market Impulse `cross_reclaim` rows with
`use_volume_filter=true` as unsupported for Bhiksha handoff, because the
current Bhiksha runtime adapter verifies cross-reclaim price/regime logic but
does not evaluate Mala's `relative_volume_N` filter.
