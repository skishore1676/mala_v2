# Mala / Bhiksha Parity Audit - 2026-05-16

## Verdict

Yes: the Mala transformation issue we found can plausibly explain some wrong
Bhiksha fires from older M1-M5 strategy packets.

That is not proven for every historical trade, and it is probably not the only
cause. But the risk is real because the older lane publishes strategy params
from Mala, then Bhiksha recomputes live features with its own copied Newton and
runtime strategy adapters. If Mala fixes a feature or strategy semantic and
Bhiksha does not carry the same semantic, Bhiksha can fire extra signals, miss
expected signals, or manage a trade under a different state read.

## Current State Checked

- Bhiksha PR #4 was merged into `main` as merge commit
  `176ab98f15a5136d19d392b2df6ca51a6e02e4ba`.
- Local Bhiksha `main` was fast-forwarded after the merge.
- Local Bhiksha tests passed after the merge: `250 passed`.
- Oldmac Bhiksha is still at git SHA `5ea2448` with local dirty changes and a
  runtime capability manifest generated at `2026-05-15T20:25:02Z`.
- Oldmac active plan currently has `14` shadow-only deployments:
  `market_impulse=7`, `opening_drive_classifier=3`,
  `jerk_pivot_momentum=2`, `elastic_band_reversion=1`,
  `manual_breakout=1`.

## Findings

### 1. The new playbook lane is not Bhiksha-runtime ready yet

The Mala 2.2 mean-reversion playbook uses features that exist in Mala's Newton
surface but not in Bhiksha's runtime Newton copy yet:

- `opening_vwap_rth`
- `prior_rth_close_atr`
- `prior_rth_close`
- `atr_distance_from_prior_rth_close`
- `relative_volume_rth`
- state percentile context used by the consultation card

That is correct for the current stage. The playbook should stay advisory until
Bhiksha gets a matching playbook adapter and parity tests.

### 2. Older M1-M5 packets have real parity exposure

The older strategy lane does cross into Bhiksha today. The active oldmac book
includes Market Impulse, Opening Drive, Jerk Pivot, and Elastic Band rows. Those
strategies depend on duplicated runtime implementations rather than a shared
Mala Newton package.

Risk areas:

- **Market Impulse:** VMA/VWMA stack, regime timeframe, MarketPulse stage
  vocabulary, warmup length, and provider-sensitive volume semantics.
- **Opening Drive:** opening-window boundaries, ET date handling, acceleration,
  jerk, directional mass, optional regime filter, and volume gate semantics.
- **Jerk Pivot:** VPOC, velocity/acceleration/jerk, jerk smoothing, volume
  moving average, and time filter.
- **Elastic Band:** VPOC distance, rolling z-score, internally recomputed
  velocity/jerk, and directional mass.

### 3. Capability checks are necessary but not sufficient

Bhiksha now has a runtime capability generator. It verifies that supported
strategy variants can compute required columns and run strategy probes. That is
good plumbing, but it does not yet prove numeric parity with Mala on the same
OHLCV bars.

The missing contract is:

```text
same input bars + same params
  -> same feature values within tolerance
  -> same signal timestamps/directions
  -> same thesis-exit decisions where applicable
```

### 4. The oldmac runtime manifest is stale relative to merged main

Oldmac's capability manifest was generated from Bhiksha SHA `5ea2448`, before
PR #4 was merged. The next runtime refresh should regenerate capabilities from
current main before trusting another active-plan compile.

## Answer to the Wrong-Signal Question

Most likely answer: **yes, transformation drift could have been a contributor**.

It is strongest for signals that depended on provider-sensitive or recently
touched features: volume, VWMA/VMA, VPOC, directional mass, opening/session
boundaries, and warmup. It is less likely to explain purely option-management
issues, broker lifecycle issues, or cases where Mala and Bhiksha were using
different providers.

So the audit hypothesis should be:

```text
Wrong fire = possible combination of:
  1. Mala/Bhiksha feature transform drift
  2. provider drift: Polygon vs Schwab/Public volume bars
  3. warmup/session boundary drift
  4. strategy adapter semantic drift
  5. execution/order-management lifecycle bugs
```

## Required Next Audit

Build a parity harness before expanding either lane:

1. Pull the exact active-plan strategy params from Bhiksha.
2. Replay the same raw OHLCV windows through Mala Newton + Mala strategy.
3. Replay the same bars through Bhiksha Newton + Bhiksha strategy.
4. Compare feature columns, signal booleans, signal directions, and exit
   decisions.
5. Emit a CSV with `match`, `missed_by_bhiksha`, `extra_in_bhiksha`, and
   feature-drift columns.

Promotion rule:

- Playbook lane: no Bhiksha execution packet until the playbook adapter passes
  this parity harness.
- M1-M5 lane: no live promotion when parity has unresolved misses/extras for the
  active packet's strategy family.
