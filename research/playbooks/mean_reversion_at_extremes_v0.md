# Mean Reversion at Extremes — Playbook Spec v0

**Status:** Phase 0 scaffold
**Owner:** Suman + Codex designer
**Implementation state:** No code yet
**Vision source:** `docs/MALA_VISION_v2.2.md`
**First-slice contract:** `docs/MALA_2_2_FIRST_SLICE.md`

---

## Purpose

Build the first Mala 2.2 playbook surface.

This is not a search for a universal autonomous strategy. The target is an operator-bias-conditioned surface:

```text
Suman arrives with:
  "I want to fade this symbol because it looks overextended."

Mala 2.2 answers:
  "Historically, this playbook improved under these conditions, failed under these conditions,
   and today currently looks favorable / partial / outside / not enough evidence."
```

The first output should help Suman decide whether the play "looks right" both statistically and visually. Runtime/Bhiksha work comes later.

---

## Initial Scope

- Playbook: `mean_reversion_at_extremes`
- Symbols: 2-3 symbols only, selected before coding
- Directions: both long and short
- Data: underlying bars only for Phase 0-2
- Options overlay: explicitly deferred until a playbook surface earns it
- Runtime: no Bhiksha changes in Phase 0

Candidate symbols to decide:

```text
IWM:
  because the motivating example is overextension / fade bias on IWM.

QQQ or SPY:
  because index ETF behavior is cleaner and sample size should be strong.

NVDA:
  because a high-beta single name should not trade like IWM; useful contrast if included.
```

---

## Phase 0 Questions

### Plain-English Play

What does "mean reversion at extremes" mean in trader language?

Working draft:

> A symbol has moved too far too fast relative to its recent behavior, and I want to fade the move when the extension looks exhausted or likely to snap back toward a reference level.

Needs Suman review:

- Does this play mean intraday snapback, multi-hour pullback, or next-day reversion?
- Is the default trade fade-up moves, fade-down moves, or both?
- Is the target "back to VWAP," "back to moving average," "partial retrace," or "just not more continuation"?

### Trader-Anchored Feature Candidates

Features Suman likely cares about visually:

- distance from VWAP
- distance from moving average
- distance from prior day / recent range
- ATR-normalized extension
- z-score extension
- slope / velocity of the move
- acceleration only if it matches chart intuition
- regime context: daily trend, 4h trend, VIX / market regime
- exhaustion signs: failure to extend, wick/reclaim behavior, volume fading or climax

Phase 0 task: choose the small feature set to inspect first. Do not include features just because the engine can compute them.

### Entry Surface Questions

For each symbol and direction, Phase 1 should be able to answer:

- At what extension ranges did fading historically improve?
- Which extension definitions behave similarly and which disagree?
- Does the play work only after a confirmation candle?
- Does time of day matter?
- Does regime matter?
- Is the favorable region broad and stable, or just one fragile threshold?

### Invalidation / Exit Surface Questions

Potential thesis-broken conditions:

- price accepts above/below the extreme instead of rejecting it
- VWAP or reference-level behavior invalidates the fade
- move continues with strength after entry
- regime flips into trend continuation
- time stop: reversion did not happen within expected window
- catastrophic stop: underlying move beyond approved max adverse excursion

Phase 0 task: decide which invalidation families belong in the first exploration.

---

## Phase 0 Acceptance Criteria

Phase 0 is complete when this document has:

- final 2-3 symbols
- agreed horizon
- agreed directions
- Tier 1 trader-visible features
- Tier 2 agent-proposed features, if any
- initial invalidation feature families
- chart visualization requirement
- explicit "do not build yet" list for Phase 1

Phase 0 is not complete if:

- the feature list is broad because "we might as well"
- the playbook cannot be explained in one paragraph
- entry and invalidation are mixed together
- Bhiksha or options work is being pulled forward

---

## Do Not Build Yet

- no Bhiksha adapter
- no option-overlay simulator
- no global playbook registry
- no Google Sheet publication
- no agent idea funnel
- no new live/shadow runtime path

---

## Phase 1 Handoff Shape

When Phase 0 is approved, the builder agent should receive:

- this spec
- the final symbol list
- the frozen feature candidate list
- expected output artifact format
- acceptance criteria for the first conditional-surface report

Expected Phase 1 output:

```text
data/results/playbooks/mean_reversion_at_extremes/<run_ts>/
  conditional_surface_summary.md
  conditional_surface_by_symbol.csv
  event_marks_for_chart.csv
  feature_bins_by_symbol.csv
```

The chart artifact should make it easy to inspect whether the historical events visually match the intended play.
