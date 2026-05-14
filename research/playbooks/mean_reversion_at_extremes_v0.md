# Mean Reversion at Extremes — Playbook Family Spec v0

**Status:** Phase 0 scaffold
**Owner:** Suman + Codex designer
**Implementation state:** No code yet
**Vision source:** `docs/MALA_VISION_v2.2.md`
**First-slice contract:** `docs/MALA_2_2_FIRST_SLICE.md`

---

## Purpose

Build the first Mala 2.2 playbook family surface.

This is not a search for a universal autonomous strategy. The target is an operator-bias-conditioned surface:

```text
Suman arrives with:
  "I want to fade this symbol because it looks overextended."

Mala 2.2 answers:
  "Historically, this playbook improved under these conditions, failed under these conditions,
   and today currently looks favorable / partial / outside / not enough evidence."
```

The family splits into:

- `mean_reversion_at_extremes_intraday`
- `mean_reversion_at_extremes_multi_day`

The first output should help Suman decide whether the intraday play "looks
right" statistically before spending effort on visual plotting or runtime work.
Runtime/Bhiksha work comes later.

---

## Initial Scope

- Playbook: `mean_reversion_at_extremes`
- First slice: `mean_reversion_at_extremes_intraday`
- Symbols: `IWM`, `QQQ`
- Directions: both long and short
- Data: underlying bars only for Phase 0-2
- Options overlay: explicitly deferred until a playbook surface earns it
- Runtime: no Bhiksha changes in Phase 0
- Visual plotting: deferred until the parameter surface has plausible candidates

Candidate symbols:

```text
IWM:
  because the motivating example is overextension / fade bias on IWM.

QQQ:
  because it provides a liquid index/tech contrast.

NVDA:
  deferred; a high-beta single name should not trade like IWM, but it adds
  complexity before the first surface is understood.
```

---

## Intraday Play Draft

### Plain-English Play

Working draft:

> Early in the session, a symbol reaches an extreme but remains in an
> accumulation/bullish context. After a 5-minute or 15-minute reversal range
> forms, Suman looks for a breakout from that reversal range and manages risk
> against the reversal bar low or midpoint.

This is intentionally crude. The first Mala 2.2 job is to discover which parts
of that description matter and which are just chart vocabulary.

### Operator-Anchored Defaults

- Entry timing: before roughly 9:00-9:10 Central time, to be operationalized in
  ET for the repo.
- Context: accumulation/bullish stage, currently read by Suman or approximated
  with the 8/21/34 VMA/VWMA stack.
- VWAP: context only at first; Suman sometimes looks at it, but it is not the
  core trigger.
- Trigger structure: 5-minute or 15-minute reversal range, then breakout from
  that reversal range.
- Initial stop candidates: low of the reversal bar or midpoint of the reversal
  bar.
- Visual review: after parameter-surface optimization, not before.

### Questions Still Open

- Does the first pass use a 10:00 ET cutoff, a 10:10 ET cutoff, or test both?
- Is the first trigger a 5-minute reversal range, a 15-minute reversal range,
  or a parameter surface across both?
- Does "bullish/accumulation stage" require a VWMA stack, operator-read proxy,
  or both?
- For profit taking, should candidate exits include VWAP return, partial
  retrace, fixed R, short moving-average return, and time stop?
- Which invalidation is primary: loss of reversal-bar low/mid, loss of reversal
  range, VWMA stack flip, or failure to validate within a time window?

### Trader-Anchored Feature Candidates

Features Suman likely cares about visually or structurally:

- distance from VWAP
- distance from moving average
- distance from prior day / recent range
- ATR-normalized extension
- z-score extension
- slope / velocity of the move
- acceleration only if it matches chart intuition
- VMA/VWMA stack 8/21/34 as stage/context
- regime context: daily trend, 4h trend, VIX / market regime
- exhaustion signs: failure to extend, wick/reclaim behavior, volume fading or climax

Phase 0 task: choose the small feature set to inspect first. Do not include
features just because the engine can compute them.

From the operator note, RSI should not be a Tier 1 feature. It can be tested
only as a Tier 2 research candidate if the paper review gives a specific reason.
The more relevant note-derived candidates are EMA/VWMA stack, volatility
extreme/band concepts, ADX/chop filtering, and VWAP/anchored VWAP as context.

### Entry Surface Questions

For each symbol and direction, Phase 1 should be able to answer:

- At what extension ranges did fading historically improve?
- Which extension definitions behave similarly and which disagree?
- Does the play work only after a confirmation candle?
- Does time of day matter?
- Does regime matter?
- Does the 5-minute or 15-minute reversal range matter more?
- Does the low-stop or midpoint-stop version behave better?
- Is the favorable region broad and stable, or just one fragile threshold?

### Invalidation / Exit Surface Questions

Potential thesis-broken conditions:

- price accepts above/below the extreme instead of rejecting it
- VWAP or reference-level behavior invalidates the fade
- move continues with strength after entry
- regime flips into trend continuation
- time stop: reversion did not happen within expected window
- catastrophic stop: underlying move beyond approved max adverse excursion
- reversal-bar low or midpoint is breached
- reversal range breaks and then fails immediately

Phase 0 task: decide which invalidation families belong in the first exploration.

---

## Phase 0 Acceptance Criteria

Phase 0 is complete when this document has:

- final symbols
- agreed horizon
- agreed directions
- Tier 1 trader-visible features
- Tier 2 agent-proposed features, if any
- initial invalidation feature families
- plotting/visualization deferral criteria
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
  RECEIPT.md
  conditional_surface_by_symbol.csv
  feature_bins_by_symbol.csv
  sample_events.csv
  config.json
```

Chart marks for thinkorswim or TradingView should come after the first
parameter surface produces candidates worth visually inspecting.
