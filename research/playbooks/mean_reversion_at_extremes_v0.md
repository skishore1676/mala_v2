# Mean Reversion at Extremes — Playbook Family Spec v0

**Status:** Phase 0 scaffold
**Owner:** Suman + Codex designer
**Implementation state:** Intraday subtype has a Phase 1 spine; multi-day subtype
is not implemented yet.
**Vision source:** `docs/MALA_VISION_v2.2.md`
**First-slice contract:** `docs/MALA_2_2_FIRST_SLICE.md`
**Build spec:** `docs/MALA_2_2_INTRADAY_REVERSION_SURFACE_SPEC.md`
**Concrete intraday spec:** `research/playbooks/mean_reversion_at_extremes_intraday_v1.md`

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

The concrete intraday v1 search grid lives in
`research/playbooks/mean_reversion_at_extremes_intraday_v1.md`. This parent spec
should stay broad enough to explain the family; the v1 file owns the exact
initial grid.

The first output should help Suman decide whether the intraday play "looks
right" statistically before spending effort on visual plotting or runtime work.
Runtime/Bhiksha work comes later.

---

## Parent Type

### Type 1: Mean Reversion at Extremes

- **Description:** Asset X is overextended and will revert toward a reference
  level.
- **Time horizon:** intraday for the first subtype; 1-3 days for the multi-day
  subtype.
- **Asset scope:** single name, ETF, or index.
- **Natural features:** extension measures, exhaustion signals, breadth/context,
  VIX/regime behavior, and reference-level distance.
- **Example:** IWM is overextended on the upside; expect a down-move toward a
  reference level.

This parent type is close to how Mala already describes hypotheses. The change
in Mala 2.2 is that the trader supplies the bias and play type, while the system
searches entry, invalidation, and exit parameter surfaces and reports where the
historical evidence is strongest.

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

### Parameter Surface To Search

These are not questions for Suman to hand-pick one by one. They are candidate
parameter families for Mala to search:

- time windows from cash open through the broad morning window
- stretch definitions such as z-score from VWAP/VPOC and ATR-normalized distance
- velocity interaction: no filter, moderate stretch, or climactic move
- 5-minute and 15-minute reversal-range definitions
- breakout confirmation from the reversal range, including 1 vs 2 confirming bars
- kinematic exhaustion using jerk/velocity where it maps cleanly to the play
- stop definitions based on reversal-bar low or midpoint
- stage/context filters based on operator read, VMA/VWMA stack, and
  `impulse_regime_5m`
- exit definitions such as VWAP return, partial retrace, fixed R, short
  moving-average/VWMA return, and time stop
- invalidation definitions such as loss of reversal range, VWMA stack flip, or
  failure to validate within a time window

The operator-facing output should be:

```text
For this symbol and playbook, the historically strongest region is X.
Today's conditions match / partially match / do not match that region.
Proceed, proceed with lower conviction, or skip.
```

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
research/results/playbooks/mean_reversion_at_extremes/<run_ts>/
  RECEIPT.md
  conditional_surface_by_symbol.csv
  feature_bins_by_symbol.csv
  sample_events.csv
  config.json
```

Chart marks for thinkorswim or TradingView should come after the first
parameter surface produces candidates worth visually inspecting. The preferred
TradingView path is a MCP review queue from `sample_events.csv`, not a separate
static chart renderer.
