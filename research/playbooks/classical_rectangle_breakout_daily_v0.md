# Classical Rectangle Breakout Daily — Playbook Spec v0

**Status:** implementation baseline approved for source-grounded fixture-shadow research
**Doctrine owner:** cited Peter Brandt public sources, narrowed by this versioned spec
**Research owner:** Mala v2
**Project sponsor and later capital authority:** Suman
**Playbook id:** `classical-rectangle-breakout-daily`
**Family:** classical pattern breakout
**Horizon:** multi-day
**Asset scope:** point-in-time liquid U.S. equities and ETFs; fixture proof first
**Architecture:** `docs/CLASSICAL_PATTERN_LAB_PRD.md`
**Machine config:** `config/classical_patterns/rectangle_daily_v1.yaml`

This is a research definition, not an execution packet. It encodes one narrow,
inspectable subset of classical chart-pattern practice so Mala can measure
semantic fidelity and economic outcomes without claiming to reproduce Peter
Brandt's private judgment or performance.

Suman is not the setup-labeling authority for this lane. He does not need to
trade like Brandt or decide whether an as-of chart is personally attractive.
Source-grounded reviewers audit whether this spec and its examples faithfully
encode the cited public method. Mala owns complete-population enumeration,
mechanics, and economic measurement. Suman owns the decision to continue,
refine, stop, or later authorize capital after seeing the evidence.

## Thesis

```text
When price spends multiple daily sessions contained inside a sufficiently
horizontal range, repeatedly tests both boundaries, and then closes beyond one
boundary, the completed range may begin a measured move in the breakout
direction. The Last Full Day level controls initial trade risk; the opposite
range boundary controls structural pattern negation.
```

The first question is not "is this profitable?" It is:

```text
Can a frozen causal rule enumerate the same rectangle concept consistently,
then report every resulting trade without hindsight or human curation?
```

## Non-Goals

- No triangles, wedges, channels, flags, head-and-shoulders, or morphing rules.
- No intraday entry, same-close fill, options overlay, sizing, or live scanning.
- No agent authority over breakout, Last Full Day, objective, lifecycle, or P&L.
- No use of TradingView content as machine-readable research input.
- No packet, parity, shadow, Sheet, or Bhiksha integration.

## Operator Language

A valid candidate should look like:

- a horizontal box lasting long enough to represent real balance;
- repeated tests near both the upper and lower boundary;
- limited net drift from the first to the final pre-breakout close;
- a full daily close beyond the relevant boundary plus a frozen ATR buffer;
- a measurable box height large enough to define risk and objective.

Reject or downgrade when:

- one spike creates a boundary with no repeated test;
- the range is too wide or too narrow relative to prior ATR;
- the pre-breakout window trends materially from one side to the other;
- fewer than the required touches exist on either boundary;
- the breakout is visible only intraday and does not close beyond the boundary;
- the next open has already crossed the objective or invalidated the stop.

## Time and Session Scope

- Source bars: adjusted U.S. equity OHLCV.
- Daily construction: regular trading hours grouped by
  `America/New_York` session date.
- Signal becomes known: after the official daily close.
- Executable baseline: next session open.
- Maximum lifecycle and trade horizon: config-defined trading sessions.
- Incomplete source sessions: retained in diagnostics and excluded from the
  production signal population under the frozen completeness rule.

## Direction and Vehicle

- Directions: long and short, evaluated separately.
- Research vehicle: underlying shares with unit-notional return accounting.
- Entry/fill: next-session open with adverse slippage.
- Same-close fill: non-executable diagnostic only; not implemented as a trade.

## Deterministic Pattern Definition

For each possible breakout session `t` and each predeclared lookback `N`:

1. Use only the `N` completed daily bars ending at `t-1`.
2. Identify swing highs/lows with the frozen centered pivot span. A pivot is
   usable only when all right-side confirmation bars also end by `t-1`.
3. Cluster confirmed swing-high and swing-low prices within the frozen ATR
   tolerance; use each cluster's median as its central boundary.
4. Form upper/lower pairs around the median window close. Require repeated,
   separated touches, alternating interaction, recent contact with both sides,
   center/outer close containment, ATR-scaled height, and bounded OLS drift.
5. Define an outer upper/lower edge by adding/subtracting the frozen tolerance
   from the central boundaries.
6. Compute ATR using only bars available through `t-1`.
7. Long breakout: close at `t` is greater than the upper outer edge plus the
   breakout buffer. Short breakout is symmetric below the lower outer edge.
8. Within each lookback, select the best qualifying geometry before reading the
   breakout close. The audit row records how many alternatives qualified; an
   alternative is never promoted merely because the breakout cleared its edge.
9. If multiple lookbacks then produce the same symbol, breakout session, and
   direction, keep one representative using only pre-breakout information:
   minimum-side touch count, alternations, close containment, lower boundary
   dispersion, touch recency, shorter lookback, and stable lexical tie-breaking.

Every selected-lookback candidate remains in diagnostics, and every audit row
includes its qualifying-geometry count. Every representative signal enters the
outcome population unless a predeclared mechanical rejection reason applies.
Human review cannot add or remove economic signals.

## Evidence and Authority Contract

The lane keeps four authorities separate:

1. **Public doctrine evidence:** cited Brandt material and the TechCharts
   taxonomy constrain what may be called Brandt-inspired.
2. **Semantic audit:** independent, outcome-blind reviewers judge fidelity to
   that frozen source contract and may mark genuine source ambiguity.
3. **Deterministic research:** Mala decides mechanically whether an event
   qualifies, enters, exits, or is rejected, and reports the entire population.
4. **Human governance:** Suman decides whether the evidence justifies another
   research iteration or a separately governed promotion. His chart preference
   is not a training label and cannot select the backtest sample.

`trade`, `watch`, and `no_trade` impressions captured in earlier calibration
packets remain historical reviewer observations only. They are not doctrine,
ground truth, eligibility filters, or optimization targets.

The V1 `RectangleCandidate.tradeable` field is retained only for artifact
compatibility and is ignored by the simulator. Mechanical no-trade reasons are
derived inside the simulator from bars and frozen config. Each run fails closed
unless the set of signal ids entering economics exactly equals the enumerator's
representative signal ids and the exact `(signal_id, variant_id)` pair set has
one row for every predeclared variant with no duplicates.

## Derived Levels

Mala derives these values from resolved bars plus the frozen definition:

- **Breakout boundary:** upper outer edge for long, lower outer edge for short.
- **Last Full Day:** scan backward from `t-1`; use the most recent completed
  bar whose full high-low range lies inside the rectangle.
- **Initial risk stop:** below the Last Full Day low for long, above its high
  for short, plus the configured adverse buffer.
- **Structural negation:** opposite outer edge plus the configured adverse
  buffer.
- **Measured objective:** one central-boundary rectangle height projected from
  the central breakout-side boundary.
- **Expiry:** configured number of sessions after breakout.

The Last Full Day stop is a trade-quality/risk rule. Structural negation is a
pattern-validity rule. They must remain separate in artifacts.

## Breakout Outcome Classification

Classification is retrospective after breakout and independent of entry/fill:

- `type_1`: objective occurs before any meaningful boundary retest.
- `type_2`: boundary is retested, Last Full Day remains intact, then objective
  occurs.
- `type_3`: Last Full Day is violated, structural negation remains intact, then
  objective occurs.
- `type_4`: structural negation occurs before objective.
- `unresolved`: objective and negation occur in the same daily bar without
  lower-timeframe ordering evidence.
- `censored`: available data or the frozen horizon ends before objective or
  structural negation.

The breakout bar itself is not used to claim a post-close objective or
negation path. Classification begins with the next session. Any LFD, negation,
or objective level touched earlier inside the breakout bar is retained as a
diagnostic code; it does not veto a prospective next-open trade or manufacture
an outcome.

## Trade Rule Variants

The v1 fixture-shadow implementation has a deliberately bounded grid:

- next-open entry only;
- raw Last Full Day stop and one ATR-buffered stop variant;
- one-height measured objective;
- zero re-entries;
- fixed session expiry;
- conservative stop-before-objective ordering when both trade levels occur in
  one daily bar;
- adverse entry/exit slippage and round-trip cost from config.

Re-entry remains deferred until the single-entry population is proven causal
and reviewable.

## Output Contract

Each run writes:

- `receipt.json` and `RECEIPT.md`;
- `daily_bars.parquet`;
- `candidates.csv` with every qualifying lookback candidate;
- `signals.csv` with one causal representative per breakout cluster;
- `lifecycle_events.csv`;
- `outcomes.csv`;
- `trades.csv`, including mechanical no-trade reasons;
- `economic_scorecard.csv`;
- `REPORT.md`.

The receipt includes code/data/config hashes, dirty-tree state, inclusion and
same-bar policies, tested variants, rejected counts, and artifact paths.

## Consultation Contract

Deferred until deterministic fixture shadow and historical surface review pass.
The future desk card must preserve `READ`, `STATE`, `ANALOG`, `POLICY`, `EXIT`,
`STOP`, and `WATCH`, but this first slice does not manufacture a recommendation.

## Locked Packet Criteria

No packet may be proposed until:

- semantic samples match the intended rectangle language;
- the full-population surface is reported across frozen splits;
- costs and concentration are visible;
- a specific variant is reviewed rather than selected only for peak returns;
- P1 surface review passes.

## Feasibility Classification

- Build tag: `new-class + new-feature`.
- Reuse: Chronos bars, ET helpers, Polars conventions, Oracle trade records and
  cost/stress ideas, playbook receipt/artifact discipline.
- New build: daily RTH normalization, rectangle enumeration, classical-pattern
  lifecycle, next-open simulator, full-population runner.
- Deferred: agent-model workflow, consultation adapter, packet registry,
  kernel/Bhiksha runtime, options, and live authorization.

## Frozen v1 Decisions

| Date | Decision | Reason |
| --- | --- | --- |
| 2026-07-17 | Daily horizontal rectangles first | Smallest complete classical-pattern lifecycle with inspectable boundaries |
| 2026-07-17 | Next-session-open executable baseline | Daily close confirmation is not fillable at that same close |
| 2026-07-17 | Full enumerator population owns economics | Prevent reviewed-example and agent curation bias |
| 2026-07-17 | Type 1–4 computed after breakout | Outcome labels cannot enter signal construction |
| 2026-07-17 | Underlying-only fixture shadow | Vehicle and money-path work must be earned later |
| 2026-07-17 | Confirmed pivots and ATR-scaled boundary clusters | Makes repeated tests causal and independently auditable |
| 2026-07-17 | Zero re-entries in v1 | Avoids doubling the event/accounting state before the base population is proven |
| 2026-07-17 | Suman is sponsor, not Peter-style chart curator | Personal trade preference cannot establish fidelity to an external method or select an economic sample |
| 2026-07-17 | Source fidelity, mechanics, and alpha are separate gates | Prevents reviewer taste from leaking into deterministic eligibility or P&L |
