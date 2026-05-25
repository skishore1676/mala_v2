# <Playbook Name>

**Status:** draft
**Owner:** Suman + Codex
**Playbook id:** `<slug>`
**Family:** `<family>`
**Horizon:** `<intraday | multi-day | swing | other>`
**Asset scope:** `<symbols or universe>`

This document defines a trader-supplied playbook. It is not an autonomous
strategy promotion artifact. Mala maps the evidence surface; the trader owns
the play definition.

---

## Thesis

Plain-language thesis:

```text
<When I see X, I expect Y over Z horizon because...>
```

Example operator question:

```text
I want to <long/short> <symbol> because <chart/bias read>. Is this a good idea,
and if I take it, how should I manage it?
```

## Non-Goals

- `<What this playbook is not>`
- `<Adjacent playbooks that should stay separate>`

## Operator Language

The trader would describe a valid setup as:

- `<visual/context clue>`
- `<timing clue>`
- `<stage/regime clue>`
- `<trigger clue>`
- `<management instinct>`

The trader would reject or downgrade the setup when:

- `<semantic rejection>`
- `<context rejection>`
- `<execution/vehicle rejection>`

## Time And Session Scope

- Session: `<RTH | premarket | full session | multi-day>`
- Primary decision window: `<e.g. 09:30-11:00 ET>`
- Expected hold: `<e.g. 5-30m, EOD, 1-3 days>`
- Out-of-window behavior: `<out_of_scope | management-only | different playbook>`

## Direction And Asset Scope

- Directions: `<long, short, both>`
- Symbols/universe: `<IWM, QQQ, ...>`
- Vehicle assumption: `<underlying-only research | 0-2DTE options | other>`

## Natural Feature Families

Features the play may need:

| Family | Candidate features | Why it matters | Existing support |
| --- | --- | --- | --- |
| Stretch / extension | `<...>` | `<...>` | `<yes/no/file>` |
| Trend / stage | `<...>` | `<...>` | `<yes/no/file>` |
| Trigger / confirmation | `<...>` | `<...>` | `<yes/no/file>` |
| Volume / participation | `<...>` | `<...>` | `<yes/no/file>` |
| Regime / context | `<...>` | `<...>` | `<yes/no/file>` |
| Time / session | `<...>` | `<...>` | `<yes/no/file>` |

## Entry Families

Candidate entry concepts:

- `<entry family 1>`
- `<entry family 2>`
- `<entry family 3>`

Mala should search numeric thresholds where possible. Bring Suman in only when
the entry semantics change.

## Context / Stage Families

Candidate context filters or dimensions:

- `<stage / MarketPulse / VWMA stack>`
- `<gap / prior close / ATR stretch>`
- `<volatility / VIX / broader index context>`
- `<relative strength / breadth / correlation context>`

These are search dimensions first. Do not kill the whole playbook because it
only works in one context; map the context where it works.

## Invalidation Families

Separate catastrophic risk stop from thesis invalidation:

| Type | Candidate rule | Meaning |
| --- | --- | --- |
| Risk stop | `<price/ATR/bar stop>` | `<where risk is capped>` |
| Thesis invalidation | `<state/context flip>` | `<why the idea is no longer true>` |
| Time invalidation | `<minutes/session rule>` | `<why the move is too late>` |

## Management / Exit Families

Candidate management rows:

- `<fixed R>`
- `<reference return>`
- `<partial retrace>`
- `<state flip>`
- `<time stop>`
- `<vehicle-aware scalp target>`

For options-led use, define which exits are underlying-anchored and which are
option-premium or execution-quality exits.

## Surface Grid

Initial search dimensions:

| Axis | Initial values | Notes |
| --- | --- | --- |
| `<axis>` | `<values>` | `<why>` |

Caps or sampling limits:

- `<max configs / axis sweep / full interaction / reason>`

## Output Contract

Required artifacts:

- `RECEIPT.md`
- `conditional_surface_by_symbol.csv`
- `feature_bins_by_symbol.csv`
- `sample_events.csv`
- `surface_review/SURFACE_REVIEW.md`
- chart-review / TradingView packet when useful

Review rows should show:

- candidate type
- match grade
- calibration and holdout stats
- failed criteria
- sample count
- chart-review events

## Consultation Contract

State-management query should answer:

```text
I am looking at <symbol> <direction> at <timestamp>. What did nearest historical
analogs do next, and what management choices would have helped?
```

Policy card fields:

- `READ`
- `STATE`
- `ANALOG`
- `POLICY`
- `EXIT`
- `STOP`
- `WATCH`

Playbook-specific state percentiles:

- `<percentile metric 1>`
- `<percentile metric 2>`
- `<percentile metric 3>`

## Journal Gate

Before locking a packet, collect:

- target closed rows: `<8-12 default>`
- mix of take/pass decisions
- card agreement and disagreement rows
- operator notes
- actual outcomes

Batch review questions:

- Did Mala reduce bad trades?
- Did it improve management choice?
- Did it reject trades the trader still believes were valid?
- Were exits realistic for the vehicle?
- Should policy thresholds change?

## Locked Packet Criteria

Lock a packet only if:

- chart semantics match the intended play
- consultation rows show useful decision or management value
- management policy is realistic
- target/stops are tradable
- feature contract can be recomputed live

Packet must define:

- feature constraints
- trigger
- risk stop
- thesis invalidation
- selected management policies
- time rules
- execution vehicle assumptions
- Bhiksha/public live feature contract

## Execution Bridge

Expected handoff:

```text
Mala policy card
-> PlaybookTradeIntent
-> Bhiksha/public option preview
-> operator approval
-> managed lifecycle
-> feedback artifact back to Mala
```

Executioner responsibilities:

- option contract selection
- spread/liquidity/risk checks
- sizing
- order preview
- protective management
- underlying-thesis monitoring
- option-behavior monitoring
- feedback logging

## Feasibility Classification

- Build tag: `<config-only | new-class | new-feature | new-execution-contract>`
- Reusable components:
  - `<component>`
- New build required:
  - `<component>`
- Deferred:
  - `<component>`

## Open Questions

- `<question>`

## Decision Log

| Date | Decision | Reason |
| --- | --- | --- |
| `<YYYY-MM-DD>` | `<decision>` | `<reason>` |

