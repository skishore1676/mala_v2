---
name: playbook-builder
description: Use when starting or extending a Mala 2.2 playbook from trader intuition into a research surface, consultation desk, locked packet, and eventual Bhiksha/public execution lane.
---

# Playbook Builder

## Scope

Use this skill inside `mala_v2` when the user wants to create, extend, or
compare a playbook such as mean reversion, continuation, breakout, failed
breakout, trend day, or multi-day reversion.

This skill exists so new playbooks do not restart from first principles. It
captures the durable lessons from the first Mala 2.2 playbook:

```text
trader thesis
-> feature/search feasibility
-> historical surface
-> chart review
-> state-management consultation
-> consultation journal
-> locked packet
-> locked stress/parity
-> Bhiksha/public execution preview
-> approval-gated management
-> feedback ingestion
```

Do not collapse this into the old autonomous strategy promotion path. A
playbook starts as a trader-bias consultation surface. Automation is a later
gate, not the default product.

## First Files

Read only what the task needs:

1. `agent.md`
2. `docs/MALA_VISION_v2.2.md`
3. `docs/PLAYBOOK_CONSULTATION_LAYER.md`
4. `docs/PLAYBOOK_AUTOMATION_GATES.md`
5. `research/playbooks/TEMPLATE.md`
6. the most similar existing playbook under `research/playbooks/`

For the current reference implementation, use:

- `research/playbooks/mean_reversion_at_extremes_intraday_v1.md`
- `src/strategy/intraday_mean_reversion.py`
- `src/research/playbook_surface.py`
- `src/research/playbook_surface_query.py`
- `src/research/playbook_policy_card.py`
- `src/research/playbook_consultation_log.py`

## Operating Principles

- Start from trader language, not from a strategy class.
- The trader owns the play definition: thesis, horizon, visual pattern,
  invalidation language, and management intent.
- Mala owns the measurement: historical analogs, parameter surface, outcome
  accounting, policy card, and replay/live journal.
- Bhiksha/public own execution: option selection, spread/risk checks, order
  lifecycle, protective management, and broker reconciliation.
- Do not promote a broad surface. Lock one packet only after chart and
  consultation evidence show the math matches the intended trade.
- Do not let chart hindsight be the only proof. The consultation journal is the
  feedback loop for whether Mala helped before outcome was known.
- Keep playbook output trader-readable: `READ`, `STATE`, `ANALOG`, `POLICY`,
  `EXIT`, `STOP`, `WATCH`.

## Builder Loop

### 1. Capture The Trader Thesis

Ask only for missing semantics. Useful inputs:

```text
play name
directional bias
time horizon
asset scope
when the setup is usually considered
what the chart must look like
what invalidates the idea
how the trader naturally wants to manage it
```

Do not ask the user to hand-pick every numeric threshold if Mala can search it.

### 2. Classify Build Feasibility

Before code, classify the playbook:

- `config-only`: existing strategy/search surface can express it.
- `new-class`: needs a new strategy event constructor.
- `new-feature`: needs Newton/Oracle feature work.
- `new-execution-contract`: needs Bhiksha/public trade-intent support.

Record what is reusable and what must be built.

### 3. Draft The Playbook Spec

Create a markdown playbook from `research/playbooks/TEMPLATE.md`.

The draft must separate:

- thesis
- horizon
- operator language
- natural features
- entry families
- context/stage families
- invalidation families
- management/exit families
- surface grid
- consultation contract
- execution-packet criteria
- open questions

### 4. Reuse The Existing Spine

Prefer existing infrastructure:

- Newton for reusable market features.
- Strategy event constructor for playbook-specific event rows.
- Oracle/trade simulator for bar-by-bar outcomes, MFE/MAE, R math, and exits.
- `playbook_surface` for calibration/holdout surfaces.
- `playbook_surface_query` for state-management analog queries.
- `playbook_policy_card` for deterministic desk cards.
- `playbook_consultation_log` for replay/live feedback.
- `PLAYBOOK_AUTOMATION_GATES` for promotion beyond consultation.

Add new generic hooks only when the second playbook proves the first-playbook
code is too narrow.

### 5. Build The Historical Surface

The first run answers:

```text
Where does this trader-supplied play historically work, partially work, or fail?
```

It should not answer:

```text
What strategy did the machine discover?
```

Artifacts should include receipt, conditional surface, feature bins, sample
events, and surface review. Favor candidate taxonomy over raw holdout sorting.

### 6. Do Chart Review

Use TradingView/thinkorswim-facing artifacts, not synthetic charts as the final
review surface.

Chart review asks:

- Do these events look like the intended play?
- Are entries too late, too early, or semantically wrong?
- Are exits realistic for the trader's vehicle?
- Are the favorable/partial/outside regions intuitive enough to continue?

### 7. Add Consultation

Every playbook that survives chart review needs a state-management lane.

The query should retrieve nearest historical analogs for a trader-supplied
timestamp and bias. It should not merely report whether a rule fired.

The policy card should stay compact:

```text
READ:    verdict, confidence, cohort size
STATE:   playbook-relevant percentile context
ANALOG:  cohort quality and similarity tail
POLICY:  take/pass/wait/out-of-scope rule
EXIT:    selected or best management row
STOP:    paired risk reference
WATCH:   horizon decay or caveat
```

### 8. Journal Before Automation

Require 8-12 closed replay/live consultation rows before locking a packet unless
the user explicitly accepts a smaller sample as a scout.

Closed rows should include:

- take and pass decisions
- card agreements and disagreements
- realistic operator notes
- actual outcomes from replay-close or live close

### 9. Lock One Packet

A locked packet is one narrowed playbook variant, not the whole surface.

It must define:

- exact feature constraints
- trigger
- risk stop
- thesis invalidation
- management policies
- time/horizon rules
- execution vehicle assumptions
- what Bhiksha/public must compute live

### 10. Promote Through Playbook Gates

Use `docs/PLAYBOOK_AUTOMATION_GATES.md`.

The playbook lane is:

```text
surface gate
-> locked validation gate
-> parity gate
-> shadow execution gate
-> live approval gate
-> automation gate
```

The automation gate remains blocked until feedback ingestion and a separate
autonomous-control approval exist.

## Common Pitfalls

- Starting from a broad grid before the play is semantically defined.
- Treating a favorable surface row as live authorization.
- Ranking by holdout expectancy alone.
- Mixing chart hindsight with forward decision evidence.
- Letting `wait_no_trigger` become the operator product.
- Using options approximations before the underlying consultation question is
  useful.
- Making Bhiksha guess Mala's thesis from loose notes.
- Carrying old Strategy_Catalog/active_strategy assumptions into Mala 2.2.

## When To Bring The User In

Bring the user in for:

- play definition and chart-language semantics
- whether sample events match the intended setup
- whether management rows are realistic for their options vehicle
- take/pass decisions in consultation replay
- approval to lock a packet or move toward execution preview

Do not interrupt for:

- exact cutoffs that the grid can search
- artifact wiring that follows existing patterns
- generic test/documentation updates
- routine reruns and receipt regeneration

