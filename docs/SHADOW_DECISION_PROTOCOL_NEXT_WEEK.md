# Mala/Bhiksha Shadow Decision Protocol - Next Week

## Purpose

This is not an open-ended experiment. Next week is a decision gate for whether
Mala-derived options strategies deserve more capital, a provider-path change,
or a strategy redesign.

The goal is to answer three questions with daily evidence:

1. Is Bhiksha adopting Mala correctly enough that the shadow data is
   trustworthy?
2. Is provider or broker divergence large enough that the live data/execution
   path must change?
3. When Mala and Bhiksha agree on a signal, does the realized options result
   support Mala expectancy?

## What Research Ops Owns

Research Ops should run the daily evidence loop and publish a human-readable
brief without waiting for Suman:

- sync/compile the active plan from Google Sheets
- run Bhiksha observation review
- run Mala shadow daily report
- run Mala/Bhiksha signal EV and counterfactual replay
- publish an Obsidian decision brief
- summarize root causes and recommend the owner: Bhiksha plumbing, provider
  contract, broker/execution, or strategy/exit

Suman should only need to read the Obsidian brief and decide whether to kill,
continue, pause, or escalate.

## Daily Output

The daily OpenClaw cron writes:

- Mala raw reports under `data/results/research_ops/`
- Obsidian brief under
  `/Users/sunny/Library/Mobile Documents/iCloud~md~obsidian/Documents/northstar/areas/trading/mala-shadow/YYYY-MM-DD.md`
- copied report artifacts under
  `areas/trading/mala-shadow/attachments/YYYY-MM-DD/`

## Decision Gates

### Gate 1: Adoption/Plumbing

Green:

- same-bar Mala replay match rate is at least 95%
- true signals are through active-plan Mala-sourced deployments
- exits, trade plans, and shadow lifecycle are recorded
- no repeated runtime/lifecycle errors

Yellow/red:

- same-bar replay mismatches are repeated
- counterfactual misses are `no_runtime_evaluation_observed`
- active-plan rows lack sheet-sourced exit contracts
- shadow cannot reconstruct entry/exit/PnL evidence

Action: fix Bhiksha/Mala plumbing before judging strategy.

### Gate 2: Provider/Broker

Provider issue:

- repeated `provider_feature_mismatch*` rows on traded deployments
- volume/VPOC/directional-mass features explain false positives or misses
- same strategy behaves differently only because live feature source differs

Action: move Mala-derived live feature computation to Polygon or pause
provider-sensitive rows. Do not change broker first unless quote/fill/lifecycle
evidence, not feature computation, is the blocker.

Broker issue:

- option quotes/fills are stale or unavailable
- order lifecycle cannot be managed deterministically
- fills/spreads materially violate assumptions after signal parity is clean

Action: evaluate broker change only after provider-feature parity is clean.

### Gate 3: Strategy/Expectancy

Green:

- clean matched signals have positive average realized option R
- PnL family agrees with Mala expectancy
- exits do not systematically cut winners or hold losers

Red:

- at least 20 clean matched closed trades and negative average realized R
- adverse trades versus evidence exceed positive trades by more than 2:1
- stock-side MFE/MAE looks fine but options PnL fails, implying option overlay
  or exit translation is broken

Action: stop plumbing work for that family and redesign the exit/strategy
thesis.

## Suman's Daily Protocol

1. Read the Obsidian brief.
2. Look only at the recommendation and root-cause table first.
3. If owner is plumbing/provider, let agents fix or investigate.
4. If owner is strategy/exit, decide whether to pause, retune, or kill the
   family.
5. Do not manually inspect raw logs unless the brief cannot assign ownership.

## End-of-Week Decision

At the end of next week, each active family gets one of four outcomes:

- continue shadow: plumbing clean, sample still small
- pause provider-sensitive rows: mismatch remains provider-driven
- redesign strategy/exit: clean matched trades lose money
- promote-review candidate: clean adoption, enough sample, positive option R,
  and no unresolved runtime defects

Promotion review is allowed only after the first three questions are cleanly
answered.
