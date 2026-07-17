---
title: External playbook tests must separate the sponsor from the setup curator
type: decision
area: classical-pattern research governance
date: 2026-07-17
tags: [semantic-review, external-doctrine, backtesting, anti-curation]
refs: [research/playbooks/classical_rectangle_breakout_daily_v0.md, docs/CLASSICAL_PATTERN_SEMANTIC_REVIEW.md, config/classical_patterns/rectangle_daily_v1.yaml:44, src/research/classical_patterns/runner.py:48]
---

# External Playbook Tests Must Separate the Sponsor From the Setup Curator

## Context

The Classical Pattern Lab was modeled after Mala's Flywheel review loop. That
worked for Suman's own trades because he was the source of the setup semantics.
It failed as a role assignment when the hypothesis became Peter Brandt's public
method: the Round 2 card asked Suman whether individual charts were tradeable
even though he does not trade that style.

## What We Learned

The sponsor of an external-playbook experiment is not automatically the
semantic authority for that playbook. Keep four roles distinct:

1. cited public evidence constrains the doctrine;
2. outcome-blind reviewers audit whether the machine faithfully encodes it;
3. deterministic code enumerates the full event population and measures it;
4. the sponsor decides whether the evidence justifies continuing or promotion.

A human or model `trade`, `watch`, or `no_trade` opinion is not a profitability
label. It must never filter, weight, or optimize the economic population.

## Why / When It Applies

This applies whenever Mala tests a method attributed to another trader, book,
paper, or public strategy. It does not replace the Flywheel contract for
Suman's own trades, where Suman legitimately owns the intended setup meaning.

The distinction prevents two subtle failures: asking an unqualified sponsor to
invent the external trader's discretion, and allowing a persuasive reviewer to
become a hidden signal model without out-of-sample economic proof.

## Specifics

- `config/classical_patterns/rectangle_daily_v1.yaml` already sets
  `human_review_may_filter_economics: false`.
- `run_research()` enumerates signals and simulates trades directly from bars
  and frozen config; it does not load semantic-review decisions.
- Round 2's `as_of_trade_worthiness` remains in the immutable V2 artifact for
  provenance, but is deprecated as doctrine and economic input.
- Source ambiguity can block a claim of Brandt fidelity. It does not authorize
  selecting whichever interpretation has the best backtest.

## Apply It Next Time

Before publishing a review card, ask: "Who actually owns the truth this field
claims to capture?" If the answer is an external source, give reviewers a cited
rubric and ask about fidelity. Ask Suman only about scope, risk, promotion, or
capital—not whether he personally likes each historical chart.

## Dead Ends

Copying the Flywheel's operator-curation step unchanged looked natural but
silently changed the research question from "does Peter's frozen method have
edge?" to "which Peter-like charts does Suman happen to like?"
