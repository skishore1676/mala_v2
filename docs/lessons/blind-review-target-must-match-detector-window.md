---
title: Blind semantic review must match the detector's causal target
type: gotcha
area: classical-pattern semantic calibration
date: 2026-07-17
tags: [causality, blind-review, chart-patterns, calibration]
refs: [src/research/classical_patterns/review.py:863, src/research/classical_patterns/review.py:2273, tests/test_classical_pattern_review.py:491]
---

# Blind Semantic Review Must Match the Detector's Causal Target

## Context

The mixed rectangle-calibration packet hid detector classes correctly, but its
first public chart showed up to 81 sessions while each hidden class described
one exact 20, 40, or 60-session candidate.

## What We Learned

Outcome blindness is not enough. A reviewer and a detector must judge the same
causal object. If the reviewer sees a wider chart, they can select a different
structure and produce a reasonable label that is nevertheless incomparable to
the machine label.

## Why / When It Applies

This applies whenever a model evaluates a bounded proposal inside richer
context: chart patterns, event windows, document spans, image regions, or time
series segments. Extra context is safe only when the target itself is marked
unambiguously and the rubric says whether surrounding evidence may redefine it.

## Specifics

The initial packet showed 81 raw bars to avoid presenting fixed windows as
natural bases. Two blind reviewers then disagreed on 5 of 18 strict labels; in
several cases they explicitly described a longer structure outside the hidden
candidate window. The repair made each public SVG contain exactly
`lookback_sessions + 1` bars and described it neutrally as a candidate window,
not a machine verdict.

The verifier now rejects any chart whose metadata bar count differs from the
candidate window, and the regression test proves that 20/40/60-session detector
windows render as 21/41/61 bars including the evaluation cutoff.

## Apply It Next Time

Before trusting reviewer agreement, compare these three identities:

1. the machine object's start and end;
2. the visible review target's start and end;
3. the wording that tells the reviewer what may be relabeled.

Hash and verify all three. If richer context is needed, mark the target window
explicitly instead of asking the reviewer to infer which structure is under
review.

## Dead Ends

Hiding the fixed lookback and showing a generic longer chart felt less leading,
but it changed the question. Neutral presentation must hide the verdict, not the
identity of the object being adjudicated.
