---
title: Causal chart patterns separate geometry selection, signal confirmation, and outcomes
type: pattern
area: classical-pattern research
date: 2026-07-17
tags: [backtesting, lookahead, chart-patterns, receipts]
refs: [src/research/classical_patterns/rectangle.py:138, src/research/classical_patterns/lifecycle.py:21, src/oracle/rectangle_trade_simulator.py:51, src/research/classical_patterns/runner.py:117, 16b65f3]
---

# Causal Chart Patterns Separate Selection, Signal, and Outcome

## Context

The first Classical Pattern Lab slice turned a visually discretionary daily
rectangle into deterministic enumeration, Type 1–4 lifecycle classification,
and next-open trade simulation.

## What We Learned

A causal chart-pattern backtest needs three clocks that cannot be collapsed:

1. select the pattern geometry from bars ending before the signal bar;
2. confirm the signal using the completed signal bar;
3. classify outcomes and simulate fills only from the next tradable session.

The complete population and every artifact must then reconcile under an
authoritative receipt. Visual plausibility or a green P&L is not a substitute.

## Why / When It Applies

If the breakout bar is allowed to choose among competing boxes, the detector
quietly adapts its boundary to the observed breakout. If pre-confirmation highs
or lows are treated as post-signal outcomes, a path that did not exist yet can
cancel or award a prospective trade. If gaps are processed after a full daily
range, MFE/MAE borrow prices observed after the position already exited.

This applies to any close-confirmed daily pattern: rectangles, triangles,
channels, flags, or head-and-shoulders.

## Specifics

- `rectangle.py` scores qualifying geometries before reading the breakout
  close. The audit retains the number of qualifying alternatives; the breakout
  cannot promote a more convenient boundary.
- Breakout-bar LFD, negation, or objective contacts are diagnostic codes. They
  neither manufacture Type 1–4 outcomes nor veto the next-open trade.
- `lifecycle.py` starts retrospective classification at `breakout_index + 1`.
- `rectangle_trade_simulator.py` enters at the next actual open, handles an
  opening gap before the session range, and uses stop-first ordering for an
  otherwise unknowable same-bar stop/target collision.
- `runner.py` fails closed unless scanned windows and candidate clusters
  reconcile, and hashes the bytes of the artifacts actually written.

## Apply It Next Time

Before adding a new pattern, require prefix-invariance, future-poison, mirrored
long/short, same-bar ambiguity, next-open gap, population-identity, and actual
artifact-hash tests. If a rule cannot state which of the three clocks owns a
field, it is not frozen enough to backtest.

## Dead Ends

- Evaluating every qualifying geometry against the breakout and retaining the
  ones it clears looks comprehensive, but lets the signal bar select the box.
- Treating the breakout candle's full range as a post-breakout path mixes
  pre-confirmation price action with prospective outcomes.
- Hashing a DataFrame's CSV rendering while writing Parquet proves a different
  payload than the receipt names.
