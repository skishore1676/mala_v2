---
title: A semantically faithful detector can still have no economic edge
type: pattern
area: classical-pattern research governance
date: 2026-07-17
tags: [semantic-review, holdout, negative-results, backtesting]
refs: [docs/CLASSICAL_PATTERN_PUBLIC_VALIDATION.md, src/research/classical_patterns/public_validation_analysis.py, ac66341, 4f1f9be]
---

# A Semantically Faithful Detector Can Still Have No Economic Edge

## Context

Rectangle v1 passed an independent outcome-blind source-fidelity gate, then ran
unchanged across a hash-bound five-year Public dataset with separate
calibration, validation, and holdout periods.

## What We Learned

Semantic fidelity and economic edge are orthogonal gates. A review can prove
that the machine consistently implements the frozen doctrine without providing
any evidence that the doctrine makes money. Once an untouched holdout fails,
the honest artifact is a durable negative result, not a list of threshold ideas
derived from the failure.

## Why / When It Applies

This applies whenever an external playbook is translated into deterministic
rules. Outcome-blind review should answer whether the implementation matches the
agreed meaning. The complete-population backtest should answer whether that
meaning has economic value. Letting chart reviewers curate events or letting a
failed holdout trigger in-place retuning collapses those questions and creates
hindsight bias.

## Specifics

- The frozen detector emitted 85 signals across 37 of 43 symbols.
- Validation shorts averaged `+0.162 R` and `+0.118 R`, but holdout shorts
  reversed to `-0.388 R` and `-0.394 R`.
- Validation longs were negative; holdout longs were approximately flat.
- Combined holdout expectancy was negative for both predeclared variants:
  `-0.147 R` and `-0.163 R`.
- All validation and holdout directional 95% trade-level bootstrap intervals
  crossed zero. Zero directional cells replicated positive.
- The robustness report's 20-trade evidence floor was created after the run and
  is therefore labeled descriptive, not retroactively called a predeclared
  gate. The sign reversal already establishes the replication failure.

The reusable artifact chain is:

1. semantic freeze with no accepted-signal allowlist;
2. frozen universe and data manifest;
3. complete-population run receipt;
4. validation-to-holdout replication scorecard;
5. a verdict that forbids retuning the consumed holdout.

## Apply It Next Time

When a playbook passes semantic review, do not say it has an edge. Run every
emitted event, read validation and holdout before calibration, and require the
effect to retain direction across both. If it fails, mark that version closed.
A proposed geometry, entry, or exit change becomes a new version with fresh
calibration and future holdout—not a repair to the failed result.

## Dead Ends

- Treating reviewer agreement as trade selection evidence.
- Promoting one favorable validation slice while its holdout reverses.
- Calling a post-run sample-size heuristic a predeclared gate.
- Removing losing symbols or directions after inspecting their contribution.
