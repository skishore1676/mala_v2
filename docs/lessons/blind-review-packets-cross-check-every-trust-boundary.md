---
title: Blind review packets must cross-check every trust boundary
type: pattern
area: classical-pattern semantic review
date: 2026-07-17
tags: [semantic-review, causality, receipts, integrity]
refs: [src/research/classical_patterns/review.py, src/research/classical_patterns/source_fidelity.py:120, src/research/classical_patterns/readiness.py, tests/test_classical_pattern_review.py, 7fbef9b]
---

# Blind Review Packets Must Cross-Check Every Trust Boundary

## Context

The first Classical Pattern Lab semantic round needed reviewable charts that
ended at signal confirmation, hid every later outcome, and remained useful as
human decisions accumulated.

## What We Learned

Hashing artifacts is necessary but insufficient. Every transition between
readiness, manifest, receipt, chart metadata, editable response, and append-only
decision state is a separate trust boundary. A verifier must recompute hashes
and cross-check identities across those boundaries; trusting an unhashed copy
of a config hash or a pre-existing decision log can manufacture false
readiness even when the chart files themselves are intact.

## Why / When It Applies

This applies whenever immutable machine evidence is paired with a human-editable
review surface. The editable file cannot be part of the immutable packet hash,
but ingestion must bind each response to the immutable card/config identity and
must revalidate any previously appended state before producing a scorecard.

## Specifics

- `readiness.py` hashes the causal data audit and reloads it through an exact
  schema. Batch generation then requires the audited symbols and daily hashes
  to exactly match the supplied inputs.
- `review.py` hashes the manifest, immutable response template, cards, and SVGs;
  it also checks receipt identity/count fields against the hashed manifest and
  checks SVG metadata against each card's cutoff and source-slice hash.
- `review_responses.csv` stays editable, while
  `review_responses.template.csv` stays immutable and hashed.
- Decision ingestion validates enums, corrections, reviewer attestations,
  response identity, duplicate history, and the response ID of every existing
  JSONL record before it writes a semantic-only scorecard.
- A nonempty batch root is rejected. A retry receives a new root so stale cards
  cannot silently enter the receipt inventory.
- A new response schema also needs a new public card wrapper. The first V3
  overlay safely reused V2 chart bytes but linked to V2 Markdown that still
  asked for `strict rectangle validity` and `trade worthiness`. A blind
  reviewer caught the stale instruction. V3 now generates and hashes sanitized
  wrappers, binds each to the original chart/source-card hashes, and rejects
  historical V2 review language during verification.

## Apply It Next Time

Draw the trust-boundary chain before implementing the review surface. For every
arrow, add one mutation test: alter only the downstream identity, hash, path,
metadata cutoff, or prior decision record and require a fail-closed error. Keep
economic results in a separate module and add an import-boundary test.

## Dead Ends

- Verifying artifact hashes while trusting receipt identity fields.
- Recomputing a readiness hash but not proving that the batch uses those exact
  symbols and daily bytes.
- Treating an append-only JSONL file as inherently trustworthy on later reads.
- Reusing a prior review card merely because its chart remains causally valid;
  instructions are part of the public trust surface too.
