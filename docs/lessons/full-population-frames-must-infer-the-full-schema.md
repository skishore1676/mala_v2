---
title: Full-population frames must infer the full schema
type: bug
area: classical-pattern research runner
date: 2026-07-17
tags: [polars, population, serialization, scale]
refs: [src/research/classical_patterns/runner.py:330, tests/test_classical_rectangle_lab.py, c3eccbe]
---

# Full-Population Frames Must Infer the Full Schema

## Context

The rectangle runner passed synthetic fixture tests, then serialized a real
43-symbol calibration population containing more than 38,000 audit rows.

## What We Learned

Schema inference over a default sample is unsafe for sparse audit columns. A
field can be null throughout the sampled prefix and become a string later in
the complete deterministic population.

## Why / When It Applies

This occurs when rejection/audit records share one row contract but populate
diagnostic fields only for certain reasons or directions. Small fixtures and
the first sampled rows may never exercise the non-null type.

## Specifics

The first complete-population run failed with:

```text
polars.exceptions.ComputeError: could not append value: "short" of type: str to the builder
```

`runner._frame()` normalized nested values correctly, but `pl.DataFrame()` used
Polars' bounded inference sample. The fix is
`pl.DataFrame(normalized, infer_schema_length=None)`, which infers across the
entire already-materialized population. A regression test places 101 nulls
before the first string so a default 100-row inference sample would fail.

## Apply It Next Time

When an in-memory research population is already fully materialized and has
sparse heterogeneous fields, infer its schema across all rows or provide an
explicit schema. Add a scale-shaped test whose first non-null value appears
after the library's default sample boundary.

## Dead Ends

- Treating a passing small fixture as proof that large audit serialization has
  the same schema behavior.
- Increasing the sample slightly; another sparse field can still appear later.
