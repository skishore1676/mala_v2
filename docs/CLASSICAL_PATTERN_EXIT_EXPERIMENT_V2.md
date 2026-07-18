# Classical Rectangle Exit Experiment V2

Status: implemented and economically negative on the frozen Public 43-symbol
cohort. This is local research only and is not executable.

## Decision

The frozen rectangle entry does **not** show an autonomous underlying-trading
edge under either the rectangle-height exit or the daily Range Expansion exit
analogue. The bounded optimizer selected the original raw-LFD,
rectangle-height policy through 2024, but that policy produced `-0.205R` per
emitted signal with profit factor `0.65` from 2025 onward. Both long and short
OOS slices were negative, and all four predeclared variants lost OOS.

Move on from further exit tuning on this dataset. The detector may still be
used as an explicitly unproven discretionary consultation signal if every
future alert and operator decision is logged prospectively. It must not be
described as validated alpha or promoted into autonomous shadow/live trading.

## Question

Does the already-aligned deterministic daily rectangle entry have positive net
underlying expectancy after a small, predeclared exit comparison?

The experiment intentionally does not ask whether an option vehicle, an agent
curator, or additional discretionary filters can rescue the result. Those are
different hypotheses.

## Frozen entry definition

- Daily close-confirmed horizontal rectangle breakout.
- Lookbacks: 20, 40, 60, and 80 sessions.
- The version-2 extension preserves every version-1 20/40/60 representative.
- An 80-session candidate adds a signal only when no version-1 event exists on
  the same symbol, breakout date, and direction.
- Entry remains the next session's open.
- No human or model outcome review filters the economic population.
- The operator explicitly waived another Obsidian semantic-card loop for the
  80-session extension after accepting the established deterministic geometry.

The 80-session extension added 11 new events without removing or rewriting any
of the 85 version-1 signals, producing 96 total signals.

## Exit variants

Exactly four variants were evaluated:

1. Rectangle-height objective with raw LFD stop.
2. Rectangle-height objective with LFD minus/plus `0.10 ATR` stop.
3. Daily Range Expansion analogue with raw LFD stop.
4. Daily Range Expansion analogue with LFD minus/plus `0.10 ATR` stop.

The Range Expansion analogue retains LFD-defined initial risk, banks 40% at
`+1R`, moves the runner stop to breakeven beginning with the next daily bar,
targets `+2R` on the remainder, applies the LOOSE prior-bar high-water
giveback, and closes by 20 sessions. It is not the minute-level option-premium
profile and makes no option-alpha claim.

Both simulators include five basis points of adverse slippage on each side and
two basis points of explicit round-trip cost. Daily same-bar stop/target
ambiguity is resolved conservatively with the stop active at the start of the
bar taking priority.

## Experimental design

- Source bars: 43 symbols, 53,922 symbol-session rows, 1,254 sessions per
  symbol, 2021-07-19 through 2026-07-16.
- Optimization: breakout dates through 2024-12-31, 63 signal episodes.
- OOS: breakout dates from 2025-01-01 onward, 33 signal episodes.
- A signal whose variants cross the optimization/OOS boundary is purged from
  both periods; none did in this run.
- Primary selector: mean net R per emitted signal. A no-trade opportunity
  contributes zero rather than disappearing from the denominator.
- Selection tie-breakers: symbol-clustered 95% lower bound, maximum drawdown,
  then stable variant identifier.
- The winning optimization procedure is read once on OOS.
- Confidence intervals resample symbols as clusters with 10,000 deterministic
  bootstrap draws.

## Results

| Period | Variant | Signals | Mean net R/signal | Profit factor | Symbol-clustered 95% interval |
|---|---|---:|---:|---:|---:|
| Optimization | Range Expansion, raw LFD | 63 | -0.058 | 0.88 | [-0.289, +0.163] |
| Optimization | Range Expansion, 0.10 ATR | 63 | -0.111 | 0.78 | [-0.322, +0.094] |
| Optimization | Rectangle height, raw LFD | 63 | -0.013 | 0.97 | [-0.255, +0.248] |
| Optimization | Rectangle height, 0.10 ATR | 63 | -0.015 | 0.97 | [-0.247, +0.231] |
| OOS | Range Expansion, raw LFD | 33 | -0.237 | 0.63 | [-0.601, +0.140] |
| OOS | Range Expansion, 0.10 ATR | 33 | -0.223 | 0.65 | [-0.578, +0.149] |
| OOS | Rectangle height, raw LFD | 33 | **-0.205** | **0.65** | **[-0.535, +0.135]** |
| OOS | Rectangle height, 0.10 ATR | 33 | -0.213 | 0.63 | [-0.524, +0.109] |

The Range Expansion analogue reduced average R relative to the matching
rectangle-height policy in both periods and under both stop buffers. For the
raw stop it trailed by `-0.045R` per signal in optimization and `-0.033R` OOS.

The selected raw-LFD baseline was not hiding one favorable side:

- Optimization long: `-0.081R`; short: `+0.058R`.
- OOS long: `-0.125R`; short: `-0.326R`.
- 2025: `-0.366R` across 17 signals.
- 2026 through July 16: `-0.033R` across 16 signals.

The 80-only cohort appeared favorable in optimization (`+0.458R` across nine
raw-LFD signals) but reversed to `-1.175R` across its two OOS signals. Two OOS
events cannot estimate an 80-session effect, and the reversal is a warning not
to promote the favorable development slice.

## Data and calculation validation

- Source receipt hashes bind the exact bars and version-1 signals.
- Replaying version 1 reproduces all 85 source signal identifiers exactly.
- Version 2 preserves every version-1 event and representative identity.
- Every incremental event uses the 80-session lookback.
- All 96 signals have exactly four unique variant rows.
- Daily data has no required nulls, duplicate symbol/date keys, invalid OHLC
  relationships, non-finite numeric cells, missing symbols, or unequal session
  counts.
- No optimization signal outcome crosses into OOS.
- Headline metrics are calculated at signal opportunity grain rather than by
  multiplying the population with exit variants.

## Limitations

- The universe is a frozen current-symbol cohort, not a point-in-time market
  universe, so survivorship limits population claims.
- Public adjustment continuity was empirically checked, but the provider's
  adjustment policy is undocumented.
- The 2025+ bars were previously inspected in rectangle v1. They are useful
  secondary OOS evidence for the new procedure, not a pristine program-level
  holdout.
- The test is on the underlying. It cannot measure option convexity, theta, or
  IV-dependent value from the real Range Expansion option profile.
- The result falsifies this deterministic entry plus these bounded daily exits;
  it does not prove that every discretionary rectangle trade lacks value.

## Reproduction

```bash
./.venv/bin/python -m src.research.classical_patterns.exit_experiment_v2 \
  --output-dir research/results/playbooks/classical_pattern_lab/rectangle_exit_experiment_v2/public_43_v2
```

The ignored result directory contains `signals_v2.csv`, `exit_trades.csv`,
`exit_scorecard.csv`, `exit_slice_scorecard.csv`,
`paired_exit_comparisons.csv`, `REPORT.md`, and
`experiment_receipt.json`.

## Next action

Do not tune more rectangle exits against the consumed 2021-2026 history.
Either move to the next separately sourced Brandt pattern hypothesis, or use
the rectangle detector only as a prospective consultation surface. A
consultation lane must log every emitted signal, take/pass decision, stated
reason, and subsequent outcome so that any human-selection value is measured
rather than assumed.
