# Rectangle Exit Experiment V2 — Revisit Entry Point

State: `CLOSED_NO_OUT_OF_SAMPLE_EDGE`
Closed: 2026-07-18
Execution authority: none

## Read in this order

1. `REPORT.md` — concise design, scorecard, verdict, and limitations.
2. `experiment_receipt.json` — authoritative hashes, population identities,
   selection procedure, and exact machine verdict.
3. `exit_scorecard.csv` — eight optimization/OOS variant cells.
4. `exit_slice_scorecard.csv` — direction and baseline-versus-80-only slices.
5. `paired_exit_comparisons.csv` — Range Expansion minus rectangle-height
   paired differences.
6. `signals_v2.csv` — all 96 emitted signal identities.
7. `exit_trades.csv` — complete 96 by 4 trade-variant ledger.

Canonical narrative:
`mala_v2/docs/CLASSICAL_PATTERN_EXIT_EXPERIMENT_V2.md`

Reusable lesson:
`mala_v2/docs/lessons/semantic-fidelity-is-not-economic-edge.md`

## Exact footprint

- Implementation commit:
  `976def49b407d15e323f4df5474362bbbfc9d588`
- V2 config hash:
  `d8fb8c6c41126a9f9ca70a785701b4b428b7fe2cf83bcb74ac0262c333fe9079`
- Source dataset manifest hash:
  `333d9cf9ed65aa527307eae377986fc79206933d51a877fc4b885c77791b27b0`
- Source daily-bars hash:
  `8db73647adf4c32af3460f47f6dbc3c052ca8367697c8411ae9030eb0e4a8333`
- Signals: 96 total; 85 preserved baseline plus 11 new 80-session events.
- Analysis cohorts: 63 optimization, 33 OOS, zero boundary purges.
- Selected procedure: `rectangle_height_lfd_buffer_0p00atr`.
- Selected OOS result: `-0.204607R` per signal; profit factor `0.647125`;
  symbol-cluster interval `[-0.535116, +0.134838]`.

The source daily Parquet remains in the hash-bound version-1 local result at:

`research/results/playbooks/classical_pattern_lab/public_validation_round_1/economic_public_43_v1/daily_bars.parquet`

It is intentionally not duplicated into this terminal bundle. If that source
file is unavailable, do not claim a byte-identical replay. A reacquired dataset
must receive a new output version unless its hash matches exactly.

## Reproduction

Use a new output directory; never overwrite this evidence:

```bash
./.venv/bin/python -m src.research.classical_patterns.exit_experiment_v2 \
  --output-dir research/results/playbooks/classical_pattern_lab/rectangle_exit_experiment_v2/revisit_<date>
```

The runner requires a clean Git tree and binds the exact implementation,
config, source receipt, source bars, and every generated artifact by hash.

## Valid reasons to reopen

- New forward data after 2026-07-18.
- A genuinely new point-in-time dataset.
- Complete prospective human take/pass consultation logs.
- A separately sourced classical-pattern hypothesis.

Adding another exit, optimizing a subgroup, or re-reading the same 2021–2026
bars is not a valid reopen condition.
