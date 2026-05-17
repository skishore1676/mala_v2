# Agent Handoff: Volume Mismatch Retune Pass

**Status:** closed / superseded by the 2026-05-17 refactor cleanup. Do not run
this as a current operating prompt; its report and legacy replay machinery are
archived under `research/reports/archive/`.

You are working in `/Users/suman/code/mala_v2`.

Read these first:

```text
agent.md
research/reports/archive/legacy_runtime_20260517/provider-volume-mismatch-audit-2026-05-03.md
data/results/provider_volume_parity/20260503T_volume_mismatch_baseline/PROVIDER_VOLUME_PARITY_REPORT.md
```

Do not write to Google Sheets. Do not promote or demote candidates. This is a
research-only pass to determine whether changing volume logic makes Mala
candidates more robust to Bhiksha's runtime data provider.

## Question

Can we replace fragile 1-minute volume usage with provider-invariant normalized
volume logic, especially 3-minute or 5-minute aggregated relative volume, without
killing the M5 candidates we care about?

## Required Work

1. Inventory all M5-promoted or shadow-eligible rows that use explicit volume
   gates or volume-derived features.
2. Separate these cases:
   - explicit volume gate, such as `volume >= volume_multiplier * volume_ma_N`
   - relative-volume gate, such as `relative_volume_N`
   - volume inside VWMA/regime construction
   - VPOC/directional-mass volume dependence
3. For each affected row, use provider parity artifacts to classify provider
   risk by symbol:
   - low: gate flip rate under 3%
   - medium: gate flip rate 3% to 7%
   - high: gate flip rate over 7%
4. Run targeted replays for candidate-safe alternatives:
   - volume filter off
   - normalized relative-volume gate
   - 3-minute aggregated relative-volume gate
   - 5-minute aggregated relative-volume gate
5. Compare against the original M5 evidence using:
   - signal count
   - entry overlap
   - trade overlap
   - expectancy
   - profit factor
   - win rate only as supporting context
6. Do not choose the best-looking retune by backtest alone. Prefer the smallest
   change that preserves edge while reducing provider flip risk.

## Inputs

Provider parity artifact:

```text
data/results/provider_volume_parity/20260503T_volume_mismatch_baseline/provider_relative_volume_parity.csv
```

Volume sensitivity machinery:

```text
research/reports/archive/scripts/legacy_forensics_20260517/catalog_volume_sensitivity.py
research/reports/archive/scripts/legacy_forensics_20260517/catalog_volume_sensitivity_test_legacy.py
```

Mala/Bhiksha replay and runtime EV machinery:

```text
src/research/bhiksha_signal_ev.py
tests/test_bhiksha_signal_ev.py
```

## Output

Write a report to:

```text
research/reports/archive/legacy_runtime_20260517/volume-mismatch-retune-findings.md
```

Use this structure:

```text
# Volume Mismatch Retune Findings

## Data Availability
## Candidate Inventory
## Provider Risk By Symbol
## Replay Results
## Rows That Survive
## Rows To Avoid In Shadow
## Smallest Code Change Recommended
## Open Questions

Verdict:
- Should Mala add a 3m/5m normalized volume feature? yes/no/partial
- Which strategy families benefit?
- Which symbols remain unsafe?
- What should Bhiksha change, if anything?
```

## Guardrails

- Do not use provider divergence results as alpha validation.
- Do not tune after looking at live PnL and then treat that as clean evidence.
- Do not recommend Polygon live data unless the report shows the strategy cannot
  be made provider-invariant and is still important enough to justify API cost.
- If a row only survives because a volume gate is removed, label that clearly as
  a new variant requiring a fresh M1-M5 pass.
