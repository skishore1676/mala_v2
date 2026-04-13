You are running inside the mala_v2 research workbench.
Read skills/research-workbench/SKILL.md first, then follow the protocol exactly.

## Your Task

Rerun the top options-primary strategy families after the 1-minute execution guardrails were added, then write a before/after comparison report.

Guardrails now configured in `config/hypothesis_defaults.yaml`:
- `entry_delay_bars: 1`
- `min_hold_bars: 2`
- `exit_evaluation_start_bar: 1`
- `cooldown_bars_after_signal: 5`

## Hypotheses To Rerun

1. `research/hypotheses/market-impulse-all-basket-discovery.md`
   - Baseline run to compare against: `data/results/hypothesis_runs/market-impulse-all-basket-discovery/2026-04-12T152932`
2. `research/hypotheses/jerk-pivot-current-basket-discovery.md`
   - Baseline run to compare against: `data/results/hypothesis_runs/jerk-pivot-current-basket-discovery/2026-04-12T182814`

## Steps

For each hypothesis:

1. Dry-run first:
   ```bash
   .venv/bin/python hypothesis_agent.py \
     --hypothesis <HYPOTHESIS_FILE> \
     --force-rerun \
     --dry-run
   ```
2. Run the full pipeline:
   ```bash
   .venv/bin/python hypothesis_agent.py \
     --hypothesis <HYPOTHESIS_FILE> \
     --force-rerun
   ```
3. Read the new run's:
   - RUN_SUMMARY.md
   - FINDINGS_REPORT.md, if present
   - CATALOG_SELECTED.csv
   - M5_execution.csv
   - m5_exit_optimizations.json
4. Compare against the listed baseline run artifacts.

## Required Output

Write one report:

`research/reports/top-family-guardrail-comparison.md`

It must cover:

- Which candidates survived before vs after guardrails
- Which candidates changed tier: promote / shadow / watch_only / kill
- Changes in single_option `mc_prob_positive_exp`, `mc_exp_r_p50`, `base_exp_r`, and `holdout_trades`
- Whether any prior candidate appears to have depended on same-bar/rapid-fire 1-minute behavior
- Whether Market Impulse and Jerk-Pivot are still the top two options-primary families
- Recommendation: continue, retune after M1, or pause for more execution-model hardening

In your final chat response, give the absolute path to `research/reports/top-family-guardrail-comparison.md`.

## Rules

- Do not retune during this comparison.
- Do not use v1 results as evidence.
- Do not skip gates.
- Do not only print findings in chat. Save the report to disk.
