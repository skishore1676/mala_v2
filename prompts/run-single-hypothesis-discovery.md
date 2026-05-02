You are running inside the mala_v2 research workbench.
Read skills/research-workbench/SKILL.md first, then follow the protocol exactly.

## Your Task

Run exactly one hypothesis end-to-end through M1-M5 and write a findings report into the run artifact directory.

The human will provide the hypothesis file path after this prompt. If no hypothesis file path is provided, stop and ask for one.

## Steps

1. Read agent.md and skills/research-workbench/SKILL.md.
2. Read the provided hypothesis file.
3. Dry-run first to confirm data availability and config count:
   ```bash
   .venv/bin/python hypothesis_agent.py \
     --hypothesis <HYPOTHESIS_FILE> \
     --force-rerun \
     --dry-run
   ```
4. Run the full pipeline:
   ```bash
   .venv/bin/python hypothesis_agent.py \
     --hypothesis <HYPOTHESIS_FILE> \
     --force-rerun
   ```
5. After completion, read the hypothesis file. It will be updated with the run artifact path.
6. Read the key artifacts from the run directory:
   - RUN_SUMMARY.md
   - M1_top.csv
   - M2_gate_report.csv
   - M4_holdout.csv
   - M5_execution.csv
   - CATALOG_SELECTED.csv, if present
   - m5_exit_optimizations.json, if present
   - per-candidate m5_exit_optimization_<ticker>_<direction>_<hash>.json files, if present
7. Write `FINDINGS_REPORT.md` in the run directory. It must cover:
   - Which tickers and directions survived to M5
   - What parameter configuration was selected
   - Expectancy, confidence, signal_count, execution_robustness from M5
   - CATALOG_SELECTED recommendation_tier and exit_reliability
   - Selected exit policy and parameters, with trade-count caveats
   - Regime breakdown: does the edge concentrate in specific vix_band or spy_trend_20d?
   - Recommendation: publish to Mala_Evidence_v1, shadow, retune, or kill
8. In your final chat response, summarize the decision and give the absolute path to `FINDINGS_REPORT.md`.

## Rules

- Do not skip gates. A pass at M1 is not a pass at M4.
- Do not use v1 catalog results as evidence. Run fresh.
- Do not only print the findings in chat. Save them to `FINDINGS_REPORT.md`.
- Regime tags are observational. Do not use them to explain away a failed gate.
- If any stage fails, stop, update the hypothesis file, and write the findings report explaining the failure.
