You are running inside the mala_v2 research workbench.
Read skills/research-workbench/SKILL.md first, then follow the protocol exactly.

## Your Task

Run the Market Impulse full-basket discovery hypothesis end-to-end through M1→M5 and write a findings report into the run artifact directory.

Hypothesis file: research/hypotheses/market-impulse-all-basket-discovery.md

## Steps

1. Read CLAUDE.md and skills/research-workbench/SKILL.md.
2. Read the hypothesis file above.
3. Dry-run first to confirm data availability and config count:
   ```
   .venv/bin/python hypothesis_agent.py \
     --hypothesis research/hypotheses/market-impulse-all-basket-discovery.md \
     --force-rerun \
     --dry-run
   ```
4. Run the full pipeline:
   ```
   .venv/bin/python hypothesis_agent.py \
     --hypothesis research/hypotheses/market-impulse-all-basket-discovery.md \
     --force-rerun
   ```
5. After completion, read the hypothesis file (it will be updated with the agent report).
6. Read the artifacts from the run directory printed in the agent report:
   - M1_top.csv — which symbols/directions passed M1 and with what parameters
   - M1_detail.csv — regime breakdown (vix_band, spy_trend_20d, session_type columns)
   - M4_holdout.csv — per-trade holdout results with regime tags
   - M5_execution.csv — execution mapping results
   - m5_exit_optimizations.json — index of selected exit policies per promoted catalog candidate
   - m5_exit_optimization_<ticker>_<direction>_<hash>.json — selected exit policy and candidate grid for each promoted catalog candidate
7. Write `FINDINGS_REPORT.md` in the run directory. It must cover:
   - Which tickers and directions survived to M5
   - What parameter configuration was selected
   - Expectancy, confidence, signal_count, execution_robustness from M5
   - Selected exit policy and parameters
   - Regime breakdown: does the edge concentrate in specific vix_band or spy_trend_20d?
   - Recommendation: promote to Strategy_Catalog, retune, or kill
8. In your final chat response, summarize the decision and give the absolute path to `FINDINGS_REPORT.md`.

## Rules
- Do not skip gates. A pass at M1 is not a pass at M4.
- Do not use v1 catalog results as evidence. Run fresh.
- Do not only print the findings in chat. Save them to `FINDINGS_REPORT.md`.
- Regime tags are observational. Do not use them to explain away a failed gate.
- If any stage fails, stop, update the hypothesis file, and report what failed and why.
