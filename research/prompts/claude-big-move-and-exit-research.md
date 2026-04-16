# Claude Handoff: Big Move Miss Audit + Exit Expansion Feasibility

You are working in `/Users/suman/code/mala_v2`.

Read `/Users/suman/code/mala_v2/AGENTS.md` first. Follow the research-only workflow. Do not write to Google Sheets. Do not make broad code changes unless the report proves a small infrastructure change is required and you clearly document it.

## Current Truth

- Normal strategy hypotheses are run with `hypothesis_agent.py`.
- The strategy runner is generic now; it uses strategy search specs and the factory registry.
- `research/hypotheses/big-move-miss-audit-current-basket.md` is a research brief, not a runnable M1-M5 hypothesis.
- Current M1-M5 gates are biased toward repeatable same-day intraday edge.
- Current exit optimization evaluates fixed reward-risk exits and VMA trailing where supported.
- Same-day flattening is built into `src/oracle/trade_simulator.py`.
- Do not retune a strategy after looking at holdout/big-move outcomes and then call that same run valid.

## Task A - Big Move Miss Audit

Goal: identify the largest directional moves in the current basket and determine whether Mala missed them because of entry logic, timeframe limitation, exit logic, same-day flattening, or scoring/gating.

Basket:

```text
SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA
```

Use the latest locally available data and artifacts. If raw cached OHLCV data is not available in the moved repo, say so clearly and use existing artifacts only; do not invent results.

Minimum report sections:

1. `Data Availability`
2. `Largest Move Events`
3. `Move Type Classification`
4. `Current Strategy Coverage`
5. `Exit Capture / Early Exit Evidence`
6. `Miss Reasons`
7. `Next Hypotheses`
8. `Infrastructure Recommendation`

Use this evidence format:

```text
Observed:
Inferred:
Next Hypothesis:
```

Write final report to:

```text
research/reports/big-move-miss-audit-current-basket.md
```

## Task B - Exit Expansion Feasibility

Question: if we keep the same winners from the known strategy-family sweep, do better exits turn them into larger winners?

Start by reading:

```text
research/reports/known-strategy-family-map.md
research/reports/shadow-candidate-packet-2026-04-12.md
research/reports/top-family-guardrail-comparison.md
data/results/hypothesis_runs/*/*/RUN_SUMMARY.md
data/results/hypothesis_runs/*/*/M5_execution.csv
data/results/hypothesis_runs/*/*/m5_exit_optimizations.json
```

Evaluate whether current exit artifacts are enough. If not enough, recommend the smallest next code change.

Candidate exit policies to evaluate or recommend:

- Generic ATR trailing stop.
- Generic moving-average trailing stop.
- Moving-average crossover exit.
- Wider fixed-R exits for trend families only.
- Time-based runner exit, such as hold to 11:30, 14:30, or EOD unless thesis break.

Do not compare exits by win rate alone. Prioritize:

```text
expectancy
profit_factor
avg_winner / avg_loser
total_pnl
tail_capture
max_drawdown or loss clustering if available
trade_count
win_rate only as context
```

## Decision Rules

- If big moves are mostly missed because no strategy fires: recommend new entry hypothesis families.
- If strategies fire correctly but exit too early: recommend exit-policy expansion first.
- If signals exist only on 5m/15m/30m/1h structure: recommend longer-timeframe support first.
- If winners are low-frequency and high-payoff: recommend a separate asymmetric-move gate instead of weakening the normal M1-M5 gate.

## Output Discipline

- Every factual claim must cite the file or artifact it came from.
- Do not use v1 results as validation.
- Do not write to `Strategy_Catalog`.
- Do not change hypothesis states unless you actually ran the corresponding workflow.
- End with a short verdict:

```text
Verdict:
- Can current infrastructure answer the big-move question? yes/no/partial
- Should we add exit policies first? yes/no/partial
- Smallest next implementation step:
```
