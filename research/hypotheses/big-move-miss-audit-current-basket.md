# Research Brief: Big Move Miss Audit - Current Basket

## Config
- id: `big-move-miss-audit-current-basket`
- state: `research_only`
- decision: ``
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Research Audit - Big Move Miss`
- max_stage: `audit`
- last_run: ``

## Thesis
Mala's current M1-M5 workflow is good at finding repeatable intraday edges, but it may miss or under-credit large directional moves because the default evaluation is same-day, reward-risk/MFE-MAE based, and consistency-gated. This audit should identify the largest moves in the current basket, classify the conditions that existed before and during those moves, and determine whether current v2 strategy families fired, fired in the right direction, or exited too early.

## Rules
- This is not a runnable `hypothesis_agent.py` file yet.
- Do not write to `Strategy_Catalog`.
- Use existing run artifacts, strategy classes, and cached market data when available.
- Treat all prior v1/v2 winners as leads, not proof.
- Separate facts from interpretation:
  - `Observed`: directly supported by data/artifacts.
  - `Inferred`: reasoned explanation from observed evidence.
  - `Next Hypothesis`: future test idea, not validation.
- Do not retune any existing strategy after looking at holdout/big-move outcomes and then claim the original run is valid.

## Audit Questions
- Which ticker/date combinations were the largest directional moves in the current basket?
- Were they gap-and-go, gap-fail, trend-day continuation, VWAP reclaim/hold, compression expansion, opening range expansion, multi-day continuation, or event/catalyst-like moves?
- Did any current Mala strategy fire before or early enough in the move?
- If a strategy fired, was the direction correct?
- If the direction was correct, did the current exit model leave a large amount of MFE uncaptured?
- Were missed moves caused by entry logic, timeframe blindness, exit logic, same-day flattening, M1/M2 gates, or lack of event/regime features?
- Which strategy family came closest to catching each move?

## Suggested Metrics
- `daily_return_pct`
- `daily_true_range_pct`
- `daily_atr_multiple`
- `open_to_close_pct`
- `gap_pct`
- `first_30m_return_pct`
- `first_60m_return_pct`
- `mfe_from_open_pct`
- `close_location_in_range`
- `strategy_fired`
- `signal_direction`
- `direction_correct`
- `minutes_from_open_to_first_signal`
- `mfe_capture_ratio`
- `exit_reason`
- `bars_held`
- `miss_reason`

## Expected Output
- Write the final report to:
  - `research/reports/big-move-miss-audit-current-basket.md`
- Include:
  - top big-move table by ticker/date
  - classification of move type
  - current-strategy coverage table
  - exit-capture findings
  - concrete next hypotheses
  - clear recommendation on whether to add new strategy families, longer timeframe support, or new exit policies first

## Notes
- Feasibility tag: `new-feature`
- Current runner limitation: `hypothesis_agent.py` does not yet have an `audit` stage.
- Current exit limitation: generic fixed-RR exists, VMA trailing exists mainly for Market Impulse, and same-day hard flat is built into the trade simulator.
- Candidate next infrastructure:
  - `big_move_audit.py`
  - generic ATR trailing exit
  - generic moving-average trailing/crossover exit
  - top-decile/tail-capture scoring
  - optional multi-day hold lane

## Agent Report
Pending.
