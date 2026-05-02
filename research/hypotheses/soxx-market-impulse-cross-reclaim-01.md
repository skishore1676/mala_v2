# Hypothesis: SOXX Market Impulse (Cross & Reclaim) discovery

## Config
- id: `soxx-market-impulse-cross-reclaim-01`
- state: `pending`
- decision: ``
- symbol_scope: `SOXX`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: ``

## Thesis
SOXX may offer a clean config-only semiconductor ETF test for early-session impulse continuation/reclaim when higher-timeframe momentum aligns, extending the recent semi Market Impulse cluster without requiring code changes.

## Rules
- Bound the first pass to dry-run/M1 only.
- Center the discovery sweep on entry_buffer_minutes=3, entry_window_minutes=60, regime_timeframe=5m, and vwma_periods=(8,21,34).
- Use the existing Market Impulse (Cross & Reclaim) strategy and declared search surface only.

## Notes
- Feasibility tag: config-only.
- Runnable with current codebase: strategy exists with 4 search parameters, 32 discovery configs, and 6 retune configs.
- Chosen because recent positive Market Impulse evidence exists in semis (AMD short, MU long, SMH short) while no SOXX Market Impulse hypothesis exists yet.
- Main caveat: SOXX may dilute single-name impulse behavior and overlaps somewhat with SMH, so treat this as a bounded sector-ETF read rather than a high-conviction edge claim.

## Agent Report
Pending.
