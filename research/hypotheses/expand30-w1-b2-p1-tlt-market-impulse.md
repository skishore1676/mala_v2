# Hypothesis: Expand30 W1-B2-P1 TLT Market Impulse

## Config
- id: `expand30-w1-b2-p1-tlt-market-impulse`
- state: `kill`
- decision: `kill`
- symbol_scope: `TLT`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-25T18:09:45+0000`

## Thesis
TLT is a liquid macro ETF candidate for testing whether the existing Market Impulse cross-and-reclaim behavior transfers outside single-name tech and sector ETFs. The honest first pass is a bounded M1-only discovery run on the existing strategy surface.

## Rules
- Entry uses the existing Market Impulse cross-and-reclaim logic.
- Longs require bullish higher-timeframe impulse regime alignment.
- Shorts require bearish higher-timeframe impulse regime alignment.
- Entry timing stays within the strategy-defined opening buffer and entry window.
- Evaluate only the declared Market Impulse search surface.

## Notes
- Campaign: Expand30 wave 1 packet W1-B2-P1.
- Strategy family requested: Market Impulse.
- Scope is dry-run plus M1 only.
- No retune and no M2+ continuation in this packet.

## Agent Report
- decision: `kill`
- updated_at: `2026-04-25T18:09:45+0000`
- source: `research_runner kill-approved`
- reason: operator approved kill from Research_Control
