# Hypothesis: Expand30 W1-B1-P3 — XLK Market Impulse

## Config
- id: `expand30-w1-b1-p3-market-impulse-xlk`
- state: `kill`
- decision: `kill`
- symbol_scope: `XLK`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-25T17:39:33+0000`

## Thesis
XLK may retain an open cross-and-reclaim impulse edge because the sector ETF concentrates large-cap tech leadership while smoothing single-name event noise, making regime-aligned morning continuation plausible.

## Rules
- Entry only on Market Impulse cross-and-reclaim events.
- Allow both long and short directions per engine defaults.
- Restrict evaluation to the strategy search surface declared by `Market Impulse (Cross & Reclaim)`.
- Stop at M1 for this packet.

## Notes
- Campaign: expand30
- Packet: `W1-B1-P3`
- Queue row: `Q007`
- Objective: test sector ETF transfer of the impulse family without retune.

## Agent Report
- decision: `kill`
- updated_at: `2026-04-25T17:39:33+0000`
- source: `research_runner kill-approved`
- reason: operator approved kill from Research_Control
