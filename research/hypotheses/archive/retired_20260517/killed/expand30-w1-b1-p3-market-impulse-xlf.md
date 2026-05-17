# Hypothesis: Expand30 W1-B1-P3 — XLF Market Impulse

## Config
- id: `expand30-w1-b1-p3-market-impulse-xlf`
- state: `kill`
- decision: `kill`
- symbol_scope: `XLF`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-24T13:42:58+0000`

## Thesis
XLF is a useful non-tech sector control for the Market Impulse family: if the morning cross-and-reclaim setup is portable beyond tech-heavy leadership, the financial ETF should show enough opening directional structure to survive M1.

## Rules
- Entry only on Market Impulse cross-and-reclaim events.
- Allow both long and short directions per engine defaults.
- Restrict evaluation to the strategy search surface declared by `Market Impulse (Cross & Reclaim)`.
- Stop at M1 for this packet.

## Notes
- Campaign: expand30
- Packet: `W1-B1-P3`
- Queue row: `Q008`
- Objective: test whether the impulse edge generalizes outside the core tech complex.

## Agent Report
### Run
`2026-04-24T084225` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M3 → M4`

### Notes
- M3: 57 detail rows
- M4: 0 promoted

### Decision
`kill`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b1-p3-market-impulse-xlf/2026-04-24T084225`
