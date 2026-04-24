# Hypothesis: Expand30 W1-B1-P3 — SMH Market Impulse

## Config
- id: `expand30-w1-b1-p3-market-impulse-smh`
- state: `completed`
- decision: `promote`
- symbol_scope: `SMH`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M1`
- last_run: `2026-04-24T13:42:24+0000`

## Thesis
SMH may preserve part of the open cross-and-reclaim impulse edge seen in the broader liquid basket because the semiconductor ETF can express early directional regime alignment with less single-name idiosyncratic noise.

## Rules
- Entry only on Market Impulse cross-and-reclaim events.
- Allow both long and short directions per engine defaults.
- Restrict evaluation to the strategy search surface declared by `Market Impulse (Cross & Reclaim)`.
- Stop at M1 for this packet.

## Notes
- Campaign: expand30
- Packet: `W1-B1-P3`
- Queue row: `Q006`
- Objective: transfer sector ETF impulse behavior into a bounded M1 test.

## Agent Report
### Run
`2026-04-24T084200` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M3 → M4 → M5`

### Notes
- M3: 45 detail rows
- M4: 1 promoted
- M5: 4 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/expand30-w1-b1-p3-market-impulse-smh/2026-04-24T084200`
