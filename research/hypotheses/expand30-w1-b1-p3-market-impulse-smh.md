# Hypothesis: Expand30 W1-B1-P3 — SMH Market Impulse

## Config
- id: `expand30-w1-b1-p3-market-impulse-smh`
- state: `completed`
- decision: `promote`
- symbol_scope: `SMH`
- strategy: `Market Impulse (Cross & Reclaim)`
- max_stage: `M5`
- last_run: `2026-05-17T12:07:38+0000`

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
`2026-05-17T070720` — strategy: `Market Impulse (Cross & Reclaim)`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.2128  signals=230  windows=5
- M2: 3 candidates promoted
- M3: 45 detail rows
- M4: 1 promoted
- M5: 4 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/research/results/hypothesis_runs/expand30-w1-b1-p3-market-impulse-smh/2026-05-17T070720`
