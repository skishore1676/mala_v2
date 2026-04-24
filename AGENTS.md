# mala_v2 — Agent Onboarding

You are working in a research-only backtesting engine. No deployment plumbing and no live execution. Output is local CSVs, with an optional final Strategy_Catalog Google Sheet upsert only after an M5 promote.

---

## What this repo does

Takes a trading hypothesis → validates it through M1-M5 gates → local CSV results.

```
Polygon.io (Parquet cache) → Newton (physics features) → Strategy → Oracle (MFE/MAE)
  → M1 walk-forward → M2 cost convergence → M3 OOS → M4 holdout → M5 exec stress
  → data/results/hypothesis_runs/{id}/{timestamp}/
```

---

## Commands

```bash
# Sync deps (one-time)
uv sync

# Run a hypothesis through the gates
python hypothesis_agent.py --hypothesis research/hypotheses/my-idea.md
python hypothesis_agent.py --hypothesis research/hypotheses/my-idea.md --max-stage M2
python hypothesis_agent.py --hypothesis research/hypotheses/my-idea.md --dry-run

# Single-shot sanity check on cached data
python main.py --tickers SPY --start 2024-01-01 --end 2024-12-31

# Tests
python -m pytest tests/ -v
```

`.env` needs only `POLYGON_API_KEY=...`

---

## Key files

| File | Purpose |
|---|---|
| `hypothesis_agent.py` | The main runner — reads `.md`, runs M1→M5, writes CSVs |
| `src/strategy/factory.py` | Strategy registry (`_NAMED_BUILDERS`, `build_strategy`) |
| `src/research/search_space.py` | Bounded discovery/retune configs from each strategy's search surface |
| `src/research/market_regime.py` | Market regime classifier (vix_band / spy_trend_20d / session_type) |
| `src/research/exit_optimizer.py` | M5-plus: evaluates thesis exit policy grid, writes per-candidate exit optimization artifacts |
| `src/research/catalog.py` | Strategy_Catalog upsert (called on M5 promote, includes exit fields) |
| `src/research/catalog_steward.py` | Advisory Strategy_Catalog + active_strategy review; writes local recommendations and optional steward Sheet annotations |
| `src/research/research_ops.py` | Research ledger/backfill/hot-start tool; reconstructs tested ideas, runs, promoted rows, and next-action findings |
| `src/research/stages/` | M1-M5 gate logic — do not touch without reading first |
| `src/newton/engine.py` | Physics features (velocity, accel, jerk, VPOC, EMAs) |
| `research/hypotheses/` | Hypothesis state machine files |
| `data/results/hypothesis_runs/` | All output artifacts per run |

---

## Hypothesis file schema

```markdown
## Config
- id:           `slug-kebab-case`
- state:        `pending`          # pending | running | retune | completed | kill
- decision:     ``                 # set by agent after each run
- symbol_scope: `SPY`             # comma-separated tickers
- strategy:     `Opening Drive Classifier`  # must match factory registry
- max_stage:    `M5`              # M1..M5
- last_run:     ``
```

**State machine:**
- `pending` → full discovery sweep, starts at M1
- `retune`  → tight sweep, starts at M1
- `running` + `decision=promote_to_m3` → resumes at M3 using previous run's CSVs
- `completed` / `kill` → skipped

---

## Available strategies

```python
from src.strategy.factory import available_strategy_names
# ('Compression Expansion Breakout', 'Elastic Band Reversion',
#  'Jerk-Pivot Momentum (tight)', 'Kinematic Ladder',
#  'Market Impulse (Cross & Reclaim)', 'Opening Drive Classifier',
#  'Opening Drive v2 (Short Continue)', 'Regime Router (Kinematic + Compression)')
```

---

## How to start a workbench session

Use `skills/research-workbench/SKILL.md` when onboarding an agent into hypothesis work.

Use `skills/catalog-steward/SKILL.md` when reviewing Strategy_Catalog against
active_strategy. That role is advisory: it can write local recommendation
artifacts by default and may update only `steward_recommendation` /
`steward_notes` in Strategy_Catalog when explicitly asked.

Use `src.research.research_ops` whenever you need the research memory layer:

```bash
# Rebuild the local research ledger from hypothesis files + run artifacts
python -m src.research.research_ops backfill

# Generate hot-start and next-action reconciliation reports
python -m src.research.research_ops hot-start
python -m src.research.research_ops next-actions
python -m src.research.research_ops action-brief --key retune_plan:my-idea
python -m src.research.research_ops action-brief --key retune_plan:my-idea --push-control

# External mutations are dry-run by default; add --apply only after review
python -m src.research.research_ops publish-pending --dry-run
python -m src.research.research_ops sync-board --dry-run
python -m src.research.research_ops push-control \
  --control-sheet-id 1qzXNn8ezagqeDR9EI9hoUTzhANKARk4jG4pdy8-32T0 \
  --control-sheet-name Research_Control

# Non-destructively suppress reviewed stale findings from future queues
python -m src.research.research_ops mark-stale \
  --category run_missing_summary \
  --key elastic-band-current-basket-discovery/2026-04-12T082350 \
  --reason "missing RUN_SUMMARY; old run; not used as evidence"
```

Use `src.research.research_runner` as the safe command surface for actual
Mala research execution. It delegates to `hypothesis_agent.py`; it does not
replace the staged engine.

```bash
python -m src.research.research_runner create-hypothesis \
  --title "AMD Market Impulse retest" \
  --strategy "Market Impulse (Cross & Reclaim)" \
  --symbol-scope AMD
python -m src.research.research_runner dry-run --hypothesis research/hypotheses/my-idea.md
python -m src.research.research_runner run-m1 --hypothesis research/hypotheses/my-idea.md
python -m src.research.research_runner continue-approved --hypothesis research/hypotheses/my-idea.md
python -m src.research.research_runner retune-plan --hypothesis research/hypotheses/my-idea.md
python -m src.research.research_runner retune-approved --hypothesis research/hypotheses/my-idea.md
```

Use `src.research.local_orchestrator` for the local agent loop. It consumes
`research_ops next-actions`, runs only safe/dry-run commands, and writes a
reasoning brief for Codex/OpenClaw/human review before any gated action.

```bash
python -m src.research.local_orchestrator once --mode dry-run
python -m src.research.local_orchestrator once --mode apply-safe
python -m src.research.local_orchestrator once --mode apply-safe --with-control-sheet
python -m src.research.local_orchestrator daemon --mode apply-safe --interval-seconds 1800
```

The workbook/CSV outputs under `data/results/research_ops/` are rebuildable
summaries, not canonical truth. Canonical research evidence remains:
`research/hypotheses/` plus `data/results/hypothesis_runs/`.
Finding dispositions are decision memory and live in
`research/reports/research_ops/finding_dispositions.jsonl`.

Mental model:
- Mala research engine proves or kills ideas through M1-M5.
- Research Ops keeps the lab notebook, backfills history, and proposes next actions.
- Action Briefs inspect the queued item, summarize evidence, and recommend a bounded operator action.
- Research Runner is the bounded command wrapper for creating/running approved hypotheses.
- Local Orchestrator consumes the next-action queue and stops at reasoning/approval checkpoints.
- Research_Control Google Sheet is the operator UI; approved rows drive the local orchestrator when `--with-control-sheet` is set. Valid actions are blank, `APPROVE_RETUNE`, `APPROVE_PUBLISH`, `APPROVE_BOARD_SYNC`, `APPROVE_SURFACE_EXPANSION`, `MARK_STALE`, and `SKIP`.
- Strategy_Catalog contains only M5-promoted execution candidates for Bhiksha/operator review.
- Catalog Steward ranks existing Strategy_Catalog candidates for live/shadow/hold/pause.
- OpenClaw/Codex agents may orchestrate later, but they should call Mala tools rather than hold private research truth.

**If the user has a new hypothesis:**
1. Create `research/hypotheses/{slug}.md` from `TEMPLATE.md`
2. Fill in `strategy`, `symbol_scope`, `max_stage`
3. Run `--dry-run` first to confirm config count and data availability
4. Run M1 first — gate: ≥50 OOS signals, ≥3 windows, ≥60% positive, exp_r>0
5. Continue gate-by-gate. M1→M2 proves cost-stability. M3 proves OOS walk-forward. M4 proves holdout. M5 proves execution robustness.
6. After M5: exit optimizer runs automatically — evaluates fixed-RR and VMA policy grid, writes `m5_exit_optimizations.json` plus per-candidate artifacts to the run dir
7. Market regime is tagged on M1_detail.csv and M4_holdout.csv (observational — not a gate). Use regime slices to check if signal quality was regime-dependent.
8. On M5 promote: Strategy_Catalog row is written with all 20 columns filled from M5 data + exit optimization results. `bhiksha_ready` is derived from Bhiksha-supported strategy keys and thesis exit policies in `src/research/catalog.py`.
9. After a research batch, run `python -m src.research.research_ops backfill` and read `data/results/research_ops/hot_start.md` before deciding the next cleanup/publish step.

**Reading regime slices post-run:**
Look at `M4_holdout.csv` columns `vix_band`, `spy_trend_20d`, `session_type`, `market_regime_key` to answer:
- Did the strategy only work in a specific VIX band?
- Was performance regime-dependent in a way that affects live deployment?
Regime is evidence, not an excuse. Do not use it to explain away a bad M4 result.

**Feasibility tags (decide before writing code):**
- `config-only` — existing strategy + parameter changes, no code needed
- `new-class` — new strategy class, scope file + methods before writing
- `new-feature` — new Newton feature or Oracle metric, biggest lift, scope first

**Rule:** do not write any code until the feasibility tag is agreed and the scope is approved.

For v1 replay work, create normal v2 hypothesis files. Do not import old v1 results as proof; use them only as leads for which families, symbols, and parameter areas to retest.

---

## Parameter spaces

`hypothesis_agent.py` does not own strategy-specific grids.

Search configs come from `src/research/search_space.py`, which asks the selected strategy for:
- `search_spec` first, including gating and ordering constraints
- `parameter_space` as a fallback

To make a strategy sweepable, add or improve the strategy class's `search_spec` / `parameter_space`. Do not add hard-coded grids back into `hypothesis_agent.py`.

---

## Gate thresholds (M1)

```
min OOS windows:   3
min OOS signals:   50
min % positive:    60%
min exp_r:         > 0
cost friction:     5 / 8 / 12 bps (M2 convergence grid)
```

Holdout window: `2025-12-01 → 2026-02-28` (never touched during M1-M3)
