# mala_v2 — Agent Onboarding

You are working in a research-only backtesting engine. No deployment, no Google Sheets, no live execution. All output is local CSVs.

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
| `hypothesis_agent.py` | The main runner — reads `.md`, runs gates, writes CSVs |
| `src/strategy/factory.py` | Strategy registry (`_NAMED_BUILDERS`, `build_strategy`) |
| `src/research/stages/` | M1-M5 gate logic — do not touch without reading first |
| `src/newton/engine.py` | Physics features (velocity, accel, jerk, VPOC, EMAs) |
| `research/hypotheses/` | Hypothesis state machine files |
| `data/results/hypothesis_runs/` | All output CSVs |

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

**If the user has a new hypothesis:**
1. Create `research/hypotheses/{slug}.md` from `TEMPLATE.md`
2. Fill in `strategy`, `symbol_scope`, `max_stage`
3. Run `--dry-run` first to confirm config count and data availability
4. Run for real, observe M1 gate result

**Feasibility tags (decide before writing code):**
- `config-only` — existing strategy + parameter changes, no code needed
- `new-class` — new strategy class, scope file + methods before writing
- `new-feature` — new Newton feature or Oracle metric, biggest lift, scope first

**Rule:** do not write any code until the feasibility tag is agreed and the scope is approved.

---

## Parameter spaces

`hypothesis_agent.py` has a `PARAMETER_SPACES` dict at the top with discovery and retune grids for each strategy. Add a new strategy entry there to make it sweepable. The discovery grid is sampled down to 32 configs by default.

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
