# Mala v2 — Research Workbench

Clean hypothesis-to-M5 research engine. No deployment plumbing. Results stay
local as CSVs, with an optional final Strategy_Catalog Google Sheet write only
when a hypothesis reaches M5 promote.

---

## How to Run

```bash
uv sync
uv run python hypothesis_agent.py --hypothesis research/hypotheses/my-idea.md --max-stage M5
uv run python hypothesis_agent.py --hypothesis research/hypotheses/my-idea.md --dry-run
uv run python -m src.research.research_ops backfill
uv run python -m src.research.research_ops hot-start
uv run python main.py --tickers SPY --start 2024-01-01 --end 2024-12-31
uv run pytest tests/ -v
```

Minimum `.env`:
```
POLYGON_API_KEY=...
```

Optional Strategy_Catalog publish on M5 promote:
```
GOOGLE_API_CREDENTIALS_PATH=/path/to/google-credentials.json
STRATEGY_CATALOG_SHEET_ID=...
STRATEGY_CATALOG_SHEET_NAME=Strategy_Catalog
```

Data is cached as Parquet under `data/` — never re-downloads what's there.

---

## Architecture

```
Polygon.io API → Chronos (Parquet cache)
  → Newton (velocity, accel, jerk, VPOC, EMAs)
  → Strategy (signal generation)
  → Oracle (MFE/MAE, confidence, trade simulator)
  → M1–M5 gates (walk-forward, holdout, execution stress)
  → local CSVs in data/results/hypothesis_runs/{id}/{run_ts}/
```

---

## Key Directories

| Path | Purpose |
|---|---|
| `src/chronos/` | Polygon.io fetch + Parquet cache |
| `src/newton/` | Physics features (velocity, accel, jerk, VPOC, EMAs) |
| `src/strategy/` | Strategy classes + factory registry |
| `src/oracle/` | MFE/MAE metrics, Monte Carlo stress, trade simulator |
| `src/research/catalog.py` | Optional Strategy_Catalog row writer for M5 promotes |
| `src/research/catalog_steward.py` | Advisory review of Strategy_Catalog candidates against active/shadow state |
| `src/research/research_ops.py` | Local research ledger, historical backfill, and hot-start reconciliation |
| `src/research/market_regime.py` | Observational regime tags for detail artifacts |
| `src/research/exit_optimizer.py` | M5-plus thesis exit optimization artifact |
| `src/research/stages/` | M1-M5 gate logic |
| `hypothesis_agent.py` | The main entry point — reads `.md`, runs gates, writes CSVs |
| `research/hypotheses/` | Hypothesis state machine files |
| `data/results/hypothesis_runs/` | All output CSVs |

---

## Research Memory

Mala has three distinct research layers:

1. **Research Engine** — `hypothesis_agent.py` runs M1-M5 and writes canonical
   local evidence under `data/results/hypothesis_runs/{id}/{run_ts}/`.
2. **Research Ops** — `src.research.research_ops` reconstructs the lab notebook:
   tested hypotheses, runs, promoted candidates, stale board state, missing
   summaries, and Strategy_Catalog publish gaps.
3. **Catalog Steward** — `src.research.catalog_steward` reviews only
   Strategy_Catalog candidates and recommends `live`, `shadow`, `hold`, or
   `pause`.

Strategy_Catalog is not the research archive. It should contain only M5-passed
execution candidates intended for Bhiksha/operator review.

Research Ops outputs are rebuildable summaries:

```bash
uv run python -m src.research.research_ops backfill
uv run python -m src.research.research_ops hot-start
uv run python -m src.research.research_ops next-actions
uv run python -m src.research.research_ops publish-pending --dry-run
uv run python -m src.research.research_ops sync-board --dry-run
```

Default output:

- `data/results/research_ops/research_ledger.xlsx`
- `data/results/research_ops/hot_start.md`
- `data/results/research_ops/csv/*.csv`

Use `hot_start.md` at the beginning or end of a research session to identify:
stale board rows, missing summaries, promoted rows absent from Strategy_Catalog,
terminal hypotheses without artifacts, and other next-action cleanup items.

Use `src.research.research_runner` for bounded execution commands. It wraps
`hypothesis_agent.py` and keeps agents on a deterministic command menu:

```bash
uv run python -m src.research.research_runner create-hypothesis --title "..." --strategy "Opening Drive Classifier" --symbol-scope SPY
uv run python -m src.research.research_runner dry-run --hypothesis research/hypotheses/my-idea.md
uv run python -m src.research.research_runner run-m1 --hypothesis research/hypotheses/my-idea.md
uv run python -m src.research.research_runner continue-approved --hypothesis research/hypotheses/my-idea.md
uv run python -m src.research.research_runner retune-plan --hypothesis research/hypotheses/my-idea.md
uv run python -m src.research.research_runner retune-approved --hypothesis research/hypotheses/my-idea.md
```

`publish-pending` and `sync-board` are dry-run by default. Require explicit
`--apply` before touching Google Sheets.

Local orchestration is available through `src.research.local_orchestrator`:

```bash
uv run python -m src.research.local_orchestrator once --mode dry-run
uv run python -m src.research.local_orchestrator once --mode apply-safe
uv run python -m src.research.local_orchestrator daemon --mode apply-safe --interval-seconds 1800
```

The orchestrator consumes `next-actions`, runs only safe/dry-run commands, and
writes a reasoning brief under `data/results/research_ops/orchestrator/`. It
must stop for approval before research execution, retunes, catalog writes,
board writes, and evidence repairs.

---

## Strategy Registry

`src/strategy/factory.py:_NAMED_BUILDERS` (authoritative list):

- `Elastic Band Reversion`
- `Kinematic Ladder`
- `Compression Expansion Breakout`
- `Regime Router (Kinematic + Compression)`
- `Opening Drive Classifier`
- `Opening Drive v2 (Short Continue)`
- `Jerk-Pivot Momentum (tight)`
- `Market Impulse (Cross & Reclaim)`

---

## Stage Gates (M1–M5)

| Stage | Tests | Key threshold |
|---|---|---|
| M1 | Walk-forward signal quality | ≥3 OOS windows, ≥50 signals, ≥60% positive, exp_r > 0 |
| M2 | Cost-friction convergence | Passes at 5, 8, and 12 bps |
| M3 | Walk-forward consistency | Full calibration window |
| M4 | Holdout (never-seen data) | `holdout_start` → `holdout_end`, ≥15 signals |
| M5 | Execution stress (Monte Carlo) | 4000 bootstrap iters, 8 bps base cost |

Calibration window default: `2024-01-02` → `2025-11-30`
Holdout window default: `2025-12-01` → `2026-02-28`

---

## Hypothesis Workbench

### How to start a session

1. Open Claude Code in this repo root.
2. Paste the prompt below (fill in `<IDEA>`).
3. Claude will produce a feasibility tag and wait before running anything.

**Onboarding prompt:**
```
I have a trading hypothesis: <IDEA>

Please read this CLAUDE.md, then:
1. Read src/strategy/factory.py:_NAMED_BUILDERS to see available strategies.
2. Read research/hypotheses/TEMPLATE.md for the hypothesis file format.
3. Give me a feasibility assessment:
   - config-only: existing strategy + parameter changes, no code needed
   - new-class: new strategy class needed, reuses existing Newton features
   - new-feature: needs a new Newton feature or Oracle metric (biggest lift)
4. For config-only: draft the parameter changes and offer to run hypothesis_agent.py.
5. For new-class or new-feature: scope the work (file, methods, ~line count) before writing any code.
Do not write code until I approve the plan.
```

### Feasibility tags

- **`config-only`** — parameter change only, lowest lift. Draft YAML config and run `hypothesis_agent.py`.
- **`new-class`** — new strategy class in `src/strategy/`, reuses existing Newton features. Scope first.
- **`new-feature`** — new Newton feature or Oracle metric. Biggest lift. Scope before touching code.

### Hypothesis file state machine

States: `pending` → `running` → (`completed` | `kill` | `retune`)

`hypothesis_agent.py` reads the state and resumes from where it left off.
On any run it writes a `## Agent Report` section back into the `.md` file.

### On kill
- Set `state: kill` in the `.md` — that's it.
- No other cleanup needed.

### On promote (M5 pass)
- `state` is set to `completed` automatically.
- Results are in `data/results/hypothesis_runs/{id}/{run_ts}/M5_execution.csv`.
- If Google Sheet settings are configured, `hypothesis_agent.py` upserts one `candidate` row into `Strategy_Catalog`.

---

## Workbench Journal

_Append one or two lines per session. Keep entries short. Prune stale ones._

- _(no entries yet)_
