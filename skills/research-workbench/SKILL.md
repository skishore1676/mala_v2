---
name: research-workbench
description: Use when working in mala_v2 on trading hypotheses, existing strategy evaluation, M1-M5 research runs, result interpretation, or Strategy_Catalog promotion. Follow the local hypothesis workbench loop and keep deployment plumbing out of scope.
---

# Research Workbench

## Scope

Use this skill inside `mala_v2` to turn one trading idea into local evidence.
Mala v2 is a research workbench, not a live execution system.

Do:
- classify the idea as `config-only`, `new-class`, or `new-feature`
- prefer existing strategies and their declared `search_spec` / `parameter_space`
- run bounded evidence through `hypothesis_agent.py`
- write or update one hypothesis markdown file
- interpret artifacts under `data/results/hypothesis_runs/`
- use market-regime tags as observational evidence, not as gates
- use `CATALOG_SELECTED.csv` as the concise selected-candidate readout
- treat `m5_exit_optimizations.json` plus the per-candidate `m5_exit_optimization_<ticker>_<direction>_<hash>.json` files as the Bhiksha-facing exit research artifacts
- remember that 1-minute runs apply execution guardrails from `config/hypothesis_defaults.yaml`
- run `src.research.research_ops` after batches when you need the master ledger, hot-start findings, or next-action cleanup list
- allow the optional `Strategy_Catalog` write only after M5 `promote`

Do not:
- add Bhiksha/session payload/live execution plumbing
- reintroduce nightly scouts, review queues, or Google Sheet control towers
- bypass M1-M5 gates when claiming a promoted candidate
- edit `.env` with secrets

## First Files

Read only what the task needs, in this order:
1. `CLAUDE.md`
2. `research/hypotheses/TEMPLATE.md`
3. `src/strategy/factory.py`
4. the requested hypothesis file, if one exists
5. strategy implementation only for the chosen strategy

## Workflow

For a new idea:
1. Create a hypothesis file from `research/hypotheses/TEMPLATE.md`.
2. Set `strategy` to the closest registry name from `src/strategy/factory.py`.
3. Use `state: pending`, empty `decision`, and a conservative `max_stage`.
4. Dry-run first:
   ```bash
   ./.venv/bin/python hypothesis_agent.py --hypothesis research/hypotheses/<id>.md --dry-run
   ```
   If intentionally replaying a completed or killed hypothesis, add `--force-rerun`.
5. Run the smallest honest stage sequence:
   ```bash
   ./.venv/bin/python hypothesis_agent.py --hypothesis research/hypotheses/<id>.md --max-stage M1
   ```
6. Continue only when the prior stage promotes or the human asks to retune.

For an existing strategy:
- inspect its `search_spec` or `parameter_space`
- do not hard-code search grids in `hypothesis_agent.py`
- if a strategy lacks a useful search surface, add or improve it on the strategy class

## Gate Discipline

The agent owns the stage protocol; the human should not need to remember it.

- start every new or replayed hypothesis at M1 unless the file is resuming a promoted run
- stop at the first failed gate and update the hypothesis state/report
- continue to M2-M5 only for survivors promoted by the prior gate
- do not treat a v1 result as validated in v2 until it passes this workbench
- after M5 promote, expect exit optimization to evaluate each promoted catalog candidate and write `m5_exit_optimizations.json`
- inspect `market_regime_key`, `vix_band`, `spy_trend_20d`, and `session_type` when interpreting detail artifacts
- write to Strategy_Catalog only after M5 `promote`

## Evidence Rules

- `retune` means some signal remains but gates did not promote.
- `kill` means the idea failed the honest gate path.
- `completed` means M5 produced execution mappings and `decision: promote`.
- Market regime can explain where an edge lives, but it must not rescue a failed gate.
- Exit optimization is research output for execution readiness; Bhiksha translation stays outside v2.
- The sheet writer should create `lifecycle_status=candidate`; human approval happens outside the workbench.

## Research Ops Layer

Use Research Ops for memory and continuity, not for gate decisions. It rebuilds
its view from local Mala evidence:

```bash
./.venv/bin/python -m src.research.research_ops backfill
./.venv/bin/python -m src.research.research_ops hot-start
./.venv/bin/python -m src.research.research_ops next-actions
./.venv/bin/python -m src.research.research_ops publish-pending --dry-run
./.venv/bin/python -m src.research.research_ops sync-board --dry-run
./.venv/bin/python -m src.research.research_ops mark-stale \
  --category run_missing_summary \
  --key elastic-band-current-basket-discovery/2026-04-12T082350 \
  --reason "missing RUN_SUMMARY; old run; not used as evidence"
```

Outputs:
- `data/results/research_ops/research_ledger.xlsx`
- `data/results/research_ops/hot_start.md`
- `data/results/research_ops/csv/*.csv`

Interpretation:
- `catalog_publish_pending` means an M5 selected candidate is absent from Strategy_Catalog; dedupe before publishing.
- `board_state_stale` means an operator-facing sheet row no longer matches Mala's local state.
- `run_missing_summary` means a run has stage artifacts but lacks a summary; repair or rerun reporting before relying on it as evidence.
- `terminal_without_artifacts` means a hypothesis file is terminal but no run directory was found; inspect before trusting it.
- `next-actions` turns those findings plus pending/retune hypotheses into a ranked operator queue.
- `publish-pending` and `sync-board` are dry-run by default; use `--apply` only after explicit review.
- `mark-stale` appends a non-destructive decision to `research/reports/research_ops/finding_dispositions.jsonl`; it suppresses that reviewed finding without deleting or moving artifacts.

## Research Runner Layer

Use Research Runner for the bounded execution surface. It delegates to
`hypothesis_agent.py`; the staged engine remains authoritative.

```bash
./.venv/bin/python -m src.research.research_runner create-hypothesis \
  --title "SPY Opening Drive continuation" \
  --strategy "Opening Drive Classifier" \
  --symbol-scope SPY
./.venv/bin/python -m src.research.research_runner dry-run --hypothesis research/hypotheses/my-idea.md
./.venv/bin/python -m src.research.research_runner run-m1 --hypothesis research/hypotheses/my-idea.md
./.venv/bin/python -m src.research.research_runner continue-approved --hypothesis research/hypotheses/my-idea.md
./.venv/bin/python -m src.research.research_runner retune-plan --hypothesis research/hypotheses/my-idea.md
./.venv/bin/python -m src.research.research_runner retune-approved --hypothesis research/hypotheses/my-idea.md
```

## Local Orchestrator Layer

Use the local orchestrator when you want a bounded agent loop rather than a
free-form agent. It consumes `research_ops next-actions`, runs only safe/dry-run
commands, and writes a reasoning brief for the next human/Codex/OpenClaw
checkpoint.

```bash
./.venv/bin/python -m src.research.local_orchestrator once --mode dry-run
./.venv/bin/python -m src.research.local_orchestrator once --mode apply-safe
./.venv/bin/python -m src.research.local_orchestrator daemon --mode apply-safe --interval-seconds 1800
```

The orchestrator must not auto-run research execution, retune execution,
catalog writes, board writes, code changes, or evidence repairs. It may plan
them and emit a reasoning brief.

## Validation

After code changes, run:
```bash
./.venv/bin/python -m pytest tests/ -q
./.venv/bin/python hypothesis_agent.py --hypothesis research/hypotheses/TEMPLATE.md --dry-run
./.venv/bin/python -m src.research.research_ops hot-start
./.venv/bin/python -m src.research.local_orchestrator once --mode dry-run
```
