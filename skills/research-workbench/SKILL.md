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
- treat `m5_exit_optimization.json` as the Bhiksha-facing exit research artifact
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
- after M5 promote, expect exit optimization to evaluate the promoted candidate and write `m5_exit_optimization.json`
- inspect `market_regime_key`, `vix_band`, `spy_trend_20d`, and `session_type` when interpreting detail artifacts
- write to Strategy_Catalog only after M5 `promote`

## Evidence Rules

- `retune` means some signal remains but gates did not promote.
- `kill` means the idea failed the honest gate path.
- `completed` means M5 produced execution mappings and `decision: promote`.
- Market regime can explain where an edge lives, but it must not rescue a failed gate.
- Exit optimization is research output for execution readiness; Bhiksha translation stays outside v2.
- The sheet writer should create `lifecycle_status=candidate`; human approval happens outside the workbench.

## Validation

After code changes, run:
```bash
./.venv/bin/python -m pytest tests/ -q
./.venv/bin/python hypothesis_agent.py --hypothesis research/hypotheses/TEMPLATE.md --dry-run
```
