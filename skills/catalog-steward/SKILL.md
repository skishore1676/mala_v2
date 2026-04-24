---
name: catalog-steward
description: Use when reviewing Mala Strategy_Catalog rows against Bhiksha active_strategy, ranking candidates for live/shadow/hold/pause, producing operator recommendation artifacts, or optionally pushing steward annotations back to Strategy_Catalog.
---

# Catalog Steward

## Scope

Use this skill after Mala has already produced Strategy_Catalog candidates.
This is an operator advisory role, not a backtest runner and not an execution
controller.

Do:
- read `Strategy_Catalog`, `active_strategy`, and Bhiksha active-plan output
- treat Mala M1-M5 as the strict research gate
- recommend `live`, `shadow`, `hold`, or `pause`
- prefer inclusive shadowing for `bhiksha_ready=true` catalog rows unless there is a clear execution or portfolio reason to hold
- write local markdown/CSV recommendation artifacts by default
- update `steward_recommendation` and `steward_notes` only when explicitly asked to push annotations

Do not:
- rerun hypotheses or weaken M1-M5 gates
- edit `active_strategy` unless the user explicitly asks for that separate execution step
- write orders, restart Bhiksha, or change live execution state
- overwrite `operator_notes` or non-steward catalog columns

## Workflow

1. Inspect the live sheets and active plan if credentials/server access are available.
2. Run the steward artifact command:
   ```bash
   ./.venv/bin/python -m src.research.catalog_steward
   ```
3. If the user explicitly wants Sheet annotations, add:
   ```bash
   ./.venv/bin/python -m src.research.catalog_steward --push-sheet
   ```
4. Review the generated report under `research/reports/catalog_steward/`.

## Codex CLI on oldmac

When delegating this review to Codex CLI over SSH on oldmac, launch through a
login shell so `~/.zprofile` exposes Node and the Codex binary:

```bash
ssh oldmac 'zsh -lc "cd ~/Documents/mala_v2 && codex exec \"Use the catalog-steward skill. Review the current sheets and produce artifact-only recommendations.\""'
```

For Sheet annotations, keep the ask explicit:

```bash
ssh oldmac 'zsh -lc "cd ~/Documents/mala_v2 && codex exec \"Use the catalog-steward skill. Run the steward with --push-sheet and report the generated artifact paths.\""'
```

## Recommendation Vocabulary

- `live`: add or keep live in `active_strategy`
- `shadow`: add or keep shadow in `active_strategy`
- `hold`: keep in Strategy_Catalog only
- `pause`: disable or remove from `active_strategy`

`active_strategy.mode` remains the execution authority. The steward fields are
advisory only.

## Sheet Annotation Contract

The only steward-owned Strategy_Catalog columns are:
- `steward_recommendation`
- `steward_notes`

`steward_notes` may be short text or compact JSON. The built-in steward command
uses compact JSON containing rank, reason, reviewed_at, and report path.
