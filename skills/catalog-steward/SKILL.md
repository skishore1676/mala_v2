---
name: catalog-steward
description: Use when reviewing Mala_Evidence_v1 rows against Bhiksha active_strategy, ranking candidates for shadow/live/hold/pause, producing operator recommendation artifacts, or checking the handoff contract.
---

# Catalog Steward

## Scope

Use this skill after Mala has produced `Mala_Evidence_v1` candidates.
This is an operator advisory role, not a backtest runner and not an execution
controller.

Do:
- read `Mala_Evidence_v1`, `Operator_Defaults_v1`, `active_strategy`, and Bhiksha active-plan output
- treat Mala M1-M5 as the strict research gate
- recommend `live`, `shadow`, `hold`, or `pause`
- prefer inclusive shadowing for evidence-backed `recommendation_tier=shadow` rows unless there is a clear execution or portfolio reason to hold
- treat `recommendation_tier=promote` as eligible for human live review, not automatic live authorization
- keep `watch_only` rows out of `active_strategy`
- write local markdown/CSV recommendation artifacts by default

Do not:
- rerun hypotheses or weaken M1-M5 gates
- edit `active_strategy` unless the user explicitly asks for that separate execution step
- write orders, restart Bhiksha, or change live execution state
- edit `Mala_Evidence_v1` by hand; it is Mala-owned and regenerated
- treat legacy Strategy_Catalog JSON blobs as runtime truth

## Workflow

1. Inspect the live sheets and active plan if credentials/server access are available.
2. Regenerate/read current Mala evidence:
   ```bash
   ./.venv/bin/python -m src.research.mala_handoff
   ```
3. Compile Bhiksha dry-run from the sheets:
   ```bash
   ssh oldmac 'cd ~/Documents/bhiksha && ./.venv/bin/python -m bhiksha.tools.compile_active_plan --google-sheet-id <sheet_id> --out /tmp/bhiksha_active_plan_review.json'
   ```
4. Compare evidence tier, active authorization, and compiled `execution.shadow_only`.

## Codex CLI on oldmac

When delegating this review to Codex CLI over SSH on oldmac, launch through a
login shell so `~/.zprofile` exposes Node and the Codex binary:

```bash
ssh oldmac 'zsh -lc "cd ~/Documents/mala_v2 && codex exec \"Use the catalog-steward skill. Review Mala_Evidence_v1, Operator_Defaults_v1, active_strategy, and Bhiksha dry compile output. Produce artifact-only recommendations.\""'
```

## Recommendation Vocabulary

- `live`: eligible for human-reviewed live authorization in `active_strategy`
- `shadow`: add or keep shadow in `active_strategy`
- `hold`: keep in `Mala_Evidence_v1` only
- `pause`: disable or remove from `active_strategy`

`active_strategy.enabled` and `active_strategy.authorization_mode` remain the
execution authority. Mala evidence and steward notes are advisory.
