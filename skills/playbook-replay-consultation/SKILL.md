---
name: playbook-replay-consultation
description: Use when the user is manually reviewing historical Thinkorswim or chart replay moments against a Mala playbook consultation surface. The trader supplies chart-first judgment; the agent runs query/policy/close commands and preserves the replay evidence trail.
---

# Playbook Replay Consultation

## Scope

Use this skill inside `mala_v2` when the user is doing manual historical chart
replay and wants Mala to act as a consultation/accounting layer.

This is not strategy discovery, live execution, or M1-M5 promotion. It is a
human-in-the-loop review loop:

1. The trader identifies a real timestamp from the chart.
2. Mala scores that timestamp against the historical playbook surface.
3. The trader decides take/pass and, if taking, selects one management row.
4. Mala closes the replay row and computes historical actuals from cached bars.

## First Files

Read only what the task needs:

1. `agent.md`
2. `docs/PLAYBOOK_REPLAY_CONSULTATION_SOP.md`
3. `docs/PLAYBOOK_CONSULTATION_LAYER.md` when the user asks how the lane
   should be used operationally
4. the current run's `RECEIPT.md` if the run directory is unclear
5. generated `QUERY_REVIEW.md`, `POLICY_CARD.md`, and `consultation_log.csv`
   for the specific replay timestamp

Default current run:

```bash
RUN_DIR=data/results/playbooks/mean_reversion_at_extremes/20260515T_clean_rth_iwm_qqq_surface64
```

If the user names a different playbook or run, use that run directory instead.

## Operator Contract

The user should provide:

```text
date, time CT, symbol, direction, chart read
```

Example:

```text
2026-05-11, 09:40 CT, IWM, short, stretched up hard and starting to fail; would consider a fast fade with stop above high.
```

Do not run the query until the user has supplied at least a minimal chart read.
The chart read can be messy, but it must exist before Mala output is shown.

Assume user-provided times are Central time (`America/Chicago`) unless the user
explicitly says otherwise. The query command should pass the timestamp with
`America/Chicago`; generated artifacts and query IDs may show the converted ET
timestamp. When summarizing results, keep the user's original CT time in the
trader-facing recap and mention ET only when referencing generated artifact IDs
or engine output.

## Consultation Loop

### 1. Run The Query

Use the repo venv:

```bash
./.venv/bin/python -m src.research.playbook_surface_query \
  --run-dir "$RUN_DIR" \
  --symbol <SYMBOL> \
  --direction <long|short> \
  --timestamp "<YYYY-MM-DD HH:MM America/Chicago>" \
  --mode state-management
```

Capture:

- `query_id`
- `QUERY_REVIEW.md`
- `query_result.json`
- printed verdict

If the timestamp is outside the playbook entry window, say that plainly and
report the tool's out-of-window/state-management framing. Do not force it into
an entry setup.

### 2. Generate The Policy Card

```bash
./.venv/bin/python -m src.research.playbook_policy_card \
  --query-json "$RUN_DIR/surface_queries/<query_id>/query_result.json" \
  --update-log
```

Read `POLICY_CARD.md`.

Summarize in trader language:

- whether the timestamp is in-window or off-playbook
- verdict/policy stance
- `STATE` percentile context: VWAP stretch, prior-close ATR stretch, and velocity
- `ANALOG` quality: tight/workable/loose/thin cohort plus similarity tail
- historical cohort quality and warnings
- best or most realistic management rows
- any risk issue, especially unusably tight risk
- what would make this a pass even if the card is constructive

Do not treat `POLICY: take` as authorization. The trader decides.

### 3. Wait For Trader Decision

Ask for only the missing decision fields:

- `take` or `pass`
- if `take`, one `exit_family`
- short operator note if not already obvious

Valid common exit families include:

- `scalp_0.15pct`
- `scalp_0.25pct`
- `scalp_0.35pct`
- `retrace_to_vwap_25pct`
- `retrace_to_vwap_50pct`
- `vwap_return`

Only use an exit row the user says they would actually follow.

### 4. Replay-Close The Row

For take:

```bash
./.venv/bin/python -m src.research.playbook_consultation_log replay-close \
  --run-dir "$RUN_DIR" \
  --query-id <query_id> \
  --taken Y \
  --selected-exit <exit_family> \
  --historical \
  --operator-note "<short trader note>"
```

For pass:

```bash
./.venv/bin/python -m src.research.playbook_consultation_log replay-close \
  --run-dir "$RUN_DIR" \
  --query-id <query_id> \
  --taken N \
  --historical \
  --operator-note "<short trader note>"
```

After closing, report:

- `actual_exit_reason`
- `actual_pnl_r`
- `actual_time_to_exit`
- `actual_exit_ts_et`
- whether the card helped, hurt, or was inconclusive based on the user's note

Do not manually infer actual PnL from Thinkorswim unless the replay close tool
is missing data.

## Batch Hygiene

A useful batch is 8-12 closed rows.

Aim to include:

- IWM and QQQ when both are real candidates
- both take and pass decisions
- moments where the card agrees and disagrees with the trader
- imperfect and ambiguous trades, not only obvious winners

Use:

```bash
./.venv/bin/python -m src.research.playbook_consultation_log list \
  --run-dir "$RUN_DIR" \
  --open-only
```

For a batch-level status and next action:

```bash
./.venv/bin/python -m src.research.playbook_consultation_log status \
  --run-dir "$RUN_DIR"
```

The batch is complete when intended review rows are closed.

## Guardrails

- Chart first, Mala second.
- The trader's chart read is subjective evidence, not something to correct
  before running the query.
- Mala supplies historical analogs and accounting; it does not authorize trades.
- Pass rows are evidence. Do not discard them.
- Do not start from `sample_events.csv` when the user is doing unbiased replay.
- Do not mix historical `replay-close` with live/manual close semantics.
- Keep summaries short and decision-oriented during chart review.

## Skill Evolution Notes

When the workflow improves, update this skill with the durable rule, not the
one-off event.

Good additions:

- a clearer default run directory
- new accepted exit families
- a better card summary template
- a new warning that repeatedly prevents bad review rows

Avoid adding:

- individual trade conclusions
- stale run-specific performance claims
- chat transcripts
- speculative live-execution rules
