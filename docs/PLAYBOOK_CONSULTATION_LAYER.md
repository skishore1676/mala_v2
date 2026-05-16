# Playbook Consultation Layer

This is the human-in-the-loop lane for using a Mala playbook before it becomes
an automated Bhiksha execution packet.

The current supported playbook is:

- `mean-reversion-at-extremes-intraday`
- symbols: `IWM`, `QQQ`
- packet: `playbook.mean_reversion_at_extremes.iwm_qqq` version `1`
- runtime state: entry-signal parity passed for Bhiksha shadow support
- execution state: not live automated; management is still operator-selected

## Roles

```text
Trader        chart-first read, take/pass, management choice
Mala          state query, historical analogs, policy card, replay accounting
Bhiksha       shadow/runtime adapter after packet authorization
Shared kernel packet id, feature contract, parity report, capability manifest
```

Mala may say the timestamp is constructive, mixed, or off-playbook. That is not
authorization. The trader still decides take or pass.

## Consultation Flow

1. The trader starts from the chart and writes the timestamp, symbol, direction,
   and chart read before seeing Mala output.
2. Mala runs `playbook_surface_query` in `state-management` mode.
3. Mala writes `QUERY_REVIEW.md` and `query_result.json`.
4. Mala generates `POLICY_CARD.md`.
5. The trader decides:
   - `pass`, or
   - `take` plus one management row they would actually follow.
6. Mala closes the consultation row:
   - historical review uses `replay-close` and cached bars
   - live/manual review uses manually supplied execution outcome later
7. The batch is reviewed for whether consultation helped the decision.
8. Only after enough useful closed rows does the playbook move toward an
   execution packet and Bhiksha-managed shadow.

## Default Commands

Set the current run once:

```bash
RUN_DIR=data/results/playbooks/mean_reversion_at_extremes/20260515T_clean_rth_iwm_qqq_surface64
```

Run a chart-first query:

```bash
./.venv/bin/python -m src.research.playbook_surface_query \
  --run-dir "$RUN_DIR" \
  --symbol IWM \
  --direction short \
  --timestamp "2026-05-11 09:40 America/Chicago" \
  --mode state-management
```

Generate the policy card:

```bash
./.venv/bin/python -m src.research.playbook_policy_card \
  --query-json "$RUN_DIR/surface_queries/<query_id>/query_result.json" \
  --update-log
```

Close a take:

```bash
./.venv/bin/python -m src.research.playbook_consultation_log replay-close \
  --run-dir "$RUN_DIR" \
  --query-id <query_id> \
  --taken Y \
  --selected-exit scalp_0.25pct \
  --historical \
  --operator-note "Would take; fast scalp only."
```

Close a pass:

```bash
./.venv/bin/python -m src.research.playbook_consultation_log replay-close \
  --run-dir "$RUN_DIR" \
  --query-id <query_id> \
  --taken N \
  --historical \
  --operator-note "Would pass; not clean enough for options scalp."
```

Check the batch:

```bash
./.venv/bin/python -m src.research.playbook_consultation_log status \
  --run-dir "$RUN_DIR"
```

`NEXT_ACTION` means:

- `start_chart_first_query`: no consultation rows yet
- `close_open_consultation_rows`: finish unresolved queries
- `add_more_chart_first_rows`: continue toward an 8-12 row review batch
- `review_closed_batch_before_promotion`: stop and judge whether the layer is
  helping before promoting anything

## What Bhiksha Does Not Do Yet

Bhiksha can recompute the entry signal for this playbook in shadow parity mode.
It does not yet select an option, arm an execution packet, manage exits, or
write the full live feedback loop for this playbook.

That next bridge is:

```text
closed consultation batch
  -> operator review
  -> execution packet review/approval
  -> Bhiksha shadow option-selection and management adapter
  -> feedback artifact back to Mala
```

The current execution-packet draft is:

```text
packets/execution/execution.mean_reversion_at_extremes.iwm_qqq/v1.json
```

It is intentionally `status=review` with pending operator approval. Bhiksha
should compile it as blocked until the operator approves the packet and the
legacy-retirement gate is clear.

## Promotion Rule

The consultation layer earns automation only if the closed batch shows that it
improves judgment or management. A good card that the trader would not actually
follow is still a pass for automation.
