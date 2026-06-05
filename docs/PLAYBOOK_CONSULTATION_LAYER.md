# Playbook Consultation Layer

This is the evidence and replay lane for using a Mala playbook before it
becomes an automated Bhiksha execution packet.

The current supported playbook is:

- `mean-reversion-at-extremes-intraday`
- current exploration universe: `IWM`, `QQQ`
- current symbol-scoped packet: `playbook.mean_reversion_at_extremes.iwm_qqq` version `1`
- runtime state: entry-signal parity passed for Bhiksha shadow support
- execution state: approval-gated packet exists; full automation still needs
  closed feedback and autonomous-control approval

Naming convention: `mean-reversion-at-extremes-intraday` is the playbook.
`iwm_qqq` is only the first explored universe suffix. Future symbol groups
should get their own symbol-scoped packet suffixes without renaming the
playbook itself.

The long-term product is not manual screen trading. This layer exists to
produce evidence, replay feedback, and management-policy proof so Bhiksha can
eventually adopt a locked packet the way it adopts Mala strategy-lane rows:
published evidence, explicit authorization, runtime capability, shadow/live
feedback, then autonomous execution approval.

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
8. Useful closed rows improve promotion evidence, but Bhiksha-managed shadow
   can start once P1-P4 pass because it is no-capital feedback collection.

## Default Commands

Set the current run once:

```bash
RUN_DIR=research/results/playbooks/mean_reversion_at_extremes/current
```

The current symlink resolves to:

```text
research/results/playbooks/mean_reversion_at_extremes/20260515T_clean_rth_iwm_qqq_surface64
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

Bhiksha can recompute the entry signal for this playbook in shadow parity mode
and can call this consultation layer from an approved shadow execution packet.
That is the backend for a future Trader Desk button: the operator supplies the
chart read, Bhiksha verifies the packet, Mala produces the query/policy card,
and Bhiksha records the bridge artifact.

Bhiksha can now select an option preview, create an approval ticket, submit a
packet-native lifecycle for a promoted packet, and monitor the underlying stop
anchor after entry. The full feedback loop back into Mala is still the next
research-review layer.

After a consultation is produced, Bhiksha can now also record the operator's
red/green decision and selected management policy as a shadow execution intent.
That intent is still `order_submission_allowed=false`; it is the handoff into
future option preview and live approval, not an order ticket.

For a `shadow_intent_ready` or `live_intent_ready` row, Bhiksha can also build
an option preview using its existing chain, quote, and risk checks. The preview
still requires live approval and keeps `order_submission_allowed=false`. For
the live-gated packet, the preview must include the underlying stop price that
will be monitored after entry.

Bhiksha now treats shadow and live as parallel lanes after option preview:

- shadow lane records the option exit mark and PnL so the playbook can be
  judged by actual executable vehicle outcomes
- live lane creates an explicit approval ticket, and the v2
  `live_approval_gated` packet can turn that ticket into a managed lifecycle
  only through the submitter command

For the v2 `live_approval_gated` packet, Bhiksha can consume the approved live
ticket and start the managed lifecycle: entry submission, fill/reconciliation,
protective stop, target or virtual target, trade-state persistence, and later
position management. The v1 reversion packet remains shadow-only and is blocked
from that submitter.

Management is now packet-declared. The execution packet carries:

```text
runtime_controls.management_policy_specs
```

Each selected policy includes stop family, stop anchor, exit family, target
model, target R, hard-flat time, option stop fallback, and source config id.
Bhiksha option preview and lifecycle submission read that spec instead of
silently mapping policy IDs to local defaults.

That next bridge is:

```text
P1-P4 packet approval
  -> Bhiksha shadow
  -> closed consultation and executable option feedback
  -> Mala ingestion
  -> promotion, retune, or kill
  -> autonomous-control review
```

The older manual proof loop remains useful when chart semantics are unclear:

```text
closed consultation batch
  -> operator review
  -> execution packet review/approval
  -> Bhiksha operator decision and shadow intent
  -> Bhiksha option preview and risk check
  -> Bhiksha shadow outcome PnL and/or live approval ticket
  -> Bhiksha packet-native submitter and lifecycle management
  -> Bhiksha underlying-anchor monitor
  -> feedback artifact back to Mala
```

The current IWM/QQQ exploration shadow execution packet is:

```text
packets/execution/execution.mean_reversion_at_extremes.iwm_qqq/v1.json
```

It is `status=approved` for Bhiksha shadow-only activation. It is not approved
for live automated execution, and its runtime controls keep `shadow_only=true`
and `live_automated_allowed=false`.

The current IWM/QQQ exploration live-approval-gated execution packet is:

```text
packets/execution/execution.mean_reversion_at_extremes.iwm_qqq/v2.json
```

It is `runtime_mode=live_approval_gated`. It does not open autonomous trading:
`live_automated_allowed=false`, every order requires an approved live ticket,
the first pilot is capped at one contract / $300 premium, and option preview
requires the underlying stop price.

## Promotion Rule

The playbook layer earns automation only if closed replay/shadow/live feedback
shows that the locked packet is useful and executable. Consultation artifacts
are evidence-gathering tools, not the desired steady-state operator workflow.
