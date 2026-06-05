# Playbook Promotion Gates

This document defines the playbook lane from surface discovery to Bhiksha
shadow, live approval, and eventual automation. The gate names use `P` because
this is the playbook lane, not the older strategy `M1-M7` lane.

## Mental Model

```text
Playbook thesis
-> P1 surface map
-> P2 packet freeze
-> P3 Mala/Bhiksha parity
-> P4 Bhiksha shadow authorization
-> P5 shadow feedback
-> P6 live approval-gated pilot
-> P7 autonomous-control approval
```

Shadowing is a feedback mechanism, not a capital-risk event. A locked stress
artifact is useful evidence, but it must not block shadow when the packet is
shadow-only, parity-clean, and live automation is disabled.

The intended end state is strategy-lane-style automation: Mala publishes a
packet after playbook-specific discovery and gates, then Bhiksha adopts the
packet autonomously when runtime capability, parity, feedback, and approval
contracts are satisfied. Manual consultation is a proof and audit mechanism,
not the steady-state trading workflow.

## Gate Contract

1. **P1 Surface Gate**
   - Input: `surface_review/candidate_regions.csv`
   - Pass: at least one `favorable` candidate region.
   - Review: only `near_favorable` regions exist.
   - Block: no favorable or near-favorable surface.

2. **P2 Packet Freeze Gate**
   - Input: reviewed playbook packet for one narrowed playbook variant.
   - Pass: packet exists. Optional locked stress evidence is recorded if present.
   - Review: optional stress evidence exists but did not pass; shadow may continue only as an explicitly marked scout.
   - Block: no packet or P1 did not pass.

3. **P3 Parity Gate**
   - Input: Mala/Bhiksha signal parity report.
   - Pass: parity status is `passed`, with zero missing and zero extra events.
   - Block: any unexplained signal drift.

4. **P4 Shadow Authorization Gate**
   - Input: approved Bhiksha shadow execution packet.
   - Pass: `runtime_mode=shadow`, `status=approved`, `shadow_only=true`, and `live_automated_allowed=false`.
   - Review: parity passed but no shadow packet is attached.
   - Block: packet is not explicitly shadow-only or live automation is enabled.

5. **P5 Shadow Feedback Gate**
   - Input: closed shadow outcomes with option R and runtime-defect flags.
   - Pass: enough closed trades, positive average option R, and zero runtime defects.
   - Review: shadow is authorized but evidence is missing or sample is still too small.
   - Block: negative executable option R or runtime defects.

6. **P6 Live Approval Gate**
   - Input: approved live approval-gated execution packet.
   - Pass: `runtime_mode=live_approval_gated`, packet is approved, live ticket is required, and live automation is disabled.
   - Review: shadow passed but no live approval-gated packet is attached.
   - Block: no approval, no ticket requirement, or hidden live automation.

7. **P7 Automation Gate**
   - Always blocked until shadow/live feedback is ingested back into Mala and a separate autonomous-control packet is approved.

## Relationship To M1-M7

The strategy lane uses `M1-M7` to prove a strategy row before shadow/live
handoff. The playbook lane uses `P1-P7` because playbooks are packetized
decision contracts rather than generic strategy rows.

| Strategy lane | Playbook lane |
| --- | --- |
| `M1-M5` historical strategy proof | `P1` surface and `P2` packet freeze |
| `M6` option translation | `P4/P5` Bhiksha shadow option preview and executable feedback |
| `M7` provider translation | `P3` parity plus Bhiksha packet compile/runtime capability |
| `active_strategy` authorization | approved playbook execution packet |
| shadow campaign evidence | `P5` shadow feedback and Mala evidence ingestion |

## Tool

```bash
python -m src.research.playbook_automation_gates \
  --run-dir research/results/playbooks/mean_reversion_at_extremes/current \
  --playbook-packet packets/playbook/playbook.mean_reversion_at_extremes.iwm_qqq/v1.json \
  --parity-report research/results/playbook_parity/playbook.mean_reversion_at_extremes.iwm_qqq/20260516T205756Z/PARITY_REPORT.json \
  --shadow-execution-packet packets/execution/execution.mean_reversion_at_extremes.iwm_qqq/v1.json \
  --shadow-outcomes <shadow_outcomes.csv> \
  --live-execution-packet packets/execution/execution.mean_reversion_at_extremes.iwm_qqq/v2.json
```

`--execution-packet` remains a backward-compatible alias for
`--shadow-execution-packet`.

Outputs:

- `PLAYBOOK_AUTOMATION_GATES.json`
- `PLAYBOOK_AUTOMATION_GATES.md`

`overall_status=shadow_ready` means P1-P4 passed and the next action is to run
or continue Bhiksha shadow. It does not mean live or autonomous approval.

## Current Policy

After P1-P4 pass, Bhiksha should shadow the packet and produce real feedback.
P5 decides whether shadow taught us enough to promote, retune, or kill. Fully
automated live trading remains blocked until P7 has explicit feedback
ingestion and autonomous-control approval.
