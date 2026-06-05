# Mala Playbook Evidence V2

`Mala_Playbook_Evidence_v2` is the proposed row-level passport for playbook
packets. It is separate from `Mala_Evidence_v1`, which remains the strategy-row
passport for the older `M1-M7` lane.

The goal is to publish what Mala discovered in the playbook lane in a form that
Bhiksha can adopt without reading research prose or inferring execution intent.

## Ownership

- Mala owns playbook discovery, packet identity, surface classification,
  management-policy evidence, parity references, and promotion status.
- Bhiksha owns runtime capability, option preview, shadow lifecycle, fills,
  PnL, broker reconciliation, and defect evidence.
- The operator owns authorization for live approval and autonomous-control
  packets.

## Row Shape

One row represents one packet-versioned playbook variant, not an entire broad
surface.

| Column | Owner | Meaning |
| --- | --- | --- |
| `mala_playbook_evidence_version` | Mala | Contract version, initially `2`. |
| `playbook_id` | Mala | Durable playbook scope, for example `mean-reversion-at-extremes-intraday`. |
| `exploration_universe` | Mala | Initial symbol universe, for example `iwm_qqq`. |
| `playbook_packet_id` | Mala | Packet id consumed by Bhiksha. |
| `playbook_packet_version` | Mala | Packet version consumed by Bhiksha. |
| `execution_packet_id` | Mala/Bhiksha | Execution packet id for shadow/live handoff. |
| `shadow_execution_packet_version` | Mala/Bhiksha | Approved shadow packet version. |
| `live_execution_packet_version` | Mala/Bhiksha | Approved live approval-gated packet version, if any. |
| `symbol_scope` | Mala | Symbols in the packet scope. |
| `direction_scope` | Mala | Direction or directions in the packet scope. |
| `primary_candidate_config_id` | Mala | Locked surface config id. |
| `surface_match_grade` | Mala | `favorable`, `near_favorable`, `partial`, or `outside`. |
| `surface_candidate_count` | Mala | Number of reviewed candidate rows in the run. |
| `surface_favorable_count` | Mala | Number of strict favorable regions. |
| `sample_count` | Mala | Historical sample count for the primary candidate. |
| `holdout_count` | Mala | Holdout sample count for the primary candidate. |
| `calibration_expectancy_r` | Mala | Underlying R expectancy in calibration. |
| `holdout_expectancy_r` | Mala | Underlying R expectancy in holdout. |
| `management_policy_ids` | Mala | Packet-declared management policies Bhiksha may use. |
| `parity_report` | Mala | Path/id for the Mala/Bhiksha signal parity report. |
| `parity_status` | Mala | `passed`, `review`, or `block`. |
| `p_gate_status_json` | Mala | Current P1-P7 gate statuses from `playbook_automation_gates`. |
| `bhiksha_shadow_status` | Bhiksha | `not_started`, `running`, `sample_small`, `pass`, `block`, or `defect`. |
| `shadow_closed_count` | Bhiksha | Closed executable shadow trades. |
| `shadow_avg_option_r` | Bhiksha | Average executable option R from shadow. |
| `shadow_runtime_defect_count` | Bhiksha | Runtime defects attached to the packet version. |
| `shadow_feedback_artifact` | Bhiksha | Path/id to the shadow outcome artifact. |
| `promotion_verdict` | Mala | `shadow`, `promote_review`, `live_approval_gated`, `retune`, `kill`, or `autonomy_blocked`. |
| `promotion_reason` | Mala | Human-readable reason for the verdict. |
| `next_action` | Mala | The next concrete action for this packet. |
| `updated_at` | Mala | Evidence row generation timestamp. |

## Adoption Rules

Bhiksha may shadow a row when:

- `P1`, `P2`, `P3`, and `P4` pass.
- The execution packet is approved with `runtime_mode=shadow`.
- `shadow_only=true` and `live_automated_allowed=false`.

Bhiksha may prepare a live approval-gated pilot only when:

- `P5` passes from closed shadow feedback.
- A live execution packet is approved with `runtime_mode=live_approval_gated`.
- `live_ticket_required=true` and `live_automated_allowed=false`.

Bhiksha may not run autonomous live playbook execution until:

- P7 passes through a separate autonomous-control packet.
- Mala ingests shadow/live feedback as first-class evidence.
- The operator explicitly authorizes the autonomous-control packet.

## Local Publisher

Generate the current local evidence artifacts with:

```bash
python -m src.research.playbook_evidence_v2 \
  --run-dir research/results/playbooks/mean_reversion_at_extremes/current \
  --playbook-packet packets/playbook/playbook.mean_reversion_at_extremes.iwm_qqq/v1.json \
  --shadow-execution-packet packets/execution/execution.mean_reversion_at_extremes.iwm_qqq/v1.json \
  --live-execution-packet packets/execution/execution.mean_reversion_at_extremes.iwm_qqq/v2.json
```

Outputs:

- `Mala_Playbook_Evidence_v2.csv`
- `Mala_Playbook_Evidence_v2.json`

## Current First Row

The current first row is the IWM/QQQ exploration packet for
`mean-reversion-at-extremes-intraday`. The strongest discovered candidate is
the IWM short prior-RTH-close ATR stretch region with `reversal_extreme` stop
and `fixed_1r` target. QQQ remains evidence-bearing but not the primary
promotion candidate.
