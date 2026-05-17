# Playbook Automation Gates

This document defines the automated playbook lane. It is deliberately stricter
than the consultation lane and different from the generic strategy lane.

## Mental Model

```text
Playbook thesis
-> surface map
-> locked packet
-> locked validation
-> Mala/Bhiksha parity
-> shadow execution
-> live approval-gated pilot
-> feedback ingestion
-> separate autonomous-control approval
```

Bhiksha should never automate a vague playbook. It can only automate an
approved packet version with evidence attached at each gate.

## Gate Contract

1. **Surface Gate**
   - Input: `surface_review/candidate_regions.csv`
   - Pass: at least one `favorable` candidate region.
   - Review: only `near_favorable` regions exist.
   - Block: no favorable or near-favorable surface.

2. **Locked Validation Gate**
   - Input: reviewed playbook packet plus locked validation artifact.
   - Pass: locked validation status is `passed`.
   - Review: packet exists but locked validation/stress evidence is missing.
   - Block: no packet or failed validation.

3. **Parity Gate**
   - Input: Mala/Bhiksha signal parity report.
   - Pass: parity status is `passed`, with zero missing and zero extra events.
   - Block: any unexplained signal drift.

4. **Shadow Execution Gate**
   - Input: closed shadow outcomes with option R and runtime-defect flags.
   - Pass: enough closed trades, positive average option R, and zero runtime defects.
   - Review: parity passed but sample is still too small.
   - Block: negative executable option R or runtime defects.

5. **Live Approval Gate**
   - Input: approved live approval-gated execution packet.
   - Pass: packet is approved, live ticket is required, and live automation is still disabled.
   - Block: no approval, no ticket requirement, or hidden live automation.

6. **Automation Gate**
   - Always blocked until live/shadow feedback is ingested back into Mala and a separate autonomous-control packet is approved.

## Tool

```bash
python -m src.research.playbook_automation_gates \
  --run-dir data/results/playbooks/mean_reversion_at_extremes/<run> \
  --playbook-packet <packet.json> \
  --locked-validation <locked_validation.json> \
  --parity-report <PARITY_REPORT.json> \
  --shadow-outcomes <shadow_outcomes.csv> \
  --execution-packet <execution_packet.json>
```

Outputs:

- `PLAYBOOK_AUTOMATION_GATES.json`
- `PLAYBOOK_AUTOMATION_GATES.md`

## Current Policy

The playbook lane can reach approval-gated live pilot after clean shadow
evidence. It cannot reach fully automated live trading until feedback ingestion
and autonomous-control approval exist.
