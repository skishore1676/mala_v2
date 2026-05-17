# Mala Evidence M5.5 Option-Exit Archive - 2026-05-17

This archive closes the pre-M5.5 evidence book after the option-aware exit
rerun and Bhiksha shadow activation rebuild.

## Current Artifacts

- Option-exit sweep log: `data/results/mala_evidence_option_exit_20260517/sweep_run_log.jsonl`
- New handoff packet: `data/results/mala_handoff_option_exit_20260517_publish/MALA_HANDOFF_CANDIDATES.csv`
- Old-vs-new option verdict: `data/results/mala_evidence_option_exit_20260517/OLD_VS_NEW_OPTION_EXIT_VERDICT.md`
- Sheet readback validation: `data/results/mala_sheet_readback_validation/20260517T123236Z/SHEET_READBACK_VALIDATION.md`
- Shadow activation packet: `data/results/shadow_campaign/activation/20260517_option_exit_applied/SHADOW_ACTIVATION_PACKET.md`

## Final Counts

- Current evidence rows: 36
- Option-trade-ready rows: 33
- Runtime and option-ready shadow rows: 13
- Blocked evidence-only rows: 23
- Published sheet readback mismatches: 0 headers, 0 cells
- Bhiksha active-plan deployments: 13 strategy shadow rows plus 1 manual row
- Bhiksha suppressed rows: 0

## Legacy Retirement

The old static Bhiksha strategy catalog/deployment wires were archived on both
the local Bhiksha checkout and oldmac under:

- `config/archive/legacy_retired_20260517/deployments/`
- `config/archive/legacy_retired_20260517/strategy_catalog/`

The retired static strategy IDs were:

- `market_impulse_spy_short_v1`
- `market_impulse_qqq_short_v1`
- `jerk_pivot_momentum_tsla_short_v1`

The final oldmac active plan contains no legacy static catalog entries and no
legacy deployments. Fresh runtime authorization now comes only from the
current `Mala_Evidence_v1` plus `active_strategy` packet.
