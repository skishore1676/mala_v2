# Mala Evidence Old Book Archive - 2026-05-17

This archive closes the pre-refactor Mala evidence book after the full
current-transform sweep.

## Archived Inputs

- Old baseline: `data/results/mala_evidence_full_sweep_20260517/old_baseline_candidates.csv`
- New sweep: `data/results/mala_evidence_full_sweep_20260517/new_sweep_candidates.csv`
- Old-vs-new verdict: `data/results/mala_evidence_full_sweep_20260517/OLD_VS_NEW_VERDICT.md`
- Clean handoff packet: `data/results/mala_handoff_full_sweep_20260517_no_stale/MALA_HANDOFF_CANDIDATES.csv`
- Clean rework manifest: `data/results/mala_evidence_rework/20260517T041304Z/MALA_EVIDENCE_REWORK.md`

## Final Counts

- Old unique M5-selected candidates: 35
- New unique M5-selected candidates: 31
- Old candidates that reappeared at M5: 26 / 35
- Old candidates that re-earned actionable shadow/promote: 14 / 35
- Clean current handoff rows: 31
- Current actionable shadow rows: 16
- Current Bhiksha-ready rows: 14

## Decision

The old book is archived. `Mala_Evidence_v1` should carry only the clean
current handoff packet generated after the full sweep. Dropped, demoted, and
stale rows should not remain in the active evidence surface.

Rows with `bhiksha_ready=false` remain evidence rows only; they are not runtime
authorization rows.
