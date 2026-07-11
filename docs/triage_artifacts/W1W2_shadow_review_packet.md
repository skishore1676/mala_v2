# Complete Triage — CONSOLIDATED review packet (2026-07-10)

10 shadow-ready candidates across trend + reversion families. Every candidate cleared ALL gates:
- multi-regime survival (deployable recent + robust prior era, **direction-consistent**)
- operator-validated yardstick (mc_prob≥0.70, profile-exit option-path>0; win/payoff = context)
- profile-exit + management_policy_spec mapped
- bhiksha_capability_status = supported
- M7 Schwab-vs-Polygon provider parity ≥0.90 (computed via 1-month oldmac fetch)

**The only step left is your publish.** Staged (NOT written): data/results/triage_stage/ALL__staged_evidence.csv

| strategy | sym | dir | profile | prof-exit% | M7 | cap | tier | win | payoff | trades |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Elastic Band Reversion | BA | long | Exhaustion Reversal | +19.0% | 99.0% | supported | shadow | 0.53 | 6.28 | 30 |
| Elastic Band Reversion | SNOW | short | Exhaustion Reversal | +17.1% | 98.5% | supported | shadow | 0.52 | 1.20 | 25 |
| Opening Drive Classifier | SNOW | short | Trend Continuation | +16.3% | 100.0% | supported | shadow | 0.68 | 2.12 | 19 |
| Market Impulse | QQQ | short | Trend Continuation | +10.5% | 95.2% | supported | shadow | 0.47 | 3.76 | 53 |
| Market Impulse | PDD | long | Trend Continuation | +8.9% | 96.5% | supported | shadow | 0.49 | 2.32 | 45 |
| Market Impulse | WFC | long | Trend Continuation | +8.6% | 97.9% | supported | shadow | 0.47 | 4.05 | 45 |
| Opening Drive Classifier | BAC | short | Trend Continuation | +8.4% | 99.9% | supported | shadow | 0.48 | 1.99 | 23 |
| Market Impulse | RBLX | long | Trend Continuation | +5.0% | 97.9% | supported | shadow | 0.47 | 3.13 | 34 |
| Market Impulse | XOM | long | Trend Continuation | +1.5% | 97.2% | supported | shadow | 0.51 | 1.50 | 51 |
| Market Impulse | AXP | long | Trend Continuation | +0.4% | 97.1% | supported | shadow | 0.51 | 4.24 | 35 |

## Notes
- Sturdiest (higher mc_prob + more trades + DNA-band win): XOM/AXP/PDD long, BA long, QQQ short.
- Thinnest (flagged, still honest gate-passers): SNOW (both families), BAC — low trade counts.
- SNOW appears twice (opening_drive TREND short + elastic_band EXHAUSTION short) — distinct strategies.
- Option-path magnitudes use modeled IV (not a runtime blocker; bhiksha uses no IV — shadow accrues real IV/fills).
- Adversarial disprove pass runs pre-PROMOTE, after shadow (shadow is the next thinning stage).
