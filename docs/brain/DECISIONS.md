---
title: DECISIONS — the ADR ledger (operator decisions + their why)
as_of: 2026-07-09
maintainer: brain steward (candidate) → operator gate
sources:
  - WP  = docs/LIVE_LOOP_WORKPLAN.md            # status board + § sections + diary
  - VP  = docs/VEHICLE_POLICY_DECISION.md       # read-only data pull, 2026-07-08
  - RFC = docs/RESIDENT_INTELLIGENCE_RFC.md     # §9a operator decisions
  - SUP = bhiksha/src/bhiksha/execution/supervisor.py
  - CMP = bhiksha/src/bhiksha/active_plan/compiler.py
  - L-money = bhiksha/docs/lessons/money-path-audit-rounds-catch-different-bug-classes.md
  - L-sheet = bhiksha/docs/lessons/sheet-is-the-operator-control-surface.md
  - MEM = ~/.claude/projects/-Users-suman-code-mala-v2/memory/   # personal DNA stays here, by reference
---

# DECISIONS — ADR ledger

Every **operator** decision about the live loop, with the *why* in Suman's terms, sources, and
status. Personal trading DNA (universe, exit playbooks, P&L narrative) is held **by reference** in
`MEM/operator-trading-profile.md` + `MEM/exit-profile-gap.md`, not embedded. Dollar figures appear
only where the number *is* the rationale (rail arithmetic). Trust order: runtime evidence > diary >
this file. Status: **active · superseded · rejected · planned**.

---

### ADR-000 — Flip to profile exits for a month; expand to shadow; build MECHANICAL rails (2026-07-01)
- **Decision:** Run a month-long live test where profile-ladder exits drive the 5 live lanes; expand the book 6→19 lanes (rest shadow); build risk rails as the safety layer.
- **Why (operator):** June ≈ −$1,000 over ~20 live trades read as *sample starvation, not proven no-edge* — profile exits were armed in the Sheet since ≤Jun 23 but never dispatched (deploy gap, then a reconciliation bug closing the gate). "Prove the exit DNA with real fills." Rails "must be mechanical, not vigilance-based." Founds the whole experiment.
- **Sources:** WP diary 07-01/07-02; MEM/live-experiment-status-2026-07.md.
- **Status:** active (running; +$1,348 live over 9 trades through 07-09).

### ADR-001 — Exit-authority rule: profile-armed ⇒ profile owns ALL profit-taking (2026-07-02)
- **Decision:** When a deployment's profile exit is armed live, the profile ladder owns ALL profit-taking — **no full-size resting target, no virtual-target machinery, no config override.** The protective STOP always stays resting at the broker. **NON-OVERRIDABLE.**
- **Why (operator):** On flip day NVDA and AMD carried *both* a full +35% resting target and an armed ladder; the target filled broker-side before the ladder could bank its 60% T1 partial → both exited 100% at 1R, T2 runner structurally impossible. Lowering T1 wouldn't fix it (the full target still amputates the runner). Crash cost of the rule = a missed partial, never an unprotected position.
- **Enforcement (verified):** single choke point `_profit_target_configured` (`SUP:4669`) returns `False` whenever `_profile_owns_profit_taking` (`SUP:4606`; docstring states the rule + "non-overridable by config"). Emits `profit_target_suppressed: profile_owns_profit_taking` per armed entry; shadow-only lanes keep target machinery. Confirmed live 07-06 (IWM), 07-08/07-09 (QQQ full ladder).
- **Sources:** WP §2, board #2, diary 07-02/07-06/07-08/07-09; SUP:4606,4669; bhiksha/README.md.
- **Status:** active.

### ADR-002 — Risk-rail calibration: catastrophe brake, not a per-trade stop (2026-07-02 → revised 2026-07-08)
- **Decision:** Two-tier daily-drawdown rail (tier-1 halts new entries, tier-2 flattens the book). Calibrated **halt ≈$500 / flatten ≈$730 → set 5.0% / 7.5%** (07-02); **operator-revised to 7.5% / 11.25% on 07-08 "till the account grows"** (headroom for a multi-lot book). Ratio held ~1:1.5. Resolution order: **env > `Operator_Defaults_v1` sheet > default.**
- **Why (operator):** Rails are a *catastrophe brake on an abnormal day*, not a per-trade stop — the 35% premium stop already bounds each trade. One worst-case stop-out at the $2,000 cap = −$700, which stays *under* the 7.5% halt (≈−$768 on the 07-08 $10,237 budget) → survives one loss, halts before two. The original 2%/3% would wrongly end a day on a single routine stop (validated 07-06: −$310 IWM loss sat inside the −$527 tier-1 line, no halt). Bigger caps without looser rails just tighten the effective risk budget (VP §4c).
- **Sources:** WP §1, §11b, board #1/#15, diary 07-02/07-06/07-08; VP §4c; verified live 07-09 `risk_manager_startup` (7.5/11.25, no validation warnings).
- **Status:** active (5.0/7.5 superseded by the same ADR's 7.5/11.25 revision).

### ADR-003 — Vehicle policy: OI floor 100→50 + $2,000 caps now; per-symbol percentile OI next (2026-07-08) [ACCEPTANCE-TEST DECISION]
- **Phase 1 — APPLIED 2026-07-08 eve to the 5 live rows** (backup taken; LANE_CONFIG diff = exactly the cap raises): `min_open_interest` 100→**50**; premium caps → **$2,000 uniform**.
- **Why the floor dropped (operator):** on 2026-07-07 SMH live signaled short (08:59 + 09:03 CT) but **ALL 1,122 candidate contracts were rejected → 0 live trades** = selector starvation, invisible in P&L (looked like a "quiet day"). Breakdown: 561 wrong type (calls), 295 outside the 3–7 DTE window, then of **~266 in-window puts: 168 open_interest_below_min (largest bucket), 55 delta_below, 34 delta_above, 9 spread → 0 survived.** A live-entry filter was quietly costing live entries.
- **Honesty caveat (load-bearing):** the selector is a **waterfall** — each contract is bucketed into the FIRST filter it fails (OI→delta→spread), so OI-blocked contracts are never tested against delta/spread. The logs prove OI is the largest *counted* bucket, NOT that a lower floor would have produced a fill. Floor-50 is directional, settled by a live A/B, not quantified. AMD's own live fills sitting at OI 163/175 (vs 100 min) is the real corroborating evidence that these lanes trade thin near the floor.
- **Who pushed what:** **floor-50 = Claude's recommendation, operator accepted** (the decision data flagged AVGO-EB / AMD as the thin-near-floor lanes). **$2,000 uniform caps = operator pushed for bigger caps** (to enable multi-lot / the T2 runner); the **rail arithmetic constrained the number to $2,000** so a single worst-case stop-out (−$700) stays under the same-day 7.5% halt (−$768) — $2,500/$3,000 would have tripped the 7.5% *flatten* tier on ONE bad entry (VP §4b/§4c). The two decisions were made coherent on the same afternoon.
- **Validation:** floor-50 validated within **21 minutes** on 2026-07-09 — AVGO elastic-band converted its **first-ever trade** (8 signals / 0 trades lifetime under the old floor), 2-lot, can_ladder=1; selector empties 0 that day (vs 4 the prior). Then QQQ ran the first-ever live T2 runner at the new 10-lot size (+$1,064).
- **Phase 2 — PLANNED (operator's design, data accruing):** replace the global floor with **per-symbol LEARNED percentile OI** — liquidity is symbol-relative (OI=100 means nothing on SPY, everything on SMH). Judge each contract's OI as a *percentile within its own symbol's chain per DTE window*; learn "good OI" per symbol by correlating entry OI-percentile with realized fill slippage. **OI modulates entry AGGRESSIVENESS (thin OI → near-bid passive, no chase), not a hard gate.** Guard against top-30%-of-nothing with a low absolute sanity bound + spread co-gate. Training set = `option_chain_snapshots` captured at selection time (built + deployed 07-08 `d285ebb`; accruing since 07-09 — 6,020 rows by 10:00 07-09); set the percentile policy after 3–4 sessions of data.
- **Premium cap:** DEFERRED pending snapshot data — caps stay per-lane ($2,000 uniform for now); the multi-lot / cheaper-strike question folds into the same learning loop.
- **Sources:** WP §11, §11b, board #11, diary 07-07(eve)/07-08(close)/07-09; VP (whole doc, esp. §1b, §4b, §4c, §5); RFC §6 (this is the fresh-session acceptance question).
- **Status:** phase 1 active; phase 2 planned.

### ADR-004 — DTE-window widening REJECTED (2026-07-08 brainstorm)
- **Decision:** Do NOT widen the entry DTE window to reach 0–4 DTE on the single-name lanes.
- **Why (operator):** single names don't offer 0–4 DTE granularity — the tight short-DTE ladder only exists on SPY/IWM/QQQ-class ETFs (QQQ's live lane already runs a 0–3 window; AMD/SMH/NVDA run 3–7 or 7–14 because that's what their chains carry). SMH's 07-07 chain offered only 3/6/10/13/15 DTE. Widening single names would only pull in thinner, wider contracts — the OI/percentile approach (ADR-003) is the right lever, not the DTE window.
- **Sources:** WP §11 (SMH chain 3/6/10/13/15); VP §5 (QQQ 0–3 vs single-names 3–7/7–14). *Unverified:* no standalone "rejected" line in the docs — reconstructed from the brainstorm framing + the DTE-granularity evidence.
- **Status:** rejected.

### ADR-005 — Evidence gates ≠ safety gates: shadow may relax evidence, never safety (2026-07-02)
- **Decision:** Shadow (non-live) rows may **relax evidence-quality gates** (`mala_evidence_ready`, `activation_candidate`/M7 concordance, `option_trade_ready`), stamped visibly into `source.metadata.evidence_gates_relaxed`. They may **NEVER** relax safety/integrity gates: runtime capability, `bhiksha_ready`, explicit `m7_status=block`, `triage_verdict=KILL`, retired, symbol/strategy_key mismatch. **Promotion to live re-runs the full gate (fresh M7) or takes an explicit operator override.**
- **Why (operator/audit P5):** shadow lanes are the instrument that *gathers* activation evidence — blocking shadow on missing activation evidence is circular. But a KILL verdict (e.g. `option_not_tradeable`) is a *verdict*, not missing evidence — paper-trading an untradeable option teaches nothing. The relaxed stamp guarantees a weak shadow row is never promoted to live by accident. Surfaced per-lane in daily + weekly reports.
- **Sources:** L-sheet; WP board #17; CMP; `bhiksha/.../ops/daily_report.py`, `.../weekly_scorecard.py` (evidence_gates_relaxed).
- **Status:** active.

### ADR-006 — Money-path changes require multi-round ADVERSARIAL audit (2026-07-02, standing)
- **Decision:** Any change that can place, size, or suppress a real order goes through fresh adversarial audit rounds with repros — a green test suite is NOT proof of readiness. Canon (and the brain) admit a money-path lesson only carrying its verification artifacts.
- **Why (operator/precedent):** the 07-02 cycle shipped 3 money-path changes, each with a fully green suite; the audits still found **5 real reproducible live-money bugs**, each a different class. Pattern that works: (a) fresh adversarial agent with a "disprove readiness" hunt list; (b) **send the fix-delta back to the SAME auditor** to re-run its own repros — context makes round 2 sharp; (c) per-finding verdicts + pinned regression tests. The same-auditor re-run has caught the worst bug **4 consecutive times** — incl. bug #3, where Claude's own unit test had *encoded the false positive as expected* and would have silently amputated the T2 runner. Item #21 (07-09) is the live proof: 3 rounds, reconciliation-race blocker caught round 2, null-is-zero-fill caught round 3 (settled from 89/89 real broker payloads).
- **Sources:** L-money; WP board #20/#21, diary 07-08/07-09(eve); MEM/live-experiment-status-2026-07.md.
- **Status:** active (governs promotion of money-path lessons into brain canon — RFC §5.6.2).

### ADR-007 — Budget-before-entry, block-on-unknown (fail-safe FLIP) (2026-07-02, audit P2)
- **Decision:** Prefetch the cash budget at startup; if the day's budget is **unknown**, **block new entries** — but do NOT flatten on unknown.
- **Why (operator audit):** the prior posture let live entries through during the startup window before Rail A had a `cash_budget_day` (proven 07-03 08:31–08:37: SMH live entry allowed, Rail A inactive). Flipped the fail-safe: unknown budget = pause entries (safe), not close positions (over-reaction). Third consecutive clean startup by 07-09.
- **Sources:** WP board #14, diary 07-02/07-06/07-09.
- **Status:** active.

### ADR-008 — Schwab guard alerting: notify only on FAILED re-auth (2026-07-07)
- **Decision:** The Schwab token guard alerts the operator **only when re-authorization FAILS**; a successful proactive near-expiry renewal is **silent**.
- **Why (operator):** after root-causing the silent 7-day refresh-token lapse (guard only browser-renewed *after* expiry — browser path invoked 0× ever), the fix made near-expiry its own branch with proactive renewal while the token still works. Operator's refinement (e908920): a successful self-renewal is routine and shouldn't page him; only a real failure (which would block the 08:20 live-start) warrants a notification. Closes the silent-alert gap without adding noise.
- **Sources:** WP board #19, diary 07-07(00:15/00:45).
- **Status:** active.

### ADR-009 — The Sheet IS the operator arming surface: honor + surface gate keys, never strip (2026-07-02)
- **Decision:** `active_strategies` Sheet-set dispatch-gate keys (e.g. `profile_exit_drives_live`) are **honored and surfaced**, never deny-listed. Every gate key a row sets appears in `plan.summary["gate_override_key_warnings"]` in the compiled-plan audit trail.
- **Why (operator):** the Sheet already decides `mode=live` (real order submission) — strictly *more* power than any gate key. A hostile Sheet means you've already lost; the realistic actor writing those cells is the *operator arming a feature*. Stripping the keys as "hardening" would have silently disarmed his pre-staged live flip on the next 08:20 sync — security theater that fights legitimate intent. Correct posture: make arming visible, never block it.
- **Sources:** L-sheet; CMP:190-248,373-416 (`_detect_gate_override_keys`, `gate_override_key_warnings`).
- **Status:** active.

### ADR-010 — Operating cadence + deploy authority (2026-07-02, operator-agreed)
- **Decision:** ~2-week structure: **09:42 morning watch · 13:07 midday watch · 15:24 close readback + ONE build increment/day** from the status board. **Fridays = weekly synthesis instead of a build.** Full deploy authority is delegated but **gated on: green tests + passed audit (adversarial round if money-path) + an evening/session boundary** (one increment/day keeps the live experiment attributable).
- **Why (operator):** don't make Suman the scheduler/bottleneck, but keep every live change attributable to a single day and provable before it touches money. The deterministic safety layer (Telegram reports 3×/day, watchdog, rails) runs on oldmac independently of the session-scoped cadence crons (which expire ~07-13).
- **Sources:** WP "Operating cadence" / "What we are building", diary 07-02/07-09(midday/eve).
- **Status:** active (07-09 the operator directed a same-day whole-backlog fold-in via a supervisor lane — a deliberate one-off override of one-build/day, still fully audited).

### ADR-011 — Playbook-discovery evidence engine: reject counterfactuals + manual-bot trickle (2026-07-03)
- **Decision:** For proving the 4 exit playbooks, **reject** cross-profile counterfactuals and the manual-bot trickle as evidence engines. Direction = refine the 4 playbook *hypotheses*, then tag the operator's own timestamped fills → detectors that "fire where he fired" → option-path validation → shadow.
- **Why (operator):** counterfactuals are thesis-coupled — "we'd never act on them," so they can't be evidence. The live book is a TREND_CONTINUATION monoculture (17/19 lanes); the real path to the other 3 profiles is his own 5,760-round-trip corpus, not synthetic what-ifs.
- **Sources:** WP board #18, diary 07-03; docs/PLAYBOOK_DISCOVERY_PROGRAM.md; MEM/profile-coverage-gap.md, MEM/operator-trading-profile.md.
- **Status:** rejected (the counterfactual/trickle engines); the discovery program itself is active/spec'd.

### ADR-012 — Resident intelligence: where the brain lives and how it stays true (RFC §9a, 2026-07-09)
- **Decision (bundle, all operator-decided 07-09 evening):**
  1. **Home = mala_v2** at `docs/brain/`, git-versioned. Decisive rationale: **bhiksha's checkout IS the production runtime** (deploy-gated, audited money path) — knowledge churn must **never ride the money path**; the embryo, session entry points, and system-level scope already live in mala_v2. bhiksha/docs/lessons/ stays code-adjacent; the brain indexes it by reference.
  2. **Auto-commit + advisory curation.** "Auto commit is the right way, I do not want to become the bottleneck on a document written by an agent... the same idea holds that I can really help with curation." → steward auto-commits ALL brain layers; the Lathi-bus card becomes a periodic *advisory curation digest* that prunes/corrects canon after the fact, never blocks it.
  3. **Privacy: personal P&L / trading-DNA stays in memory_core** — verified 07-09 both mala_v2 and bhiksha are PUBLIC on GitHub. Standing rec: flip both private (they encode live edge); until then the boundary is enforced by content type and mala_v2 pushes stay paused/diary-free.
  4. **Phone digest via Pulsar** (companion-pack), through its governed path (mc namespace and/or a companion-pack lane consuming bus cards). **Jarvis is deprecated**; `jarvis-northstar` survives only as a legacy-named bus profile delivered by Beacon.
  5. **Naming `docs/brain/` accepted** (visible/operator-readable) over `.intelligence/`.
- **Why (operator):** the repo should become the memory any visiting agent instantly inhabits (beat amnesia), without the curation gate making Suman the bottleneck, and without knowledge ever touching the live order path.
- **Sources:** RFC §5.1, §9a, §9; MEM/live-experiment-status-2026-07.md, MEM/companion-stack-hermes-pulsar-beacon.md.
- **Status:** active (RFC cleared for phase 1 — the scaffold this file is part of).
