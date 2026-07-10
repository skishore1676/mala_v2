# BRAIN INDEX — the map (always loaded)

as_of: 2026-07-09 · maintainer: brain steward (nightly, dev-Mac launchd 21:45 — `scripts/brain/steward.py`) + supervisor sessions
Trust order: **runtime evidence > diary > brain summary.** A brain claim without a
citation is not admissible. If an `as_of` below looks stale, re-verify before relying.

## Facts that must never be re-derived

1. **Live money is downstream.** bhiksha runs real orders on oldmac; the runtime checkout
   is `/Users/sunny/Documents/bhiksha` (NOT ~/code) — its HEAD at the 08:20 CT live-start
   is what trades. Deploys: green tests + adversarial audit (money path) + session boundary.
2. **Exit-authority rule (non-overridable):** profile-armed ⇒ the profile ladder owns ALL
   profit-taking; no full resting target; the protective stop always rests at the broker.
   Enforced at `_profit_target_configured` in bhiksha's supervisor. (ADR-001)
3. **Money-path changes require multi-round adversarial audit** — a green suite is not
   proof; the same-auditor re-run round has caught the worst bug 4 consecutive times. (ADR-006)
4. **Risk rails 7.5% / 11.25%** (halt/flatten, env > Operator_Defaults_v1 sheet > default)
   — a catastrophe brake, NOT a per-trade stop; the 35% premium stop bounds each trade. (ADR-002)
5. **Vehicle policy phase 1 live since 07-08:** OI floor 50, $2,000 uniform caps on the 5
   live rows; phase 2 = per-symbol percentile OI from `option_chain_snapshots`, OI modulates
   entry aggressiveness rather than gating. (ADR-003 — the acceptance-test decision)
6. **The Google Sheet `active_strategies` tab is the operator's arming surface** — gate
   keys are honored + surfaced (`gate_override_key_warnings`), never stripped. (ADR-009)
7. **Shadow lanes may relax evidence gates, NEVER safety gates** (capability, bhiksha_ready,
   m7 block, triage KILL); promotion to live needs fresh M7 or operator override. (ADR-005)
8. **This repo is deliberately NOT pushed** (public remote; P&L + edge live here). Personal
   operator DNA lives in memory_core / the Claude memory dir, by reference only. (ADR-012)
9. **Reconciliation rewrites tracker positions from the broker portfolio every ~15s** —
   never treat `position.quantity` as an order's placed quantity; use the order payload's
   own `quantity`. And `filledQuantity: null` is Public's zero-fill idiom, not garbage
   (89/89 real payloads carry `quantity`). (board #21, 3-round audit)
10. **Companion stack:** Jarvis is deprecated — Pulsar (companion-pack) is the companion,
    Hermes the front door, Beacon the Telegram bot; `jarvis-northstar` is a legacy-named
    bus profile that still works.

## The brain (deep layer — read on demand)

| File | What it holds | as_of |
|---|---|---|
| `ARCHITECTURE.md` | 4-repo organism, compile pipeline, runtime loop, deploy topology | 2026-07-09 |
| `OPERATIONS.md` | Runbook: cadence, 7 launchd jobs, ssh/sqlite/bus idioms, deploy protocol | 2026-07-09 |
| `DECISIONS.md` | ADR ledger 000–012: every operator decision with its why | 2026-07-09 |
| `STATE.md` | What is true right now (experiment, deploys, config, queue, watch items) | 2026-07-09 |
| `candidates/` | Steward drafts awaiting curation — never loaded at bootstrap | — |

## Primary sources beyond the brain

- `docs/LIVE_LOOP_WORKPLAN.md` — THE canonical running doc: status board (items 1–25) +
  dated diary of the live experiment + cadence spec ("resume the trading cadence").
- `docs/lessons/` (here) and `bhiksha:docs/lessons/` — code-adjacent engineering lessons;
  read before touching the areas they cover.
- `docs/VEHICLE_POLICY_DECISION.md` — the OI/cap data pull behind ADR-003.
- `docs/PLAYBOOK_DISCOVERY_PROGRAM.md` — the 4-profile coverage research program (board #18).
- `docs/RESIDENT_INTELLIGENCE_RFC.md` — this brain's own design + operator decisions (§9a).
- `.supervisor-lane/STATE.md` — supervisor-lane control state + verdict logs (07-09 fold-in).
- `.super-goal/exits-live-scale-riskmgr/` — the completed goal packet that armed the exits.
- `~/.claude/projects/-Users-suman-code-mala-v2/memory/` — personal operator facts +
  session memory (Claude-only; system knowledge belongs HERE, not there).
- Runtime readback: `ssh oldmac`, sqlite `-cmd ".timeout 8000"` on-host / `immutable=1`
  for snapshots; artifacts under `~/Documents/bhiksha/artifacts/playbook/`.

## Standing posture

Read-and-recommend by default. Execution authority is granted per lane by the operator.
Before live-loop work: read `STATE.md`, then the diary tail, then verify anything
load-bearing against the runtime. When sources disagree, the runtime wins.
