# Live Loop Workplan — running follow-ons from the exits-live lane

> Working document. Status table first; detail sections below. Update the table as items move.
> Born 2026-07-02 (day 1 of the month-long live profile-exit test). Companion evidence:
> `.super-goal/exits-live-scale-riskmgr/STATE.md` (proof log), `bhiksha/docs/lessons/` (patterns).

## Status board

| # | Item | Status | Type | Notes |
|---|---|---|---|---|
| 1 | Risk-tier calibration | **DONE 2026-07-02** | config | Operator decided: halt ≈$500, flatten ≈$730 → set 5.0%/7.5% in oldmac .env (scales with account). Live at next 08:20 start. |
| 2 | Exit authority rule: profile-armed ⇒ no full resting target | **DEPLOYED** 2026-07-02 eve | bhiksha code | Non-overridable, in `_profit_target_configured`; stop placement proven untouched; `profit_target_suppressed` event per entry. Live at 2026-07-03 open. |
| 3 | Rail-A consult restructure (once/bar, not per symbol) | **DEPLOYED** 2026-07-02 eve (5,070→39 rows/day) | bhiksha code, small | 4,966 redundant evals/day (13× multiplier). §3 |
| 4 | Morning bias overlay (operator instinct #2) | TODO (next lane) | new feature | Pre-open brief → bounded plan pruning. Hooks exist (BiasSelection, suppression). |
| 5 | Weekly profile-vs-legacy comparison | **NOT BUILT** | analytics + Lathi bus | The month-test verdict mechanism. Data accruing since 2026-07-02; build analytics + weekly job + bus publish. §5 |
| 6 | Session reports → Obsidian approve/archive surface | PARTIAL | lathi-bus wiring | Telegram via lathi-bus WIRED (jarvis-northstar). Obsidian/approve→archive NOT wired. Control-tower: bhiksha-side contract done (`bhiksha/docs/lathi_control_tower_bhiksha_jobs.md`); tower-side consumption to verify. |
| 7 | Revive daily shadow-EV report | TODO | job re-home | Old OpenClaw cron died in migration (May). Re-home per app-owns-jobs structure; publish via bus. |
| 8 | Capability: market-impulse descendant variants + compression family | TODO (bigger) | bhiksha capability | Unblocks 9 catalog rows (mi-desc ×5, compression/vpoc ×4). |
| 9 | Serial bar-fetch stall under provider slowness | TODO | bhiksha perf | One 241s heartbeat spike 2026-07-02 14:13 CT; no exposure (protection is broker-side). Fix = concurrent per-symbol fetch. |
| 10 | Label profile-dispatched exits distinctly (`exit_mode`) | TODO, small | bhiksha reporting | Today they show `exit_mode=strategy`; month readback needs clean attribution. |
| 11 | `entry_selector_empty` on AVGO (×4) / AMZN (×1) | TODO, small | vehicle filters | New shadow lanes can't find contracts passing DTE/delta/OI filters. |
| 12 | Operator's risk-manager audit | RESOLVED 2026-07-02 | — | Delivered in-conversation; verdict accepted; its 5 priorities are items 13–17 below. |
| 13 | launchd_status stranded-return bug (audit P1) | **FIXED** 2026-07-02 | bhiksha bug | `_runtime_status` parsing was dead code after `_bhiksha_python`'s return → Control Tower lied by omission. Fixed + 2 regression tests; **DEPLOYED + verified live** (runtime non-null on oldmac). |
| 14 | Budget before entry / block on unknown (audit P2) | **DEPLOYED** 2026-07-02 eve — production proof at 2026-07-03 08:20 startup | bhiksha code | 08:31–08:37 window: Rail A inactive (no cash_budget_day), SMH live entry allowed. Fix = startup budget prefetch + `risk_rail_a_budget_unavailable` entry block when unknown (no flatten on unknown). Flips the earlier fail-safe per operator audit. |
| 15 | Risk knobs → Operator_Defaults_v1 + report readback (audit P3) | TODO | bhiksha + Sheet | Env works today (tiers set 5.0/7.5 = −$479/−$718 on audit's $9,579.74 budget) but knobs belong on the operator surface; resolved values + validation warnings into the session report. SettingsSource hook already exists. |
| 16 | Rail A.5: mark-to-market open-book drawdown (audit P4) | TODO | bhiksha code | Rail A is realized-only; native stops protect per-trade meanwhile. v1 = open-book drawdown WARNING event from position marks; halt/flatten escalation a later decision. |
| 17 | Relaxed-evidence labels in reports (audit P5) | TODO (analytics lane) | reporting | Keep shadow relaxation (strategically right per audit); surface `evidence_gates_relaxed` per lane in session/weekly reports so a weak shadow row is never promoted by accident. |

## §1 Risk-tier calibration (the 2% / 3% question)

Current: usable budget ≈ $9,700 → tier-1 halt-new-entries at realized day P&L ≤ **−$194**,
tier-2 flatten-book at ≤ **−$291** (defaults 2% / 3%).

Per-trade risk actually configured on the live lanes: premium caps $900–$2,000 with a 35% premium
disaster stop → a single routine stop-out loses **$300–$700** (today's NVDA position: $1,944 premium
→ 35% stop = −$680). So one normal loss blows through both tiers and flattens + halts the day.
June had 6 of 11 traded days beyond −$194.

Three coherent postures (pick one):
- **A. Raise tiers to fit sizing** — e.g. halt 6% (−$582 ≈ one stop-out), flatten 10% (−$970 ≈ two).
  Keeps current position sizes; rails act on genuinely abnormal days.
- **B. Shrink sizing to fit tiers** — premium caps ≤ ~$500 so a 35% stop ≈ −$175; 2%/3% then means
  "halt after ~1 loss, flatten after ~2". Slower experiment, tighter guardrails.
- **C. Keep as-is deliberately** — a "one-loss-and-done" daily stop. Legitimate but probably not
  intended given the caps you set.
Knobs (env, validated at startup): `BHIKSHA_RISK_MAX_DAILY_DRAWDOWN_PCT`,
`BHIKSHA_RISK_FLATTEN_DAILY_DRAWDOWN_PCT`. Recommendation: **A** (halt 6 / flatten 10) for the
experiment month — matches your many-small-losses style; revisit with real dispersion data.

## §2 Exit authority rule (DECIDED 2026-07-02)

**Operator rule: when a deployment's profile exit is armed live, the profile owns ALL
profit-taking — no full-size resting target, no virtual-target machinery, and no config override.**
The protective stop always stays resting at the broker (crash cost = a missed partial, never an
unprotected position). Rationale: the resting +35% full target filled before the ladder could bank
its 60% partial — flip day 1 saw NVDA/AMD exit 100% at 1R with the T2 runner structurally
impossible. Lowering T1 would NOT have fixed this (the full target still kills the runner at +35%).
Enforced in code at `_profit_target_configured` (single choke point for both arming sites);
documented in `bhiksha/README.md`. Shadow-only lanes keep their target machinery (the profile
cannot dispatch there).

## §3 Rail-A consult restructure

`book_actions()` runs once per symbol-bar (13 symbols × 389 minutes = 4,966 evals on 2026-07-02).
Rail A is book-level — once per bar-minute suffices (13× reduction), and the `ok` decision events
can be sampled (e.g. 1/10) while keeping every non-ok event. Entry-consult events (8 today) stay 1:1.

## §5 Weekly profile-vs-legacy comparison (not built)

Data already accruing: every position tick records the profile decision (`profile_exit_shadow`
events incl. gate_inputs) + actual fills in `trade_sessions`. Build: script that reconstructs, per
closed live trade, "what the profile did (or would do)" vs "what a legacy stop/target/EOD would
have produced" → weekly md/json report → lathi-bus publish (Telegram summary + Obsidian for
approve/archive). Owner job: bhiksha launchd (app owns its jobs), weekly Fri post-close.

## Operating cadence (2026-07-02 → ~2 weeks, operator-agreed)

Trading days (CT): **09:42** morning watch (quiet unless notable) · **13:07** midday watch (quiet
unless notable) · **15:24** close readback to operator + ONE build increment from the status board.
**Every wake-up appends a dated entry to the Diary section below** (the close entry is the full one;
watches add a line or two even when all-quiet); the close job commits this doc in mala_v2
(worker + tests + review + adversarial round if money-path; deploy evenings only, one increment/day
so the live experiment stays attributable). **Fridays**: weekly synthesis instead of a build.

Build queue order: #10 exit-attribution labeling + #17 relaxed-evidence report labels → #15 knobs →
Operator_Defaults_v1 + report readback → #5+#6 weekly profile-vs-legacy analytics + Lathi bus
(Telegram + Obsidian approve→archive) → #16 Rail A.5 MTM warning → week 2: #4 morning bias overlay
lane, with #11 selector tuning / #7 shadow-EV revival / #9 bar-fetch concurrency as fillers; #8
capability work last.

NOTE: the watch/build cron jobs live in the Claude session on the dev Mac (session-scoped — they do
not survive a Claude restart and expire after 7 days). If they're gone, tell Claude "resume the
trading cadence" — this section is the spec to recreate them. The deterministic safety layer
(session reports to Telegram 3×/day, watchdog, rails) runs on oldmac independently of all of this.

## What we are building (plain terms)

Five tracks, in priority order:
1. **Trust the machine's judgment** — the risk manager (rails live), quiet enough to read, knobs on
   the operator sheet, mark-to-market awareness next. Done when: Suman changes a risk dial in the
   Sheet and sees it in the morning report without touching code.
2. **Prove the exit DNA** — the month-long live profile-exit experiment (running since 2026-07-02)
   plus the analytics that judge it: exit attribution, weekly profile-vs-legacy comparison,
   published to Telegram + Obsidian approve flow. Done when: a Friday report says, with real fills,
   whether the profiles beat legacy — per lane, per rule.
3. **Widen the evidence funnel** — 19 lanes running (was 6), selector tuning for lanes that can't
   find contracts, capability work to unblock the 9 excluded rows, shadow-EV daily readout revived.
   Done when: every catalog row is either trading (live/shadow) or has a named blocker.
4. **Put the operator's eye back in the loop** — the morning bias overlay: a pre-open market brief
   that prunes/scales the day's plan (never adds trades). Done when: the morning report shows "today
   we stood down X because of Y" and the month's data shows whether that helped.
5. **Runtime robustness** — bar-fetch concurrency (kill the stall mode), Control Tower truthfulness
   (fixed), deploy hygiene. Done when: a slow provider or a sleepy laptop can't blind or stall the loop.

## Diary

> One dated entry per wake-up/observation/build. Newest at the bottom. Terse but complete —
> this is the record we'll use in two weeks to judge the journey and reprioritize.

### 2026-07-01 (Tue) — the diagnosis
Live readback: June = ≈ −$1,000 over ~20 live trades on a $10.2k account; verdict = sample
starvation, not proven no-edge. Discovered the profile exits were ARMED in the Sheet since ≤Jun 23
but never dispatched once (deploy gap until Jun 30, then a reconciliation bug silently closing the
dispatch gate). Operator decided: flip to profile exits for a month, expand everything to shadow,
build risk rails ("must be mechanical, not vigilance-based").

### 2026-07-02 (Wed) — flip day
Overnight: shadow book 6→19 lanes (evidence-gates partition in the compiler), risk rails built +
3-round adversarial audit (5 real bugs found incl. one my own test had blessed), reconciliation
source fix (root cause of the dead gate), all deployed pre-open. **09:22 CT: first-ever live
profile dispatch** (SMH no_progress scratch −$8). Day: live **+$1,001** (NVDA +$684, AMD +$325 —
both full exits at the +35% resting target). GREEN report, zero warnings. Operator audit arrived:
P1 Control Tower blind (fixed same evening + regression tests), P2 budget-unavailable entry window
(fixed same evening: startup prefetch + block-on-unknown). Operator decisions: tiers $500/$730
(set as 5.0%/7.5%); **exit-authority rule** — profile-armed lanes never rest a full profit target
(the resting +35% target had amputated the T2 runner on both winners) — implemented, non-overridable,
deployed. Evening sprint (in flight): exit attribution + relaxed-evidence labels; risk knobs →
Operator_Defaults_v1 with report readback. Cadence agreed: 09:42/13:07 watches + 15:24 close
readback + one build/day for ~2 weeks.
