# Live Loop Workplan — running follow-ons from the exits-live lane

> Working document. Status table first; detail sections below. Update the table as items move.
> Born 2026-07-02 (day 1 of the month-long live profile-exit test). Companion evidence:
> `.super-goal/exits-live-scale-riskmgr/STATE.md` (proof log), `bhiksha/docs/lessons/` (patterns).

## Status board

| # | Item | Status | Type | Notes |
|---|---|---|---|---|
| 1 | Risk-tier calibration | **DONE 2026-07-02, revised 2026-07-08** | config | Operator decided: halt ≈$500, flatten ≈$730 → 5.0%/7.5%; revised 07-08 to **7.5% / 11.25%** "till the account grows" (headroom for multi-lot book). Verified live 07-09 via risk_manager_startup (no validation warnings). |
| 2 | Exit authority rule: profile-armed ⇒ no full resting target | **DEPLOYED** 2026-07-02 eve | bhiksha code | Non-overridable, in `_profit_target_configured`; stop placement proven untouched; `profit_target_suppressed` event per entry. Live at 2026-07-03 open. |
| 3 | Rail-A consult restructure (once/bar, not per symbol) | **DEPLOYED** 2026-07-02 eve (5,070→39 rows/day) | bhiksha code, small | 4,966 redundant evals/day (13× multiplier). §3 |
| 4 | Morning bias overlay (operator instinct #2) | TODO (next lane) | new feature | Pre-open brief → bounded plan pruning. Hooks exist (BiasSelection, suppression). |
| 5 | Weekly profile-vs-legacy comparison | **DEPLOYED** 2026-07-09 eve | analytics + Lathi bus | `weekly_scorecard.py` + Friday 15:20 CT job + bus publish; verified on oldmac real data (Relaxed column populates from runtime deployments). First verdict (wk 07-06→09): profile exits +$1,429 (8t,5W) vs legacy −$1,629 (15t,3W). Promotion candidates: 0 (META near-miss). Friday run = its live debut. |
| 6 | Session reports → Obsidian approve/archive surface | **DEPLOYED** 2026-07-09 eve | lathi-bus wiring | Session reports now ALSO project to `07 Agents/Coding/Inbox` (profile coding-agent-northstar) with approve/revise/park decision block; transport-graceful, never fails the report job; env-gated (BHIKSHA_SESSION_REPORT_OBSIDIAN_MODE, default on). Verified end-to-end from oldmac. Integration fix: source path absolutized (bus cwd-switches). |
| 7 | Revive daily shadow-EV report | **DEPLOYED** 2026-07-09 eve | job re-home | Lineage root-caused (openclaw plist .disabled + orphaned mala-shadow-daily.sh). Rebuilt v2 phone-first in bhiksha: per-lane EV incl. partial legs, rolling-10, trend flags; daily 15:30 CT job; first live send delivered (shadow book since flip: 24t, EV −$36/t, only PLTR lane positive). |
| 8 | Capability: market-impulse descendant variants + compression family | **DEPLOYED** 2026-07-09 eve | bhiksha capability | 4 new capability strings, manifest v1→v2; 9 rows pass capability gating (differential parity vs mala research: 342 signals, 0 mismatch; live-lane parity adversarially proven: 0 drift over real live params). Only 2 rows become shadow lanes after mala steward republishes bhiksha_runtime_supported (watch_only ×4, TSLA KILL ×2 stay held). |
| 9 | Serial bar-fetch stall under provider slowness | **DEPLOYED** 2026-07-07 eve (ec12a11) | bhiksha perf | Per-symbol fetch now concurrent (asyncio.gather + Semaphore(8)); sweep wall-clock ~max not sum; dispatch order + error isolation preserved; 5 new tests. Watch heartbeat_lag_ms drops next session. |
| 10 | Exit attribution (`exit_rule` column + Exit column in reports) | **DEPLOYED** 2026-07-02 eve | bhiksha reporting | Additive trade_sessions.exit_rule (profile:<rule> vs stop/target/strategy/hard_flat); exit_mode untouched (reprice branches key off it). |
| 11 | Vehicle policy: OI floor + caps + percentile learning | **PHASE 1 APPLIED** 2026-07-08 eve | vehicle filters + operator judgment | NOT cosmetic: on 07-07 SMH LIVE signaled short but ALL 1122 contracts were rejected → 0 live trades. Of ~266 in-DTE-window puts: 168 open-interest-below-min, 89 delta-out-of-band, 9 spread. OPEN QUESTION for operator: is the OI floor correctly protecting against illiquid SMH short-DTE puts, or too strict and costing live entries? Tuning a live-entry filter needs operator sign-off on the liquidity/fill trade-off. See §11. |
| 12 | Operator's risk-manager audit | RESOLVED 2026-07-02 | — | Delivered in-conversation; verdict accepted; its 5 priorities are items 13–17 below. |
| 13 | launchd_status stranded-return bug (audit P1) | **FIXED** 2026-07-02 | bhiksha bug | `_runtime_status` parsing was dead code after `_bhiksha_python`'s return → Control Tower lied by omission. Fixed + 2 regression tests; **DEPLOYED + verified live** (runtime non-null on oldmac). |
| 14 | Budget before entry / block on unknown (audit P2) | **DEPLOYED** 2026-07-02 eve — production proof at 2026-07-03 08:20 startup | bhiksha code | 08:31–08:37 window: Rail A inactive (no cash_budget_day), SMH live entry allowed. Fix = startup budget prefetch + `risk_rail_a_budget_unavailable` entry block when unknown (no flatten on unknown). Flips the earlier fail-safe per operator audit. |
| 15 | Risk knobs → Operator_Defaults_v1 + report readback (audit P3) | **DEPLOYED** 2026-07-02 eve | bhiksha + Sheet | Env works today (tiers set 5.0/7.5 = −$479/−$718 on audit's $9,579.74 budget) but knobs belong on the operator surface; resolved values + validation warnings into the session report. SettingsSource hook already exists. |
| 16 | Rail A.5: mark-to-market open-book drawdown (audit P4) | **DEPLOYED** 2026-07-07 (d0331bd) | bhiksha code | Warning-only MTM check from position marks (bounded mark_price_provider, once/min via book_actions cache); knob open_drawdown_warn_pct (default=tier-1); renders in report Risk Rails. Escalation to halt/flatten = later operator decision. |
| 17 | Relaxed-evidence labels in reports (audit P5) | **DEPLOYED** 2026-07-02 eve | reporting | Keep shadow relaxation (strategically right per audit); surface `evidence_gates_relaxed` per lane in session/weekly reports so a weak shadow row is never promoted by accident. |
| 18 | Playbook discovery program (all-4-profile coverage) | SPEC'D 2026-07-03 | research lane | Live book is a TREND_CONTINUATION monoculture (17/19 lanes). Program: tag operator's timestamped fills → detectors that "fire where he fired" → option-path validation → shadow. Gates + success criteria: `docs/PLAYBOOK_DISCOVERY_PROGRAM.md`. First gate = P0 spec-lock (operator sitting). |
| 20 | Exit cancel-race + partial-leg accounting (hygiene batch) | **DEPLOYED** 2026-07-08 eve | bhiksha order-path + persistence | 3-round audited (SAFE-WITH-FIXES → fixes → SAFE-TO-DEPLOY, repros independently re-verified). Fixes live-observed AMD fill-orphan race; partial legs durably recorded (trade_partial_fills); selector-empty per lane + can_ladder tag in reports. |
| 21 | Residual: dead-status resubmit filledQuantity guard | **DEPLOYED** 2026-07-09 eve | bhiksha order-path | 3-round audited (UNSAFE→fixes→UNSAFE→one-line fix→clean). Ladder derives from the ORDER's own quantity (reconciliation rewrites position qty — round-2 catch); null filledQuantity = Public's zero-fill idiom, NOT unparseable (round-3 catch, empirical: 89/89 real payloads). Reprice ladder got the same fix. |
| 22 | Residual: sweep abandonment escalation | **DEPLOYED** 2026-07-09 eve | bhiksha reporting | Abandonment now emits runtime_issue (category partial_fill_abandoned) → daily report + Telegram; escalation ordered before the durable mark so a crash can't lose it. |
| 23 | Fail-closed live evidence gates (audit latent, MEDIUM) | TODO — needs verify vs real sheet | bhiksha compile | Blank `activation_candidate` cell + operator row flipped to authorization_mode=live compiles a row LIVE-ARMED (gates trip only on explicit FALSE). Fix = `is not True` for live rows — but FIRST verify current live rows' cells are populated TRUE or the fix suppresses the live book. Repro exists (capability-family worktree tests). |
| 24 | m7 gate literal-match gap (audit latent, LOW) | TODO (small) | bhiksha compile | Gate matches literal "block"; TSLA rows' real value `provider_blocked` passes it — one KILL-cell edit arms a provider-blocked row as shadow. Fix: match block-prefixed statuses. |
| 25 | Per-deployment exception isolation in bar loop (audit latent, MEDIUM) | TODO | bhiksha runtime | Poison params pass compile then raise on every bar; `_handle_bar_event` has no per-deployment try/except — one bad lane aborts the rest of that symbol-bar (live lanes currently protected only by row order). Fix: isolate per deployment + emit runtime_issue; add compile-time required_features probe. |
| 19 | Schwab guard: proactive near-expiry browser re-auth | **FIXED + DEPLOYED** 2026-07-07 | bhiksha bug | Guard only browser-renewed AFTER expiry (browser invoked 0× ever) → refresh token silently lapsed 07-07 04:57 UTC. Fix: near-expiry gets own branch, proactive browser renewal while token still works + closes silent-alert gap. Root-caused, re-authed live (new token exp 07-14), fix deployed 6c4136c. Alerting refined per operator (e908920): notify ONLY on failed re-auth, silent on successful near-expiry renewal. |

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

### 2026-07-03 (Thu, overnight) — the monoculture finding + playbook discovery program
Live-surface audit (oldmac active_plan): **17/19 lanes are TREND_CONTINUATION, 2 profile-less** —
the month test covers 1 of the operator's 4 profiles. Blockers per profile: FLASH has no carrier
(M1 mis-kill), EXHAUSTION rows triage-killed on tradability (bhiksha supports the family), RANGE
blocked on capability (#8). Operator rejected cross-profile counterfactuals (thesis-coupled — we'd
never act on them) and the manual-bot trickle as evidence engines; direction = refine the 4 playbook
hypotheses themselves. Data correction while grounding it: the personal corpus is **5,760 unique
round-trips, not 17,002** (three overlapping export batches; superset = `20260429_232737`);
1,465 timestamped (89% IWM/SPY), bar cache to 2021-05 → supervised "distill the operator" is
feasible today on IWM/SPY. Program doc with per-gate success criteria: **item #18,
`docs/PLAYBOOK_DISCOVERY_PROGRAM.md`** (P0 spec-lock → P1 tagging → P2 fires-where-he-fired →
P3 option-path validation → P4 shadow; fast-follows F1 IWM elastic-band unblock, F2 per-profile
weekly columns). Next action gated on operator: P0 sitting (~1 hr).

### 2026-07-04 (Fri) — P0 spec-lock CLOSED
Operator returned the questionnaire via Lathi bus (decision: revise, 5 comments) + 7 pages of
handwritten trading rules (preserved local-only at
`data/personal_imports/operator_notes/Rules_2026-07-04.pdf`; context-not-spec per operator).
Material corrections folded into `EXIT_PROFILE_PLAYBOOKS.md` § P0 Spec-Lock: **EXHAUSTION
reframed** (85–90th percentile stretch, symbol-AGNOSTIC, any timeframe; entry = inability to cross
the prior extreme) — supersedes index-first; **TREND anchor is the 10 VMA**, not VWAP;
**RANGE includes earnings-gap continuation**; **FLASH is counter-trend by definition** + doubling
down is part of the play (forces episode-level tagging). Handwritten notes yield P2 feature
candidates (regime-conditioned flash direction, high-IV squeeze gate, pattern-failure trigger) and
the draft spec for item #4 morning bias ("Morning Chores" page). Two open confirmations
(non-blocking): R4 overnight intent, X3 unclassified-share guess. **Gate P1 (tagging) is next** —
corpus prep, entry-context features, rule tagger citing the spec, first adjudication packet.

### 2026-07-04 (Fri, evening) — P1 round 1 built and out
Tagger shipped (`src/research/playbook_tagging.py` + `scripts/tag_personal_trades.py`, 7 tests
green): 1,443 long timestamped fills → **390 episodes** (~3.7 fills/episode — the doubling-down is
real). Machine round-1 tags: FLASH 104 (open-hour dominated, ~breakeven $), TREND 84 (61% win,
+$17.7k — the moneymaker), EXHAUSTION 21 (mid-day only, none at the open), RANGE 18, UNCLASSIFIED
31% (target ≤25%) — and the UNCLASSIFIED bucket holds **−$65k**, i.e. the machine can't map the
worst trades to any playbook (consistent with the operator's own "rule-breaking gambles" pages).
Caught+fixed a silent-corruption bug before publishing: polars `dt.hour()` is Int8 → minute axis
wrapped mod 256 → every feature computed at the wrong bar (first fingerprint was invalid;
regression test added). Round-1 adjudication packet (43 charted episodes, inline SVG) published to
the bus + Telegram nudge. Known iteration items for round 2: 55 early-open entries (09:30–09:35)
lack lookback context (FLASH-at-open blind spot); EXHAUSTION possibly under-counted (FLASH
precedence). Awaiting operator adjudication → agreement metric → rule iteration.

### 2026-07-06 (Mon, morning) — sprint machinery confirmed live; routine morning
First trading day after the evening-sprint deploy chain (oldmac @ 0763258). Session started clean:
budget prefetched (1 row, **zero** no_cash_budget_day / budget_unavailable events — P2 fix holding),
rails quiet (14 risk_manager_decision by 10:27 vs 5,066 on flip day — noise fix holding). By 10:27:
1 live + 4 shadow. **IWM live** (row_3, long 0.77→0.5118 ×12 = **−$310**) exited via
`exit_rule=disaster_stop`, profile-route dispatched (dry_run=0) — and the entry's full target was
correctly suppressed (`profit_target_suppressed: profile_owns_profit_taking`), so the exit-authority
rule is confirmed live: the stop is the only resting order, the profile owns the rest. Shadow:
META ×2 (−$20, −$19), AAPL (+$70), SPY still target_active. Health clean: identity_mismatch=0,
broker_recovered=0, 5 protective stops submitted. Heartbeat avg 7.3s (max 39s — one slow sweep,
under the 60s cadence, no exposure; #9 bar-fetch item still stands). No T2 runner yet (IWM went
down, not up). Nothing anomalous — operator not paged.

### 2026-07-06 (Mon, late morning) — FIRST live sighting of the T2-runner mechanic
Between the morning watch and midday, **SPY shadow** executed the full ladder for the first time in
production: entered 1.12, **banked the T1 partial** (4→2 contracts, `target_1_banked=true`,
`exit_rule=target_1_partial`), stop to breakeven, and the runner is riding — profile holding at
**+0.89R**, status `target_active`. This is the exact partial-and-ride behavior the flip-day
full-size-target amputation prevented and that the exit-authority rule (#2) restored; it just took
until a shadow lane actually trended for the mechanic to show. Paper (shadow), but real market data
and real profile FSM. No live lane trended today (IWM stopped out), so this is the first end-to-end
proof the ladder does what it's armed to do.

### 2026-07-06 (Mon, midday) — SPY runner completed the full ladder
The SPY shadow runner flagged late-morning closed at **1.35 via `max_hold`** (2-contract runner after
the T1 partial): entered 1.12 → partial banked at ~1R → runner rode → max_hold exit at +0.23
premium on the runner. First **complete** end-to-end ladder cycle in production (partial + runner,
both green) — the exit DNA working exactly as armed, on real market data. Rest of book unchanged:
IWM live −$310 (disaster_stop, target correctly suppressed) is the only live trade; shadows META ×2
(−), AAPL (+). Health all clean (rails 24 decisions, budget/mismatch/recovered all 0). Operator not
re-paged — this closes the loop on the late-morning runner heads-up.

### 2026-07-06 (Mon, close) — day −$293; rails calibration validated; runner mechanic proven; A.5 building
**Day tally:** live 1 trade **−$309.84** (IWM disaster_stop, target correctly suppressed), shadow 4
trades **+$17** net (SPY +46 runner, AAPL +70, META −80/−19). Total **−$292.84**, status GREEN.
**Rails calibration validated live:** the −$310 loss sat INSIDE the −$526.55 tier-1 halt line
(usable budget $10,531 × 5%) → no halt, exactly as intended; the old 2%/−$194 would have wrongly
ended the day. Report Risk Rails section renders resolved tiers in pct+$ (item 15 confirmed live).
**Milestone:** first COMPLETE T2-runner ladder cycle in production (SPY shadow: partial banked →
runner rode → max_hold exit, both legs green) — the exit DNA doing exactly what the exit-authority
rule (#2) armed it to do. Health all clean (identity_mismatch 0, broker_recovered 0, budget
prefetched, 0 budget-unavailable, exit attribution populated: disaster_stop + max_hold).
**Build increment tonight (market closed):** item #16 Rail A.5 mark-to-market open-book drawdown
WARNING (v1 warning-only, no halt/flatten, reuses existing per-position quote seam) — worker
building; review + deploy this session. Open question for the week: no live lane has trended yet
(2 live trades total since flip both stopped/scratched); the runner mechanic is proven on shadow but
still awaits a live winner. **Experiment tally so far: 07-02 +$1,001, 07-06 −$293 live.**

### 2026-07-07 (Tue, ~00:15) — Rail A.5 deployed; SCHWAB TOKEN BLOCKER for the AM session
Build increment #16 shipped: **Rail A.5 mark-to-market open-book drawdown WARNING** (audit P4),
merged `d0331bd`, 622 tests green, deployed oldmac. Warning-only (no halt/flatten, no order path),
reuses OrderManager.get_option_quote via a bounded once/min/position mark_price_provider; new knob
`open_drawdown_warn_pct` (env>sheet>default, default=tier-1 pct); renders in report Risk Rails
("open-book MTM warn (awareness only): 5.00% ($526.55)"). Reviewed the mark seam + sign math +
fail-safes before deploy; warning-only so no adversarial round. Status board #16 → DEPLOYED.
**BLOCKER discovered during boot check (NOT caused by this deploy):** Schwab refresh token EXPIRED
04:57 UTC 07-07 (7-day life, issued 06-30). The startup health check hard-gates on `schwab_token`
(`runtime.py:161`), so tomorrow's 08:20 CT live-start will FAIL unless re-authorized. Auto-guard
renews the access token but CANNOT renew an expired refresh token → needs operator browser re-auth
(`schwab_auth url` → authorize → `schwab_auth exchange <callback>`). **Operator alerted.** Public
(live broker) health is fine (LEVEL_3, $10,775 BP). Open item: consider making schwab_token
non-fatal for live-start since Schwab is the REPLAY/enrichment provider, not the execution broker
(Public) — a token expiry on a non-execution provider arguably shouldn't block trading. Flag for a
build increment.

### 2026-07-07 (Tue, ~00:45) — Schwab token: root-caused, re-authed live, fixed, deployed
Operator asked why the "browser auto-reauth" let the token expire. **Root cause:** the guard's
`refresh_token_near_expiry` state was grouped with `access_token_stale` in one branch that only did
`direct_refresh` (mints a new ACCESS token, never a new REFRESH token). The browser-agent renewal
only fired on ALREADY-expired / refresh-failure — so across all 4 guard receipts the browser agent
was invoked **0 times** while the 7-day refresh token silently counted down and lapsed at
04:57 UTC. The browser path was fully wired (`/Users/sunny/code/browser-agent/scripts/schwab-auto-refresh.sh`,
mode=auto) but mis-triggered — set to act after the barn door was open.
**Recovered live:** ran the guard now → browser agent invoked (return_code 0, its FIRST-EVER
successful run) → new refresh token issued, **expires 2026-07-14**, state healthy. Boot check now
exit 0. Tomorrow's 08:20 session unblocked.
**Fixed (item #19, deployed 6c4136c):** near-expiry now its own branch — direct_refresh first
(keeps access fresh), THEN proactive browser renewal to reset the 7-day clock while the token still
works; near-expiry + any browser-renewal failure now always alert (closes the silent-alert gap that
hid this for a week). 626 tests. So the guard will now self-renew at the 2-day mark (~07-12) instead
of lapsing. Also confirmed: the earlier Rail A.5 boot "failure" was purely this token gate — with the
token healthy, Rail A.5 (d0331bd) boots clean.

### 2026-07-07 (Tue, morning) — Schwab token fix HELD at first real startup; quiet AM
First live-start after last night's token re-auth + guard fix: **live-start rc=0, schwab_token
True/token_valid** — the fix held, session started clean (the whole point of last night confirmed).
Budget prefetched (1 row, 0 budget-unavailable), rails quiet (12 decisions), Rail A.5 live (0 MTM
warnings, expected — no open live positions), identity_mismatch 0, broker_recovered 0. By 10:12:
**no live trades yet**; 2 shadow — IWM banked another `target_1_partial` (+$33, ladder mechanic
firing again on shadow), AAPL −$165. Heartbeat avg 7.9s but **max 54s** — creeping toward the 60s
bar cadence (was 39s on 07-02, 241s spike 07-06); the #9 bar-fetch slow-sweep signature, no exposure
(no live positions, protection broker-side) but worth watching — if it crosses 60s a bar could be
missed. Still awaiting the first LIVE winner to trend.

### 2026-07-07 (Tue, midday) — flat/quiet, all clean
13:37 CT: no change since morning — same 2 shadow trades (IWM +$33, AAPL −$165 = shadow −$132),
**0 live trades / 0 open positions** all day; live lanes simply haven't caught a qualifying signal.
Trading loop active (5,830 signal evals). Health clean: dispatches 0, suppressed_targets 0 (no live
armed entries fired), broker_recovered 0, identity_mismatch 0, MTM warnings 0. Heartbeat max held
flat at 54s (same as AM — stable, not climbing; #9 still just a watch item). Operator not paged.

### 2026-07-07 (Tue, close) — flat day (live $0); token fix held; building #9 bar-fetch concurrency
**Day tally:** live **0 trades / $0**, shadow 2 trades **−$132** (IWM +$33 target_1_partial, AAPL
−$165). Status GREEN, no rails fired (0 halts, 0 demotes), no live entries triggered (live lanes
caught no qualifying signal; trading loop healthy at 5,830+ evals). **Schwab token fix confirmed
held** at first real startup (token_valid, rc=0) — last night's recovery + guard fix validated.
Health clean all day (dispatches 0, suppressed_targets 0, broker_recovered 0, identity_mismatch 0,
MTM warnings 0). Heartbeat max flat at 54s (stable, not climbing).
**Build increment tonight:** pulling **#9 bar-fetch concurrency** ahead of the #4 morning-bias lane —
rationale: #9 is the item with real operational signal (heartbeat lag edging toward the 60s bar
cadence, 54s×2 days + a 241s spike 07-06 = risk of a missed bar), and it protects the live loop;
the morning-bias overlay (#4) is a multi-day lane deserving operator involvement, not an autonomous
evening increment. Worker parallelizing the serial per-symbol fetch sweep (asyncio.gather, error
isolation preserved, no order-path change); review + deploy this session (market closed).
**Experiment tally: 07-02 +$1,001 · 07-06 −$293 · 07-07 $0 live.** Still awaiting first live winner
to trend (the runner mechanic proven on shadow 07-06, not yet in live dollars).

**Update (~16:20):** #9 bar-fetch concurrency reviewed + DEPLOYED (ec12a11, boot green). Per-symbol fetches now fire concurrently (gather + Semaphore(8)); dispatch order and error isolation byte-identical to serial; 632 tests. Expect heartbeat_lag_ms max to drop below the ~54s plateau next session — will confirm at the 07-08 morning watch.

## §11 entry_selector_empty — the live-entry blocker (2026-07-07)

Corrects the day's earlier "no live signal" read. SMH live (row_6) DID signal short at 08:59 CT
(and 09:03); no trade resulted because the option-contract selector rejected all 1122 SMH candidates.
Full breakdown (`runtime_issue` category=entry_selector_empty): 561 wrong type (calls, correct for a
short), 295 out of the 3–7 DTE window (chain offered 3/6/10/13/15 → only 3,6 qualify), then of the
~266 in-window puts: **168 open_interest_below_min, 55 delta_below_min, 34 delta_above_max, 9
spread_above_max → 0 survived**. Dominant blocker = open interest (thin SMH short-DTE puts) + delta
band. Fallback `allow_nearest_after` (→10 DTE) exists but those contracts also failed OI/delta.

**The lever (operator judgment needed):** the OI floor + delta band (0.15–0.35) + spread max are
protecting against illiquid/bad fills, but they also mean SMH-type lanes go quiet when their
short-DTE chain is thin. Loosening trades more at the cost of fill quality. This is a live-money
behavior change → operator decides the thresholds before any tuning. Candidate next build increment
once the trade-off is decided. Also relevant: this suppression is invisible in the daily P&L (looks
like "quiet day") — worth a report line counting entry_selector_empty per live lane so silent
live-entry blocks surface.

### 2026-07-07 (Tue, evening) — CORRECTION: a live lane DID fire; selector blocked it
Re-examined "why nothing live today" (operator asked). NOT true that no live lane signaled: SMH live
signaled short 08:59 + 09:03 CT. Both hit `entry_selector_empty` — all 1122 SMH contracts rejected
(see §11). So today's live $0 was a VEHICLE-FILTER block, not a signal drought. Elevated #11 from
cosmetic filler to live-impact item pending operator call on the OI/delta thresholds. Earlier diary
lines for 07-07 saying "no qualifying signal" are superseded by this.

### 2026-07-08 (Wed, late morning) — FIRST LIVE WINNER via the full ladder + #9 fix confirmed
**The milestone we've waited for since flip day: a LIVE position ran the complete profile ladder,
end to end, and won.** QQQ live (row_5): entry 2.88 → **partial_scale** (banked T1, 10:18 CT) →
**stop_to_breakeven** (10:19) → runner rode → **high_water_giveback square_off @ 3.75 = +$87** (10:36).
All dispatches dry_run=0 (real), full target suppressed (exit-authority rule). The partial-and-runner
mechanic — proven on shadow 07-06 — now proven **LIVE in dollars**.
AMD live (row_2) also fired: target_1_partial → square_off (1-lot can't split, so squares at T1);
**DATA GAP**: exit submitted + filled + position flat + reconciliation clean (broker_recovered 0),
but exit_price/exit_filled_qty did NOT write back to trade_sessions → AMD's live P&L is unrecorded.
QQQ's final square_off wrote back fine (3.75); the gap appears specific to the target_1_partial→
square_off path on a 1-lot. Not a risk (book flat, no naked position) but undercounts live P&L →
INVESTIGATE (candidate small build). Shadow: AAPL −$136.
**#9 bar-fetch fix CONFIRMED WORKING:** heartbeat max **54s → 16.7s**, avg 7.9s → 4.0s. The
concurrency deploy delivered. Health otherwise clean (identity_mismatch 0, budget_unavail 0,
MTM warnings 0). Experiment tally: 07-02 +$1,001 · 07-06 −$293 · 07-07 $0 · 07-08 +$87 live so far
(AMD P&L pending the data-gap fix).

### 2026-07-08 (Wed, midday) — book flat after the milestone morning; selector blocked AMD twice more
12:45 CT: no new trades since the morning's QQQ +$87 (first live full-ladder winner) and AMD
(P&L pending backfill). Book flat, health clean (identity_mismatch 0, broker_recovered 0, heartbeat
max 16.7s — #9 fix continuing to hold). **Selector-empty hit 4 more times today, 2 on AMD LIVE**
(row_2 — after its morning trade closed, follow-up signals found no acceptable contract) — the #11
vehicle-policy question is now costing live entries on 2 of the last 2 sessions; operator decision
on OI-as-aggressiveness numbers pending (brainstorm 07-08). Exit-race audit verdict SAFE-WITH-FIXES:
3 fixes (fail-closed readback, partial-fill-at-cancel handling + oversell prevention, sweep timeout/
give-up) dispatched to the worker; deploy at tonight's close block after re-verification.

## §11b Vehicle-policy decisions (operator, 2026-07-08 afternoon)

**Rails DECIDED + APPLIED:** halt 7.5% / flatten 11.25% (env on oldmac; live at next 08:20 start).
On today's $10,237 budget: halt ≈ −$768, flatten ≈ −$1,152. Coherent with current caps: one worst-case
stop-out at the $2,000 cap (−$700) stays under the halt line (survives one, halts before two).
Ratio preserved at 1:1.5. Revisit as account grows.

**OI: per-symbol LEARNED thresholds, not a global floor (operator's design).** A fixed OI=100 means
nothing on SPY and everything on SMH — liquidity is symbol-relative. Direction: judge each contract's
OI as a PERCENTILE within its own symbol's chain (per DTE window), and LEARN what "good OI" means
per symbol by correlating entry OI-percentile with realized fill slippage once chain snapshots +
fills accrue. Guard against the uniform-dead-chain failure (top-30%-of-nothing): keep a low absolute
sanity bound + spread co-gate, and OI tier still maps to entry AGGRESSIVENESS (near-bid passive +
no-chase on thin) per the 07-08 brainstorm. Chain-snapshot capture (approved) is the training set —
build `option_chain_snapshots` at selection time, collect 3-4 sessions, then set the percentile
policy from data.

**Premium cap: DEFERRED pending snapshot data** (interpretation to confirm with operator): caps stay
per-lane as-is; the multi-lot question folds into the same learning loop (cheaper-strike preference
within existing caps first; cap raises revisited with the slippage data and the new rails).

### 2026-07-08 (Wed, afternoon supplement) — decisions landed; book unchanged
14:02 CT: book unchanged since midday (3 trades, flat, health clean, heartbeat max steady 16.7s).
Afternoon output: operator decisions locked in brainstorm — rails **7.5/11.25 APPLIED** to env
(−$768/−$1,152 at tomorrow's start), OI → per-symbol percentile-learning design (§11b), caps
deferred pending data. Public API confirmed NO historical chains → chain-snapshot capture built,
audited-lite, **merged d285ebb** alongside the 3-round-audited hygiene batch (87fb8e1); both deploy
at tonight's close block. QQQ true size confirmed 3-lot (banked leg 2x pending backfill with AMD's
orphaned exit).

### 2026-07-08 (Wed, close) — corrected day +$689 live; vehicle policy phase 1 + 2 deploys landed
**Corrected day tally (after broker backfills):** live **+$689** — QQQ full ladder: banked leg 2×@3.94
(+$212, backfilled to trade_partial_fills) + runner +$87 = **+$299**; AMD **+$390** (exit 14.30
backfilled — the orphaned fill was a WIN, not a scratch). Shadow AAPL −$136. Day total +$553.
**Experiment tally: 07-02 +$1,001 · 07-06 −$310 · 07-07 $0 · 07-08 +$689 = +$1,380 live since flip**
(June baseline: ≈−$1,000/month). Rails: none fired. Health clean all day.
**Deployed tonight (oldmac @ d285ebb):** #20 hygiene batch (3-round audited exit-race fix +
partial-leg accounting + selector visibility + can_ladder) and chain-snapshot capture (percentile-OI
training data starts tomorrow). **Vehicle policy phase 1 applied to the 5 live rows** (backup taken;
LANE_CONFIG diff = exactly the 4 cap raises): min_open_interest 100→**50**, caps → **$2,000 uniform**
(coherent with the operator rails 7.5/11.25 set today: worst stop-out −$700 < halt −$768). Compiled
plan verified (oi_min=50 cap=2000 on all 5), boot green. Phase 2 = per-symbol percentile OI +
entry-aggressiveness tiering, built from snapshot data (~3-4 sessions).
Tomorrow watch: first session on new floor/caps — expect multi-lot entries (can_ladder tag now in
reports), chain snapshots accruing, corrected accounting end-to-end.

### 2026-07-09 (Thu, morning) — first session under vehicle policy phase 1; clean start
08:26 CT, session 6 min old: live-start rc=0, budget prefetched instantly (third consecutive clean
startup for audit-P2 fix). **New policy survived the 08:20 Sheet resync**: all 5 live lanes compiled
with oi_min=50, cap=$2,000; rails 7.5/11.25 active. No trades yet (entry windows opening); chain
snapshots correctly 0 until first signal. Watching for: first multi-lot live entry (can_ladder),
AVGO-EB shadow conversion (8 signals, 0 trades under old floor), AMD lane clean after 07-08
dead_lane, first snapshot rows.

### 2026-07-09 (Thu, 08:51 update) — floor-50 validated in 21 minutes
**AVGO elastic-band converted its FIRST trade ever** (8 signals/0 trades under old floor): 2-lot,
2.36→2.48, +$24 paper, can_ladder=1 — precisely the lane the decision data flagged. Chain snapshots
live (3 attempts, 1,010 contract rows). Selector empties 0 (vs 4 yesterday). can_ladder tag
populating (TSLA/META shadow 1-lots tagged 0). Heartbeat 6.1s max. No live-lane signals yet.

### 2026-07-09 (Thu, 10:00 update) — FIRST LIVE T2 RUNNER: QQQ +$1,064; book transformed
**The experiment's target event happened:** QQQ live entered 10-lot @ 1.94 (new cap), banked 6 @ 2.72
(target_1_partial +$468, auto-recorded in trade_partial_fills), stop→breakeven, 4-lot runner rode to
**target_2_runner @ 3.43 (+$596)**. First T2 exit ever; full exit DNA at proper size = +$1,064 one
trade. Throughput transformed: 18 trades by 09:59 (> any full prior day); PLTR shadow ladder banked
partial + giveback +$70; AVGO-EB 4 trades (was 0 lifetime); snapshots 6,020 rows/18 attempts;
selector_empty 0; can_ladder=1 on 12/18. Open live: QQQ 10@1.98 + NVDA 7@2.57 (~$3.8k deployed —
NOTED: two concurrent max-size stop-outs (~−$1.3k) would jointly breach flatten tier −$1,152;
coherent with rails-as-catastrophe-brake; flagged concurrent-exposure cap as optional rail addition).
Health clean (hb max 15.6s).

### 2026-07-09 (Thu, midday) — round-trip to flat; exits proven both ways; deploys verified
Live day settled at −$32: QQQ T2 runner +$1,064, then NVDA no_progress cut −$406 (saved ~$220 vs
full stop) and QQQ #2 disaster stop −$690 (filled −34.8% vs 35% design — zero live slippage). Five
distinct exit rules fired today, each per design intent; suppressed-target rule held on all armed
trades. Book now flat. Signals: 36 → 18 entries + 18 open_protected blocks, 0 selector empties.
Live since flip +$1,348 (9 trades); shadow WTD −$857 (18 trades — META/AVGO impulse churn is the
signal-quality evidence for #5). Deploy verification: oldmac==origin==local (1d7ab16), rails
7.5/11.25 confirmed in startup event, partial-leg accounting auto-recorded first real T1 bank.
Operator aligned with midday readback. TONIGHT'S BUILD FLIPPED: #5 weekly scorecard first (Friday
synthesis needs it), #21/#22 after if time allows.

### 2026-07-09 (Thu, evening) — BACKLOG FOLD-IN: 5 items built, audited, DEPLOYED in one day
Operator directive at midday: "fold all the build today itself from the backlog." Ran as a
supervisor lane (5 parallel build workers in isolated worktrees + 2 Fable adversarial auditors +
supervisor artifact review). ALL FIVE SHIPPED to oldmac (ae75dc8): #5 weekly scorecard (Friday
15:20 job — tomorrow's synthesis reads from it; first verdict: profile +$1,429 vs legacy −$1,629
this week), #21+#22 order-path hardening (3 audit rounds: reconciliation-race blocker caught round 2,
null-is-zero-fill caught round 3 — empirically settled from 89 real broker payloads), #6 session
reports → Obsidian approve/archive Inbox (verified end-to-end from oldmac; one integration fix:
absolutize source path), #7 shadow-EV daily report revived (15:30 CT; first edition delivered:
shadow book 24t, EV −$36/t since flip, only PLTR positive), #8 capability families (live-lane
parity adversarially proven 0-drift; 9 rows pass capability gate, 2 become lanes post-steward-
refresh). Audits produced 3 NEW latent findings → board items 23–25 (fail-closed evidence gates
needs sheet verification first — do NOT rush). 760 tests green at integration; all 7 launchd jobs
loaded. Vault Inbox has 3 review notes (TEST + 2 deploy-verify) — archive them from the phone as
the first exercise of the new approve/archive surface. #4 morning bias remains the only queued
build, deliberately operator-in-the-loop.

### 2026-07-09 (Thu, late) — THE BRAIN IS LIVE: phase 1 built same-night, acceptance test PASSED
Operator: "why can't you spin multiple agents and get it done tonight?" — done. 4 Opus workers wrote
docs/brain/{ARCHITECTURE,OPERATIONS,DECISIONS,STATE}.md (13 ADRs; live-verified deploy state);
supervisor wrote CLAUDE.md bootstrap contract + INDEX.md (never-re-derive facts) + bhiksha stub
(pushed). CLEAN-ROOM ACCEPTANCE TEST PASSED: a fresh zero-context agent answered the OI-floor
question (with the waterfall caveat + phase-2 plan), located exit-authority enforcement to the
line, and identified #23 as dangerous-to-rush with the correct failure mode — from bootstrap alone
(~590 lines; INDEX front-loaded ~70%). Tester also caught real staleness: VEHICLE_POLICY_DECISION
carried pre-revision rails → superseded banner added. Memory dir slimmed to pointers (4 notes →
brain). RFC decisions all closed (home=mala_v2; auto-commit + advisory curation; personal data in
memory_core while repos public; digest via Pulsar; Jarvis deprecated). Phase 2 next: nightly brain
steward (agent-broker + launchd, oldmac) + weekly curation digest card.

### 2026-07-09 (Thu, night) — BRAIN PHASE 2: nightly steward BUILT + first real run committed
Steward pipeline live end-to-end tonight: scripts/brain/{steward.py,steward_prompt.md,
steward_policy.yaml,freshness_lint.py} — deterministic runner gathers evidence (mala git log +
diary tail + supervisor-lane tail + bhiksha logs + READ-ONLY oldmac ssh: runtime git, launchctl,
latest_status.json), hires a TEXT-ONLY agent via agent-broker (opus→sonnet→codex, no tools),
parses file blocks fail-closed (path whitelist docs/brain/STATE.md + candidates/ only, length +
frontmatter + trust-banner validation), auto-commits per RFC 9a Q1. FIRST REAL RUN: opus layer-0,
outcome=updated, committed dd26502 — draft QUALITY-REVIEWED: upgraded claims to runtime-verified
(7 jobs verified EXECUTED with pids/rc, shadow-EV Telegram-delivered), caught dev-bhiksha 2 doc
commits ahead of runtime (correctly flagged non-money-path), preserved the #23 DANGER note, and
surfaced a NEW watch item: Schwab refresh token expires 07-14. Also filed its first candidate note
(AGENTS.md bootstrap move). Q2 REVISED with runtime evidence: steward home = DEV-MAC launchd
(21:45), not oldmac — oldmac's mala_v2 checkout is stale/diverged/pre-brain and the repo is
deliberately unpushed (ADR-012), so oldmac can't host canon without a 2-way sync protocol +
breaking the ssh-read-only rule + an interactive token mint. Friday digest → Lathi bus (doctor
green). REMAINING: operator runs `bash scripts/brain/install_brain_steward.sh install` (launchd
persistence needs operator's own hands — permission-gated, correctly).

### 2026-07-09 (Thu, later night) — steward DEPLOYED (launchd loaded) + Control Tower wiring built
Operator authorized deploy: `com.mala.brain-steward` installed + loaded on the dev Mac (launchctl
verified, exit 0). Control Tower tracking built the established way (bhiksha pattern: owner emits
status, Lathi projects): mala owns `scripts/brain/tower_status.py` (mala.launchd.status.v1 snapshot,
derived from run dirs/receipts/git/lint) + `tower_status_reader.py` (stdlib-only, recomputes
staleness at READ time on oldmac — dead Air renders stuck+stale_last_run in ≤27h, never frozen-
healthy). steward.py now pushes status after EVERY run incl. failures. Lathi `[sources.mala]`
(C.4, status-only) added to external_sources.oldmac.toml — verified against Lathi's REAL adapter
locally: fresh→armed, 50h-stale→stuck+stale_last_run, missing→stuck. OPERATIONS updated with the
one sanctioned steward write to oldmac (non-secret snapshot → ~/Documents/mala_v2/artifacts/brain/
only). REMAINING (permission-gated, operator hands): first `tower_status.py --push`, oldmac lathi
pull + tower kickstart, tower JSON readback.
