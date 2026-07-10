---
as_of: 2026-07-09T22:44-05:00
sources:
  - docs/LIVE_LOOP_WORKPLAN.md            # status board + dated diary (07-09 entries)
  - .supervisor-lane/STATE.md             # 07-09 backlog fold-in verdict log
  - mala_v2 git log (dev Mac, canonical)  # through 72e00fa 07-09 22:08
  - bhiksha git log (dev Mac checkout)    # through 0f66ad1
  - oldmac runtime: git log + launchctl + latest_status.json (readback 2026-07-09 ~22:44 CT)
replaced_by_steward: nightly (live)        # this file is REPLACED each night, not appended
---

# STATE — what is true right now

> **Freshness:** This file describes **2026-07-09 (late evening, CT)**. If today is later,
> prefer the diary tail (`docs/LIVE_LOOP_WORKPLAN.md`) + live runtime readback over this
> summary. Trust order (RFC §5.3): **runtime evidence > diary > this brain summary.**
> Dollar figures appear here only because both repos are local-only for the live diary
> (mala_v2 is unpushed; see RFC §9a Q3).

## The experiment

Month-long **live profile-exit test**, started **2026-07-02** (operator committed for a
month). **19 lanes: 5 live + 14 shadow.** Live book is a TREND_CONTINUATION monoculture
(17/19 lanes) — profile coverage is the strategic gap, not yet the live focus.

- **Live realized +$1,348 over 9 trades** since flip, through 07-09. Day-by-day live:
  **07-02 +$1,001 · 07-06 −$310 · 07-07 $0 · 07-08 +$689 · 07-09 −$32.** June baseline
  ≈ −$1,000/month. [diary 07-09 midday; 07-08 close]
- **07-09 landmark — first live T2 runner.** QQQ live entered 10-lot @1.94 (new $2k cap),
  banked 6 @2.72 (T1 +$468), stop→breakeven, 4-lot runner rode to target_2_runner @3.43
  (+$596) = **+$1,064 in one trade** — first T2 exit ever at proper size. The day then
  round-tripped to −$32 (NVDA no_progress cut −$406; QQQ #2 disaster stop −$690 filled at
  −34.8% vs 35% design = ~zero live slippage). Five distinct exit rules fired, each per
  design; suppressed-target rule held on all armed trades. [diary 07-09 10:00 + midday]
- **First scorecard verdict (wk 07-06→09):** profile-rule exits **+$1,429 (8t, 5W)** vs
  legacy-path **−$1,629 (15t, 3W)**; live-only profile +$1,347 (4t) vs legacy −$690 (1
  stop). Promotion candidates: 0 (META near-miss −$146/6t). [.supervisor-lane 14:42]
- **Shadow book since flip — VERIFIED live 07-09 (delivered Telegram report):** 24 trades,
  **−$864, EV −$36/trade, wr 33%**, only the **PLTR** lane positive (+$119). 8 lanes
  active / 12 traded. [oldmac latest_status shadow-ev-report; diary 07-09 evening]

## Deployed versions

- **bhiksha runtime (oldmac) = `ae75dc8`** — VERIFIED live 2026-07-09 ~22:44 CT via
  oldmac `git log -1` on `/Users/sunny/Documents/bhiksha` (runtime checkout, NOT ~/code);
  `main...origin/main` clean. This is the money-path HEAD that traded today. [oldmac git log]
- **Dev-Mac bhiksha checkout is 2 doc-only commits AHEAD of runtime:** `0f66ad1` / `46a9d0d`
  (bootstrap contract → AGENTS.md; CLAUDE.md thin stub pointing at the mala_v2 brain). These
  are NOT money-path and NOT on oldmac — runtime remains `ae75dc8`. [bhiksha git log]
- **mala_v2 canonical (dev Mac) = `72e00fa`** (07-09 22:08 — "Bootstrap contract home =
  AGENTS.md (agent-agnostic); CLAUDE.md = thin @import, RFC 9a Q6"). [mala_v2 git log]
- **7 launchd jobs — VERIFIED loaded AND executed tonight** (oldmac `launchctl list` +
  `latest_status.json`): live-watchdog (pid 26669 running since 08:20 CT) · live-stop
  (rc=0, terminated 15:10 CT) · schwab-guard (token healthy) · session-report (ran,
  published to Obsidian) · shadow-ev-report (delivered Telegram, msg 539) · weekly-scorecard
  (produced report, alert mode OFF) · live-start. [launchctl; oldmac latest_status]
- **What shipped 07-09 evening** (supervisor lane @ ae75dc8, 5 workers + 2 Fable audits):
  #5 weekly profile-vs-legacy scorecard (Friday 15:20 job) · #6 session→Obsidian
  approve/archive Inbox · #7 daily shadow-EV report (15:30 job) · #8 capability families
  (manifest v1→v2; 9 rows pass gate SHADOW-only, only 2 become lanes post-steward-refresh)
  · #21 dead-status `filledQuantity` guard (3-round audited) · #22 sweep-abandonment
  escalation. 760 tests green at integration. [.supervisor-lane 17:45; bhiksha git log]

## Config now live (from documents — NOT re-verified against Sheet tonight)

- **Rails 7.5% / 11.25%** (halt / flatten) — "till the account grows." Diary reports these
  confirmed in the 07-09 startup event on oldmac. [workplan §11b; diary 07-09 midday]
- **OI floor 50** (lowered from 100, vehicle-policy phase 1, 07-08 eve). [workplan §11b]
- **$2,000 uniform premium caps** on the 5 live rows. [diary 07-08 close]
- **5 live rows: `runtime_mode = live_approval_gated`.** [prior STATE — not re-verified
  tonight]. Deploy-time compile verified oi_min=50/cap=2000 on all 5 per diary 07-08 close;
  no read-only Sheet-cell check performed this session.
- **Schwab token healthy — VERIFIED live tonight:** refresh token expires **2026-07-14**
  (4.7 days left) via premarket direct_refresh ok. [oldmac latest_status schwab-guard]

## Queue (priority order)

1. **Morning-bias overlay (#4)** — operator-in-the-loop by design; the only remaining
   backlog build. Awaiting operator's earlier-routine material + first daily brief. [board #4]
2. **Brain phase 2 — steward now live** (this nightly run). Phase 1 COMPLETE 07-09
   (acceptance test PASSED). Bootstrap contract moved to AGENTS.md (agent-agnostic),
   CLAUDE.md = thin @import (RFC 9a Q6) — see candidate note. Next: weekly curation digest.
   [mala_v2 git log 72e00fa; diary 07-09 late]
3. **Board latent audit findings #23–25** (from the 07-09 #8/#21 audits):
   - **#23** fail-closed live evidence gates (MEDIUM). **DANGER:** needs **real-sheet-cell
     verification FIRST** — blank `activation_candidate` + a live-armed row means the naive
     `is not True` fix could **suppress the live book** if current cells are blank. Do not rush.
   - **#24** m7 gate literal-match gap (LOW) — matches literal "block", misses
     `provider_blocked`; fix = match block-prefixed statuses.
   - **#25** per-deployment exception isolation in `_handle_bar_event` (MEDIUM) — one poison
     lane can abort the rest of a symbol-bar; live lanes protected only by row order today.
4. **Vehicle policy phase 2** — per-symbol percentile OI + entry-aggressiveness tiering,
   built from `option_chain_snapshots` after 3–4 sessions (accruing since 07-09: 6,020 rows
   on day one). [workplan §11b; diary 07-09 10:00]
5. **Friday 2026-07-10 15:20 CT** — weekly scorecard **first DELIVERED run** + weekly
   synthesis (NO build increment). Render already verified on oldmac tonight (report produced,
   alert mode OFF = Telegram delivery still unexercised). [oldmac latest_status; cadence]

## Watch items

- **First live multi-lot ladders at scale** — 07-09 was day one at the new $2k cap /
  floor-50 (QQQ 10-lot). Watch fill quality and ladder behavior as size persists.
- **Concurrent-exposure question (flagged 07-09, operator aware, OPTIONAL rail):** two
  max-size live positions (e.g. QQQ 10@1.98 + NVDA 7@2.57, ~$3.8k deployed) can *jointly*
  breach the flatten tier (−$1,152) even though each alone survives. A concurrent-exposure
  cap is an optional addition, not built. [diary 07-09 10:00]
- **Schwab refresh token expires 2026-07-14** (healthy tonight, 4.7 days left). Guard job
  refreshes premarket; watch that it renews before expiry. [oldmac latest_status schwab-guard]
- **Cadence cron jobs expire ~07-13.** The watch/build cron lives in the dev-Mac Claude
  session (session-scoped, 7-day life) — NOT the oldmac deterministic layer. If gone, tell
  Claude "resume the trading cadence"; spec = workplan "Operating cadence" section.
- **Vault Inbox review notes awaiting archive** — a new deploy-verify session-report note was
  published tonight (20:20 UTC, YELLOW/provider_warning) on top of the prior TEST + deploy
  notes; the #6 approve/archive surface is verified end-to-end but notes still unarchived.
  [oldmac latest_status session-report; diary 07-09 evening]

## Open risks / honesty notes

- Runtime tier IS available tonight; deploy/launchd/token claims above are VERIFIED live.
  Config values (rails, OI floor, caps, runtime_mode) are still **from documents** — not
  re-checked against the live Google Sheet or compiled plan this session.
- Dev-Mac bhiksha is ahead of the oldmac runtime by 2 doc-only bootstrap commits
  (`0f66ad1`); the money path on oldmac is unchanged at `ae75dc8`. Do not report the bootstrap
  commits as deployed.
- The weekly scorecard produced a real report on oldmac tonight (render verified) but with
  alert mode OFF — **Telegram/live delivery is still unexercised** until Friday 07-10 15:20.
- Shadow EV is negative (−$36/t, runtime-confirmed); 0 promotion candidates. The live edge
  rests on entry win rate — one full T2 ≈ one stop + one stagnation cut (exit economics
  confirmed 07-09). [oldmac latest_status shadow-ev; diary 07-09 midday]
