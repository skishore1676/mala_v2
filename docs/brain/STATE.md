---
as_of: 2026-07-09T21:30-05:00
sources:
  - docs/LIVE_LOOP_WORKPLAN.md            # status board + dated diary (esp. 07-09 entries)
  - .supervisor-lane/STATE.md             # 07-09 backlog fold-in verdict log
  - ~/.claude/projects/-Users-suman-code-mala-v2/memory/live-experiment-status-2026-07.md
  - live verify 2026-07-09 ~21:30 CT      # git log -1 (local + oldmac ssh), launchctl list (oldmac)
replaced_by_steward: nightly (planned)     # this file is REPLACED each night, not appended
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
  ≈ −$1,000/month. [workplan diary; memory note]
- **07-09 landmark — first live T2 runner.** QQQ live entered 10-lot @1.94 (new $2k cap),
  banked 6 @2.72 (T1 +$468), stop→breakeven, 4-lot runner rode to target_2_runner @3.43
  (+$596) = **+$1,064 in one trade** — the experiment's target event, first T2 exit ever
  at proper size. The day then **round-tripped to −$32** (NVDA no_progress cut −$406,
  QQQ #2 disaster stop −$690 filled at −34.8% vs 35% design = ~zero live slippage). Five
  distinct exit rules fired, each per design; suppressed-target rule held on all armed
  trades. [workplan diary 07-09 10:00 + midday]
- **First scorecard verdict (wk 07-06→09):** profile-rule exits **+$1,429 (8t, 5W)** vs
  legacy-path **−$1,629 (15t, 3W)**; live-only profile +$1,347 (4t) vs legacy −$690 (1
  stop). Friday 07-10 15:20 CT = its live debut. [.supervisor-lane 14:42; board #5]
- **Shadow book since flip:** 24 trades, **EV −$36/trade**, only the **PLTR** lane
  positive. **Promotion candidates: 0** (META the near-miss: 6t / −$146). [board #7; #5]

## Deployed versions (VERIFIED live 2026-07-09 ~21:30 CT)

- **bhiksha main = `ae75dc8`** ("Integration fix: absolutize review-publish source path").
  Verified BOTH: `git log -1` on local `/Users/suman/code/bhiksha` **and** read-only ssh
  `oldmac: cd ~/Documents/bhiksha && git log --oneline -1` — **identical `ae75dc8`.**
  oldmac runtime checkout = `/Users/sunny/Documents/bhiksha` (NOT ~/code). [verified]
- **7 launchd jobs loaded on oldmac** (verified via `launchctl list`): `com.bhiksha.`
  live-start · live-stop · session-report · live-watchdog · schwab-guard · weekly-scorecard
  · shadow-ev-report. [verified]
- **What shipped 07-09 evening** (supervisor lane, 5 workers + 2 Fable audits) — board #5/6/7/8/21/22:
  - **#5** weekly profile-vs-legacy scorecard + Friday 15:20 CT job + Lathi-bus publish.
  - **#6** session reports → Obsidian `07 Agents/Coding/Inbox` approve/archive (env-gated,
    transport-graceful; integration fix = absolutize source path under bus cwd-switch).
  - **#7** daily shadow-EV report revived (15:30 CT job; first edition delivered).
  - **#8** capability families: 4 new capability strings, manifest v1→v2; 9 rows pass the
    capability gate (SHADOW-only), but only **2 become lanes** after the mala steward
    republishes `bhiksha_runtime_supported` (watch_only ×4, TSLA KILL ×2 stay held).
  - **#21** dead-status resubmit `filledQuantity` guard (3-round audited).
  - **#22** sweep-abandonment escalation → `runtime_issue` (partial_fill_abandoned).
  - Integration: 760 tests green; pushed ae75dc8; oldmac pulled + 7 jobs loaded. [board; lane]

## Config now live (from documents — NOT independently re-verified against Sheet tonight)

- **Rails 7.5% / 11.25%** (halt / flatten) — "till the account grows." Diary reports these
  confirmed in the 07-09 startup event on oldmac. [workplan §11b; diary 07-09 midday]
- **OI floor 50** (lowered from 100, vehicle-policy phase 1, 07-08 eve). [workplan §11b; #11]
- **$2,000 uniform premium caps** on the 5 live rows. [workplan diary 07-08 close]
- **5 live rows: `runtime_mode = live_approval_gated`.** [workplan] *(unverified — Sheet cell
  read-only check not performed this session; deploy-time compile verified oi_min=50/cap=2000
  on all 5 per diary 07-08 close.)*

## Queue (priority order)

1. **Morning-bias overlay (#4)** — operator-in-the-loop by design; the only remaining
   backlog build. Awaiting operator's earlier-routine material + first daily brief. [board #4]
2. **Brain phase 1 — IN PROGRESS tonight** (this `docs/brain/` scaffold + bootstrap test).
   RFC fully resolved 07-09: home = mala_v2, auto-commit + advisory curation, personal
   data in memory_core while repos public, phone digest via Pulsar. [RFC §9a; memory note]
3. **Board latent audit findings #23–25** (from the 07-09 #8/#21 audits):
   - **#23** fail-closed live evidence gates (MEDIUM). **DANGER:** needs **real-sheet-cell
     verification FIRST** — blank `activation_candidate` + a live-armed row means the naive
     `is not True` fix could **suppress the live book** if current cells are blank. Do not rush.
   - **#24** m7 gate literal-match gap (LOW) — matches literal "block", misses
     `provider_blocked`; fix = match block-prefixed statuses.
   - **#25** per-deployment exception isolation in `_handle_bar_event` (MEDIUM) — one poison
     lane can abort the rest of a symbol-bar; live lanes protected only by row order today.
4. **Vehicle policy phase 2** — per-symbol percentile OI + entry-aggressiveness tiering,
   built from `option_chain_snapshots` after 3–4 sessions of data (snapshots accruing since
   07-09: 6,020 rows on day one). [workplan §11b]
5. **Friday 2026-07-10 15:20 CT** — weekly scorecard **live debut** + weekly synthesis
   (NO build increment; Fridays are synthesis, not build). [cadence; board #5]

## Watch items

- **First live multi-lot ladders at scale** — 07-09 was day one at the new $2k cap /
  floor-50 (QQQ 10-lot). Watch fill quality and ladder behavior as size persists.
- **Concurrent-exposure question (flagged 07-09, operator aware, OPTIONAL rail):** two
  max-size live positions (e.g. QQQ 10@1.98 + NVDA 7@2.57, ~$3.8k deployed) can *jointly*
  breach the flatten tier (−$1,152) even though each alone survives. Coherent with
  rails-as-catastrophe-brake; a concurrent-exposure cap is an optional addition, not built.
- **Cadence cron jobs expire ~07-13.** The watch/build cron lives in the dev-Mac Claude
  session (session-scoped, 7-day life) — NOT the oldmac deterministic layer. If gone, tell
  Claude "resume the trading cadence"; spec = workplan "Operating cadence" section.
- **Vault Inbox has 3 review notes** (1 TEST + 2 deploy-verify) awaiting operator archive —
  the first real exercise of the new #6 approve/archive surface. [lane 17:45; diary evening]

## Open risks / honesty notes

- Config values above are taken from the diary/workplan, **not** re-checked against the live
  Google Sheet or compiled plan this session — the diary records them as verified at deploy
  time (07-08/07-09), which is not the same as verified-by-this-file tonight.
- The scorecard's Friday 07-10 run is its **first live execution**; the +$1,429 vs −$1,629
  verdict was rendered from a snapshot by the supervisor, Telegram/live delivery unexercised.
- Shadow EV is negative (−$36/t); 0 promotion candidates. The live edge rests on entry win
  rate — one full T2 ≈ one stop + one stagnation cut (exit economics confirmed 07-09).
