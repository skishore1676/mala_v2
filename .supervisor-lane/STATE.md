# Supervisor Lane — Backlog Fold-In (2026-07-09)

Started 2026-07-09 14:00 CT. Supervisor: primary lane (Fable). Operator: Suman.
Prior lane archived: `STATE-2026-06-exit-profile-adoption-ARCHIVED.md`.

## Objective
Operator directive (midday 07-09): "fold all the build today itself from the backlog."
Build, test, audit, and merge ALL buildable status-board items from
`docs/LIVE_LOOP_WORKPLAN.md` TODAY; deploy to oldmac after the live session ends;
readback + board/diary updates close the lane.

Stopping condition: every item below is merged+deployed with live readback, OR
explicitly reported blocked with exact residue. #4 (morning bias) EXCLUDED —
operator-in-the-loop by design.

## Items → workers (wave 1, all Opus, own bhiksha worktrees)
| Item | Worker | Branch |
|---|---|---|
| #5 weekly profile-vs-legacy scorecard + Friday job + bus publish | w-scorecard | build-5-weekly-scorecard |
| #21 dead-status resubmit filledQuantity guard + #22 abandonment→runtime_issue | w-hygiene | build-21-22-order-path-hardening |
| #6 session reports → Obsidian approve/archive via Lathi bus | w-obsidian | build-6-obsidian-bus |
| #7 revive daily shadow-EV report (app-owns-jobs re-home) | w-shadowev | build-7-shadow-ev-revival |
| #8 capability: market-impulse descendants + compression family (9 rows) | w-capability | build-8-capability-family |

## Safety boundaries (HARD)
- NO deploy / NO oldmac writes until live session hard-flat (15:55 ET; deploy ≥ ~15:15 CT).
- oldmac access read-only for workers (sqlite3 .timeout 8000; no file writes).
- NO Google Sheet writes. NO live-row config changes. #8 lands SHADOW-only.
- Workers never touch /Users/suman/code/bhiksha main checkout; own worktrees under
  /Users/suman/code/bhiksha-worktrees/ (PYTHONPATH=src; kernel symlink exists).
- Money-path changes (#21, #8) require adversarial audit (Fable) before merge.
- One TEST-labeled Obsidian bus post allowed for #6 verification (it IS the review surface).

## Gates
1. Wave-1 worker finals with commit + green tests (known-environmental: test_runtime_snapshot
   may fail in worktrees — verify fails at clean main too before dismissing).
2. Adversarial audits pass on #21 and #8; supervisor artifact review on #5/#6/#7
   (render real output, judge quality — not just tests).
3. Merge order: #21/#22 → #5 → #7 → #6 → #8; full suite at integration checkout.
4. Deploy oldmac at session boundary; live readback per deploy; board + diary + readback.

## Verdict log
- 14:00 CT wave 1 spawned.
- 14:15 CT #6 TERMINAL-ACCEPTED: 7a610bd on build-6-obsidian-bus, 678 tests pass, live TEST post
  verified on disk in vault Inbox (proper frontmatter + decision block). Deploy note: gate defaults
  ON — oldmac session-report will auto-project once deployed. #8 worker was DOA (0 tool calls),
  restarted 14:05 CT. #5/#7/#21-22 still running.
- 14:29 CT #21/#22 worker TERMINAL: 371f16a on build-21-22-order-path-hardening, full suite 671
  pass, clean tree. New origin="exit_dead_status"; runtime_issue category="partial_fill_abandoned".
  Fable adversarial auditor spawned (money-path gate) — merge blocked on its verdict.
  Worktree env lesson: bare PYTHONPATH=src insufficient; need main .venv python + kernel src on path.
- 14:42 CT #5 TERMINAL-ACCEPTED after supervisor artifact review: 3fedab7 on build-5-weekly-scorecard,
  676 tests pass, clean tree. Report re-rendered by supervisor from snapshot — all sums cross-check.
  FIRST profile-vs-legacy verdict: week profile +$1,429 (8t, 5W) vs legacy −$1,629 (15t, 3W);
  live-only: profile +$1,347 (4t) vs legacy −$690 (1 stop). Promotion candidates: 0 (META near-miss
  −$146/6t). Known gaps: Relaxed column renders "?" offline (populates from runtime deployments on
  oldmac); Telegram delivery unexercised until first oldmac run. Load-bearing find: session quantity
  is overwritten to runner residual at T1 bank — banked legs live ONLY in trade_partial_fills.
  Still running: #7, #8, #21/#22 audit.
- 14:52 CT #7 TERMINAL-ACCEPTED: cce7687 on build-7-shadow-ev-revival, 679 tests, clean tree.
  Real-data anchor matched (−$857/18t/5W WTD incl partials). Old report lineage root-caused:
  ai.openclaw.trading-systems-watch.plist.disabled + orphaned mala-shadow-daily.sh (Jun 5) →
  v2 rebuilt phone-first in bhiksha (15:30 CT weekdays job). Shadow book since 07-02: 24t −$864,
  EV −$36/t, only PLTR-lane positive. Remaining: #8 build, #21/#22 audit.
- 15:05 CT #21/#22 AUDIT VERDICT: UNSAFE (third consecutive round-2 catch in this code area).
  HIGH: ladder trusts position.quantity but reconciliation rewrites it from broker portfolio
  (excludes partial) → spurious finalize orphans unprotected live contract / undersell + zombie
  trade; deterministic across restart. Fix: derive from order payload's own quantity (also patch
  merged reprice ladder — same latent assumption). MEDIUM: dead-full-fill w/o averagePrice →
  exit_price NULL forever (enricher only accepts FILLED). 2 LOW. #22 clean. Fix mission sent back
  to original worker; auditor repros to become inverted regression tests; re-audit after.
- 15:15 CT #8 worker TERMINAL: 775b2bd on build-8-capability-family, 719 tests, clean tree, full
  scope (5 MI-descendant + 4 compression/vpoc rows unblocked, differential parity vs mala research
  342 signals/0 mismatch). Claims needing hostile verification: "cross_reclaim byte-equivalent"
  (market_impulse.py +429 lines — 5 LIVE lanes run it), warmup change scope, shadow-only structural
  guarantee, capability-manifest regeneration availability. Fable auditor spawned. Also noted: only
  2 of 9 rows immediately lane-eligible post-steward-refresh (watch_only ×5, TSLA KILL ×2 stay held).
- 15:35 CT #21/#22 FIXES LANDED: 698728f (both ladders order-qty-derived, dead-with-fill enrichment,
  unparseable-fill fail-closed, abandonment reorder; 7 inverted regressions; suite 678 green).
  Honest residual flagged: missing-quantity payload + reconciliation shrink → fallback = pre-fix
  behavior. Same auditor re-engaged: re-run own repros vs real payload shape, settle residual
  EMPIRICALLY from recorded broker payloads on oldmac, hostile-read the inverted regressions.
- 15:50 CT #21/#22 RE-AUDIT: UNSAFE — new HIGH introduced BY the finding-3 fix (supervisor-prescribed):
  null filledQuantity treated as unparseable, but empirically null = Public's zero-fill idiom
  (5/5 real zero-fill orders; 89/89 carry quantity key) → routine dead-unfilled exit wedges forever
  + runtime_issue flood. One-line fix + inverted F3 regression sent to worker. Findings 1/2/4 fixes
  HELD under real-payload attack; quantity-absent fallback = dead code in practice (empirical).
  Fourth consecutive confirmation: the round-2 same-auditor pass catches the worst bug.
- 16:20 CT #8 AUDIT: SAFE-TO-DEPLOY. Live-lane parity HELD (old-vs-new detector over real live
  params, 22 tests, 0 drift incl. exception/feature parity); warmup + capability regen no-change
  proven vs oldmac artifacts. 3 LATENT findings → new board items (NOT tonight: fail-closed evidence
  gates could suppress live lanes if current cells are blank — needs its own verify vs real sheet):
  (a) MEDIUM blank evidence cells + authorization_mode=live arms an un-vetted row live (2 cells,
  2 sheets); (b) LOW m7 gate matches literal "block", misses "provider_blocked"; (c) MEDIUM poison
  params pass compile then raise every bar with NO per-deployment exception isolation in
  _handle_bar_event. Truth correction: #8 unblocks 9 rows at capability layer, but only 2 become
  lanes post-steward-refresh (watch_only ×4… auditor says 4 not 5, KILL ×2). #8 ACCEPTED for merge.
  Waiting: #21 one-line null fix.
