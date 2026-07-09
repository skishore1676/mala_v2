# Supervisor Lane — Exit-Profile Adoption (Wave 1 Step 5)

Started 2026-06-13. Supervisor: primary lane (Claude). Operator: Suman.

## Objective
Adopt the 4 operator exit profiles into live-CAPABLE automated trading across
kernel → bhiksha → mala, shadow-first but genuinely production/live-ready.
**Operator flips live himself.** Stopping condition: the profile exit path runs
in bhiksha SHADOW on real inputs with inspectable receipts, kernel contract +
mala emission landed on branches, all reviewed at gates — live flip left to Suman.

## Source of truth
- mala_v2 main `db1e6f7` (merged). Validated profile logic:
  `src/oracle/trade_simulator.py:ProfileExitPolicy`, `src/research/exit_profiles.py`,
  `src/research/option_translation.py`. Canonical: `docs/EXIT_PROFILE_PLAYBOOKS.md`.
- Live option dials: `public_api_trading_v3/.../exit_policies/profiles.py`.
- Profiles are operator-live-validated (Suman trades by hand) and TIGHTER than hold-to-eod.

## Safety boundaries (HARD)
shadow-first · NO real orders · NO live enablement · NO Google Sheet auto-push
(dry-run only) · NO oldmac/machine sync · branch-only, NO merge to main without
Suman review. Live flip is Suman's, outside the lane.

## Waves
- **W1 — KERNEL (in progress):** extend `mala-bhiksha-kernel` ManagementPolicySpec
  with Tier-1 profile fields (additive, back-compat), bump capability, tests.
  Gate: contract reviewed + kernel tests green.
- **W2 — BHIKSHA + MALA (pending W1 gate, parallel):**
  - bhiksha: profile-aware exit evaluator on live premium + FSM, capability
    manifest, shadow-first, live-ready. Adversarial audit of the live path.
  - mala: emit chosen profile as ManagementPolicySpec in the Execution packet
    (dry-run; no Sheet push).
- **W3 — INTEGRATION + SHADOW PROOF:** wire kernel→mala→bhiksha; produce a
  shadow receipt (profile exit decisions on real inputs). Gate: inspectable proof.

## Verification
Each repo: its own test suite green + post-commit `git status --short` + commit hash.
Integration: re-run from supervisor checkout; shadow receipt artifact.

## Worker log
- W1 kernel: **DONE + Gate 1 verified** → `mala-bhiksha-kernel` branch `exit-profile-mgmt-spec` @ `e6c3dec`. v2 ManagementPolicySpec (Tier-1 fields, back-compat, giveback validator), capability bumped v1→v2, 17 tests green. Verified: ManagementPolicySpec is NOT embedded in any packet (packets use ManagementPolicy) → no fingerprint impact. **Kernel repo LEFT on this branch so Wave 2 imports v2.**
- W2 bhiksha: dispatched (background, opus) → `bhiksha` branch `exit-profile-evaluator`. Profile-aware exit evaluator on live premium+FSM, shadow-first, capability declare.
- W2 mala: **DONE** → mala_v2 branch `exit-profile-emit` @ `1a836d5` (in worktree agent-a9faa7d55d126bea3). Emits FLASH_REVERSAL etc. profile into ManagementPolicySpec v2 (mapping verified in report); additive/back-compat; also fixed shared_kernel worktree path resolution; added disaster_stop_pct + eod_flat to OPTION_PROFILES. 400 tests in worktree. **Full Gate-2 verify pending** (run its tests from integration checkout).
  - Worker surfaced a real latent bug in main: `option_translation.py` empty-pnls return referenced removed `iv_model` → **FIXED on main `b4de4cb`** + regression test (399 green). mala branch merges cleanly over it (different lines).
- W2 bhiksha: **DONE** → `bhiksha` branch `exit-profile-evaluator` @ `15fadad`. New profile_exit.py (ladder evaluator) + profile_exit_shadow.py (recorder) + capability + ExitSpec dials; 388 tests. VERIFIED: new files place NO orders (only build ExitDecision); single dispatch gate `profile_exit_dispatch_allowed` closed-by-default; new capability advertises `runtime_modes=["shadow"]` only. Self-audit honestly flagged 5 live risks — top two: (1) PARTIAL_SCALE maps to square_off but supervisor `_handle_exit_locked` ignores `exit_quantity` → a live "partial" would flatten the whole position (deferred to live-wiring); (2) ProfileExitState per-tick persistence not wired. Both shadow-safe (no live).
- W2 mala: independently re-verified (30 playbook_surface tests pass incl. 2 emission tests).
- W2.5 adversarial audit (opus, read-only): **DONE.** Verdict — **(a) shadow use SAFE to accept** (evaluator is pure, recorder only appends events, NO broker path, and it is UNWIRED from the runtime — gate closed in every shadow/non-live combo). **(b) live path BLOCKED** pending named fixes:
  - **C1 (CRITICAL):** PARTIAL_SCALE→square_off but supervisor `_handle_exit_locked` sizes closes with full `position.quantity` and never reads `features["exit_quantity"]` → a live "partial" flattens the whole position. (`supervisor.py:1506-1520`)
  - **C2 (CRITICAL):** gate whitelists `live_automated` (no-approval full-auto) — every other bhiksha gate forbids it. (`profile_exit.py:641`)
  - **H1 (HIGH):** gate is a denylist; `runtime_mode=None` / unknown `position_source` (live_pending, broker_recovered, None, typo) fail OPEN. Make it an allowlist / fail-closed. (`profile_exit.py:635-643`)
  - **H2 (HIGH):** STOP_TO_BREAKEVEN output (`replacement_stop_price`) is never read by the supervisor → the breakeven ratchet the ladder promises isn't delivered. (`profile_exit.py:573-605`)
  - **H3 (HIGH):** `ProfileExitState` (peak/T1-banked/breakeven-emitted) isn't persisted per trade → a naive per-tick loop re-banks partials, never arms giveback. (no lifecycle owner)
  - M1 (EOD silently skipped if caller omits bar_time_et), M2 (inverted disaster<initial stop), L1 (no_progress floor hardcoded 0.25) = pre-live hardening. M3 (capability honesty) — claim HELD (runtime_modes shadow-only).

## GATE 2 DECISION
Wave 2 ACCEPTED for SHADOW (kernel `e6c3dec`, mala `1a836d5`, bhiksha `15fadad`; all branch-only, unmerged). LIVE path blocked on C1/C2/H1/H2/H3. C2+H1 are gate-only safety tightening (safe to fix in the bhiksha branch). C1/H2/H3 modify the LIVE supervisor engine → **operator approval gate** before a worker touches `supervisor.py`. Nothing merged to any main pending Suman review.

## Wave 3 (operator approved 2026-06-13: full hardening incl. live engine + shadow receipt)
- W3a hardening (bhiksha, opus, continues `exit-profile-evaluator`): fix C1/C2/H1/H2/H3 + M1/M2/L1. C2/H1 = gate fail-closed allowlist; C1 = supervisor honors exit_quantity for partials (residual stays open, never flatten on partial); H2 = supervisor consumes replacement_stop_price (deliver breakeven ratchet); H3 = persist ProfileExitState per trade. Still shadow-first; no live enablement, no real orders. DISPATCHED.
- W3a hardening: **DONE + verified** → `exit-profile-evaluator` @ `42c2fbf`. Gate now strict allowlist (`DISPATCH_ALLOWED_RUNTIME_MODES={live_approval_gated}`, position sources {live_open,live_pending}, live_automated dropped, fail-closed); partial handler `_handle_partial_scale_locked` keeps residual + raises rather than flatten; breakeven `_apply_replacement_stop`; ProfileExitState persisted+cleared-on-close; M1/M2/L1 done. 411 tests green. Capability still shadow-only (defense-in-depth).
- W3b re-audit (opus, read-only): **DONE.** Shadow-safe CONFIRMED (accept). C1/C2/H1/H2/M1/M2/L1 sound as gated shadow-only. LIVE still blocked: NEW-1 (naked residual on stop-less partial, supervisor.py:1618 re-arm gated on restored_stop_price), NEW-2 (double-stop when cancel_protection_orders=False + live stop, :1618-1633), NEW-3 (H2 cancel-OK/place-fail naked window, no auto-reprotect), NEW-4 (ProfileExitState not cleared at close sites :2273 EOD-sweep + :2439 halt; and get_or_create has ZERO prod callers → H3 unwired), NEW-5 (gate enum vs str), NEW-6 (phantom stop_price persisted on failed stop). All latent (evaluator unwired; no live strategy emits partial_scale/replacement_stop_price).
- W3c CONVERGENCE (bhiksha, opus, continues branch): **DISPATCHED** — fix NEW-1/2/3/4/5/6 (surgical) + wire a SHADOW receipt driver (creates the production caller of get_or_create/clear ProfileExitState → completes+proves H3 across ticks; gate closed; no live, no orders) emitting an inspectable artifact.
- W3c convergence: **DONE + verified** → `exit-profile-evaluator` @ `f7ed78d`. NEW-1..6 fixed (worker stash-tested each new test fails pre-fix); shadow-receipt driver `src/bhiksha/tools/profile_exit_shadow_receipt.py` → artifact under `artifacts/profile_exit_shadow/` (gitignored, reproducible). 422 tests. I re-ran driver myself: gate CLOSED, total_dispatched=0, orders=0, broker_api_touched=False, all_terminals_matched=True, all ladder rungs observed. H3 now has a real caller (state persists across ticks then cleared).
- W3d FINAL re-audit (opus, read-only): **DONE.** (a) Shadow-safe + receipt valid = **YES** (evaluator import-isolated from live loop; recorder/driver order-free; gate structurally fail-closed; receipt exercises H3; 422 tests). (b) Live = **NOT live-wired** (evaluator not in PositionMonitor.evaluate_symbol — a flip alone won't activate it) + residual latent fixes: MEDIUM-1 (phantom stop_price after failed partial-residual stop, supervisor.py:1747 comment lies vs :2161), NEW-4 (state not cleared in `_mark_trade_closed_with_exit_truth`/`_mark_disappeared_trade_closed`/`_reconcile_pending_entry_release`; LOW), HIGH-2 (re-arm no-ops if deployment has no recovery stop pct). All latent (behind unwired evaluator + closed gate).

## LANE CLOSEOUT (2026-06-14) — gate language: FIXTURE SHADOW PROVEN; live = SKELETON+blocked
**Stopping condition reached for shadow; live integration is a separate future phase.**
- Branches (all branch-only, NOT merged, NOT live): kernel `e6c3dec` (v2 ManagementPolicySpec + capability v2, 17 tests); mala `exit-profile-emit` `1a836d5` (emits profile into spec, back-compat, 400 tests, optimizer profiles gated off); bhiksha `exit-profile-evaluator` `f7ed78d` (profile-aware exit evaluator + hardened live handlers + shadow-receipt driver, 422 tests, capability shadow-only, UNWIRED from live loop).
- Verified by supervisor: ran each suite; re-ran the shadow receipt driver (gate CLOSED, 0 dispatched, 0 orders, broker untouched, all ladder rungs, state persist+clear). 3 adversarial audits (opus; fable inaccessible).
- Also fixed on mala main during the lane: `option_translation` iv_model NameError (`b4de4cb`).
- Suman decisions (2026-06-14): **MERGED all 3** + **greenlit supervised live-integration.**

## MERGE DONE (2026-06-14)
kernel main `552d029` (pushed), mala main `5be15a2` (pushed; iv_model=0, dials in, 401 tests), bhiksha main `b2b50a9` (pushed; merged origin's unrelated `ad614ef`; 437 tests). Production unchanged (optimizer profiles gated off, capability shadow-only, evaluator unwired). Feature branches retained.

## WAVE 4 — LIVE INTEGRATION (supervised; operator flips live)
- W4a (bhiksha, opus, new branch off main): wire evaluator into the live monitor as SHADOW-RECORD DUAL-RUN — existing exit path stays authoritative + unchanged; profile decisions are recorded (shadow) on profile deployments; a `default-OFF` operator flag is the only thing that would later let the profile decision drive the exit. Plus fix MEDIUM-1 (clear stop_price on failed partial residual), NEW-4 (clear state in the 3 reconcile closes), HIGH-2 (require recovery stop pct on profile deployments). DISPATCHED.
- W4a wiring: **DONE + verified** → bhiksha branch `exit-profile-live-shadow-wiring` @ `f8e2527`. Shadow dual-run at `_manage_open_position_locked` tail; flag `profile_exit_drives_live` default OFF; flip seam dormant (audit marker, no route); MEDIUM-1/NEW-4/HIGH-2 fixed; 452 tests (+15, 0 deletions); invariant test proves byte-identical broker calls flag-off.
- W4b wiring audit (opus, read-only): **DONE.** (a) flag-off zero-change invariant HOLDS (profile branch provably can't reach broker; mutates only state no real path reads; sole delta = benign extra quote read + audit events). (b) **shadow wiring SAFE to accept on main.** (c) 2 PRE-FLIP items (NOT blocking shadow merge): **HIGH-1** — `runtime_mode` hardcoded literal at supervisor.py:270 (no real mode plumbed) → before flip, read the deployment's real mode so `live_automated` can't auto-dispatch; **MEDIUM-1(flip)** — shadow advances ProfileExitState each tick, so a mid-position flip mis-sizes/skips breakeven → reseed state on flip or only honor post-flip positions.

## LANE STATUS — SHADOW PHASE COMPLETE (2026-06-14)
Foundation merged to all 3 mains (kernel `552d029`, mala `5be15a2`, bhiksha `b2b50a9`). Live-shadow wiring on branch `exit-profile-live-shadow-wiring` @ `f8e2527`, audited 4× (1 shadow-evaluator + 1 shadow re-audit + 1 convergence re-audit + 1 wiring audit), flag-off proven zero-change, safe to merge. Shadow receipt artifact reproducible (`bhiksha .../tools/profile_exit_shadow_receipt.py`).
**Pre-flip checklist (operator's flip): HIGH-1 (plumb real runtime_mode) + MEDIUM-1(flip) (reseed state on flip). Live flip = set `profile_exit_drives_live` + wire the dormant route.**
**LANE COMPLETE (2026-06-14).** All merged to mains: kernel `552d029`, mala `ea6d0a2` (emit + iv_model fix + lessons), bhiksha `9a5065e` (evaluator + hardened + live-shadow wiring, flag default OFF). 5 adversarial audits total. Flip-seam fixes (HIGH-1 runtime_mode, MEDIUM-1 reseed) merged, 5th audit = SAFE. Lessons compounded → mala `docs/lessons/` (committed). Shadow receipt reproducible.
**Remaining for LIVE (operator's deliberate step, NOT done): wire the dormant dispatch route (`supervisor.py` ~336-349 currently only emits a `profile_exit_dispatch_ready` audit marker) + set `profile_exit_drives_live=True` + declare `execution.runtime_mode=live_approval_gated` on the deployment.** Until then: shadow-records on live data only. Real-data shadow accrues once a profile deployment exists + bhiksha runs in market hours.
Optional closeout hygiene: prune merged feature branches + the mala worktree `agent-a9faa7d55d126bea3`.
- W3c shadow receipt (AFTER W3a): wire the evaluator into a shadow runtime path (gate closed), run on real inputs, emit an inspectable receipt artifact.
Operator will review diffs before any merge; live flip remains Suman's.
