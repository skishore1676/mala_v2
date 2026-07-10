---
as_of: 2026-07-09
sources:
  - mala_v2:docs/RESIDENT_INTELLIGENCE_RFC.md (§5.1–5.6)
  - mala_v2:docs/LIVE_LOOP_WORKPLAN.md (status board + diary)
  - mala_v2:agent.md (Mala_Evidence_v1, handoff)
  - mala_v2:docs/MALA_VISION_v2.2.md (M1–M7 gates)
  - ~/.claude/.../memory/trading-loop-architecture.md, exit-profile-gap.md
  - bhiksha:src/bhiksha/active_plan/compiler.py
  - bhiksha:src/bhiksha/ops/launchd_registry.py
  - bhiksha:src/bhiksha/risk/risk_manager.py
  - bhiksha:src/bhiksha/app/runtime.py, state/reconciliation.py, config/models.py
  - bhiksha:src/bhiksha/execution/profile_exit.py, execution/supervisor.py
  - bhiksha:README.md, docs/chart_review_manual.md
  - public_api_trading_v3:src/domain/trading/exit_policies/profiles.py
  - mala-bhiksha-kernel:src/mala_bhiksha_kernel/contracts/{packets,capabilities}.py
---

# ARCHITECTURE — the 4-repo trading organism

You are reading the map of a live options-trading system that spans four sibling repos
under `/Users/suman/code/`. Research proves ideas; a shared contract carries them; a
runtime executes them on real money on the Public.com API. This file tells a fresh agent
what each repo does and how a strategy travels from a backtest to a live order.
Trust order when facts conflict: **runtime evidence > diary > this summary.**

## The four repos and their roles

- **mala_v2** (this repo) — research + backtest engine and the **head repo** of the
  organism. Proves or kills ideas through **M1–M7 evidence gates** (M1–M5 = walk-forward
  edge, cost convergence, regime stability, holdout, Monte-Carlo stress; M6 = option-aware
  exit evidence; M7 = provider/feature-risk validation — `docs/MALA_VISION_v2.2.md`,
  `src/research/playbook_automation_gates.py`, `provider_validation_m6.py`,
  `provider_m7_workbook.py`). Its evidence surface is the Google Sheet **`Mala_Evidence_v1`**,
  published by `src/research/mala_handoff.py`: tested params, signal window, recommendation
  tier, thesis-exit evidence, and Bhiksha capability labels. Read-only, Mala-owned. No live
  execution. Also home to the live-loop diary (`docs/LIVE_LOOP_WORKPLAN.md`) and this brain.
- **mala-bhiksha-kernel** — the **contract** between research and execution. Defines the
  packet schemas (`EvidencePacket`, `PlaybookPacket`, `ExecutionPacket`), `CapabilityManifest`,
  and `ManagementPolicySpec` — the executable exit contract — in
  `kernel:src/mala_bhiksha_kernel/contracts/packets.py` + `capabilities.py`. mala_v2 imports it
  via `src/research/shared_kernel.py` (a sys.path bridge). This is the only vocabulary both
  sides agree on; changing an exit's shape means changing the kernel first.
- **bhiksha** — the **live execution runtime** on the Public.com API. Consumes the compiled
  plan, runs signals on live bars, selects option contracts, manages entry/exit + broker
  reconciliation. Every deployment is shadow (paper) or live, gated by operator arming.
  Deployed on **oldmac** (see runtime section).
- **public_api_trading_v3** — the operator's **separate manual bot** (operator-driven entry,
  profile-driven exits). NOT part of the automated loop. It is the **source of the exit-profile
  DNA**: Suman modeled ~7 years of naked-option trading as four named exit profiles in
  `v3:src/domain/trading/exit_policies/profiles.py`. Those dials were ported into the kernel's
  `ManagementPolicySpec` and are now executed live by bhiksha (code wins over the 26-day-old
  `exit-profile-gap` memory, which predates the port).

## How a strategy travels (research → live)

1. **Research** in mala_v2 backtests a playbook × symbol and runs it through M1–M7.
2. **Evidence gates** decide `bhiksha_ready`, `mala_evidence_ready`, `activation_candidate`,
   `option_trade_ready`, `m7_status`, `triage_verdict`.
3. **Sheet catalog** — survivors publish to `Mala_Evidence_v1` (the "strategy catalog" tab)
   and the operator marks intent on the `active_strategy` rows (a symbol, an
   `authorization_mode` of `shadow` or `live`, overrides).
4. **Compile** — bhiksha's active-plan compiler fuses the catalog + operator rows into
   `active_plan.json` (next section).
5. **Lanes** — each compiled deployment runs as a **live** lane (real orders) or a **shadow**
   lane (paper fills on the same live feed). Shadow is the instrument that *gathers* activation
   evidence, so it is deliberately allowed to run on sub-activation evidence; live is not.

## The compile pipeline

`bhiksha:src/bhiksha/active_plan/compiler.py` turns two inputs — the `Mala_Evidence_v1` strategy
catalog rows (`StrategyCatalogSheetRow`) and the operator's `active_strategy` control rows
(`ActivePlanSheetRow`) — into a validated `active_plan.json` (`ActivePlan` of `DeploymentManifest`s).
`compile_active_plan_from_google_sheets` reads the tabs; `compile_active_plan_from_rows` is the core.
Load-bearing behaviors:

- **Capability gating** — `evaluate_strategy_capability` / `derive_strategy_variant`
  (`bhiksha:src/bhiksha/strategy/capabilities.py`) label whether the runtime can actually run the
  variant; unsupported variants raise at compile.
- **Evidence-vs-safety gate partition** — `_validate_google_catalog_alignment` (compiler.py:1785)
  splits checks into two classes. **Safety/integrity gates** (runtime capability, `bhiksha_ready`,
  `m7_status=block`, `triage_verdict=KILL`, retired lifecycle, symbol/strategy_key mismatch)
  **always raise, in any mode**. **Evidence-quality gates** (`mala_evidence_ready`,
  `activation_candidate`, `option_trade_ready`) raise **only for live rows**; for shadow rows they
  are recorded as `evidence_gates_relaxed` metadata and the lane runs, visibly labeled. A live row
  may never carry a relaxed evidence gate (compiler.py:817).
- **gate_override honor + surface** — a Sheet row can set profile-exit dispatch-gate inputs via its
  `execution`/`exit` override channels. The compiler **honors** them (the Sheet is the operator's
  sanctioned arming surface — it already controls `mode=live`) but **surfaces every occurrence**
  into `plan.summary["gate_override_key_warnings"]` so arming is always visible in the audit trail
  (`_detect_gate_override_keys`, compiler.py:373).
- **shadow_only derivation** — a deployment compiles `execution.shadow_only = (authorization_mode
  != "live")`; only an explicit `live` row produces a live lane. Rail B demotions
  (`apply_risk_demotion_overrides`, compiler.py:703) can additionally force any row to `shadow_only`
  at compile, one-way, from the local `DemotionStore`.

## The runtime organism (on oldmac)

Production runs on **oldmac** from the runtime checkout **`/Users/sunny/Documents/bhiksha`** (NOT
`~/code`; user `sunny`), with SQLite authority `bhiksha.db` there (`bhiksha:README.md`,
`docs/chart_review_manual.md`). launchd is the clock; bhiksha owns its jobs.

- **08:20 CT live-start** restarts the runtime from the current `active_plan.json`
  (`BhikshaRuntime.run_session`, `runtime.py`).
- **The supervisor loop** (`bhiksha:src/bhiksha/execution/supervisor.py`, driven by `runtime.py`):
  on each bar it evaluates **signals** (every decision persisted via `record_signal_evaluation`),
  hands a fired signal to the **planner/selector** (`ExecutionPlanner` + `options.selectors`, which
  raises `SelectorEmptyError` when no contract qualifies), places **orders** through
  `order_manager`, and dispatches **profile-exit FSM** decisions
  (`ProfileExitState` → `profile_exit.py`) — but only **behind the fail-closed allowlist**
  `profile_exit_dispatch_allowed` (profile_exit.py:730). That allowlist dispatches a live exit
  ONLY when all hold: `live is True`, `runtime_mode == "live_approval_gated"` (the lone allowed
  mode — `live_automated` is forbidden), deployment not `shadow_only`, and `position_source ∈
  {live_open, live_pending}`. Any `None`/unknown/recovered value fails closed to shadow.
- **Reconciliation sweeps (~15s)** — a dedicated loop calls `reconcile_public_positions`
  (`bhiksha:src/bhiksha/state/reconciliation.py`) every `reconciliation_interval_seconds = 15`
  (`config/models.py:179`), **rewriting the tracker's positions from the broker portfolio**. This
  is load-bearing, not cosmetic: exit ladders derive quantity from the reconciled position, so the
  broker is the source of truth for what is actually held (see workplan #21).
- **Risk manager rails A/B** (`bhiksha:src/bhiksha/risk/risk_manager.py`) — mechanical, not
  vigilance-based. **Rail A** is a two-tier portfolio daily-drawdown cap on realized live P&L:
  **tier 1 halt at 7.5%** (block new entries this session), **tier 2 flatten at 11.25%** (also
  flatten open live positions) — revised up from 5.0/7.5 on 2026-07-08 "till the account grows".
  Consulted once per bar-minute (book-level), fail-closed for new entries on unknown budget,
  fail-safe (no spurious flatten) on missing data. **Rail B** auto-demotes a deployment to shadow,
  one-way, when its rolling last-N closed live trades average below the demote threshold.
- **The 7 launchd jobs** (`bhiksha:src/bhiksha/ops/launchd_registry.py`, all CT, weekday-gated
  unless noted):
  `com.bhiksha.live-start` 08:20 · `com.bhiksha.live-watchdog` every 10 min 08:30–15:00 ·
  `com.bhiksha.live-stop` 15:10 · `com.bhiksha.schwab-guard` 07:10 (token guard) ·
  `com.bhiksha.session-report` 09:10/11:45/14:45 · `com.bhiksha.weekly-scorecard` Fri 15:20
  (profile-vs-legacy verdict) · `com.bhiksha.shadow-ev-report` 15:30 (shadow promotion report).

## Deploy topology

Development happens on **this Mac Air**; oldmac carries checkouts of both mala_v2
(`~/Documents/mala_v2`) and bhiksha (`~/Documents/bhiksha`). Deploy = **git push here → pull on
oldmac** (diary cadence: `oldmac == origin == local` at a named commit). Deploy gates observed in
the diary: **green tests**, an **adversarial audit for any money-path change** (a green suite is not
proof — 3-round audits repeatedly caught bugs a passing test blessed), and a **session boundary**
(deploy evenings / market closed, one increment per day). Reports and lessons project to Obsidian
via the Lathi bus for phone review.

## Profile exits

The live exit ladder is the operator's **TREND_CONTINUATION** profile
(`v3:src/domain/trading/exit_policies/profiles.py`; the live book is currently a
TREND_CONTINUATION monoculture per the 2026-07-09 diary): **35% premium disaster stop** (backstop
when no live broker stop rests), **bank 60% of the position at T1 = 1R**, then
**move the stop to breakeven**, **let the runner ride to 2R**, with a **no-progress time stop at
2700s (45 min)**, a **MODERATE high-water-giveback**, and **eod_flat** (intraday thesis never sleeps
overnight). bhiksha executes these via the kernel `ManagementPolicySpec` mapped onto its `ExitSpec`
(compiler `exit_profile_spec` bridge) and the `profile_exit.py` FSM. Sibling profiles
FLASH_REVERSAL, EXHAUSTION_REVERSAL, and RANGE_EXPANSION (the one profile allowed to hold overnight)
carry their own dials.

**Exit-authority rule** — when a deployment's profile exit is armed live, the profile owns ALL exit
decisions; the runtime suppresses the mechanical full-size profit-target so the two exit engines
never fight (`profit_target_suppressed: profile_owns_profit_taking`). The full statement and its
verification history live in `docs/brain/DECISIONS.md` (ADR ledger) — reference it there; it is not
duplicated here.
