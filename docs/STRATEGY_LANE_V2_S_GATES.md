# Strategy Lane v2: S-Gates

Status: active direction (updated 2026-06-13). Exit-profile spine + two-wave
build plan — see "Current Direction" below. Working surface:
`docs/Strategy_Lane_V2_S_Gates_Planning.xlsx`; canonical record:
`docs/EXIT_PROFILE_PLAYBOOKS.md`.

## North Star

Strategy Lane v2 turns a discretionary or playbook-style thesis into an
automated, evidence-backed strategy candidate without pretending that an
underlying-only backtest is the same thing as an options trading system.

The goal is:

```text
thesis / playbook language
-> locked strategy spec
-> bounded parameter surface
-> options-aware scoring
-> out-of-sample and coarse-regime validation
-> timing robustness
-> option translation and exit economics
-> provider / broker parity
-> Mala_Evidence_v1 only after the proof contract is satisfied
```

This is not a new live-trading permission layer. Live trading remains behind
Bhiksha capability checks, active operator authorization, and explicit runtime
guardrails.

## Current Direction (2026-06-13)

After working the planning workbook with the operator, the lane has a sharper
shape than the original draft below:

- **Exit profiles are the spine.** The operator trades 4 playbooks
  (Flash Reversal, Exhaustion Reversal, Trend Continuation, Range Expansion),
  each mapped 1:1 to a named, operator-calibrated option exit profile (live in
  `public_api_trading_v3`). The S-gates exist to prove and tune those
  playbook+profile pairs. For options, **exits are the differentiator, not
  entries** (4 years / 17k personal round-trips: median 2 DTE, ~23-min holds,
  asymmetric-payoff edge).
- **Two waves.**
  - **Wave 1 — exits first, on the strategies already live in Bhiksha.** Harden
    `src/research/exit_optimizer.py` with the profile exit families (high-water
    giveback, no-progress stop, R-multiple partials), group the existing M-gate
    strategies into the 4 profiles, re-run exit-only optimization
    (`scripts/reoptimize_exits.py`), republish `Mala_Evidence_v1`, and teach
    Bhiksha to consume the richer profile. Low-risk; proves the exit/execution
    rails on entries that are already validated.
  - **Wave 2 — entry discovery for the playbooks through S0-S5 (below).** The
    hard, open-ended part. A negative result is a fork, not a wall:
    no-edge / wrong-metric / wrong-yardstick. The deliverable is a terrain map
    of where each play's edge lives (symbol × stretch metric × regime).
- **Scoring.** Reward payoff asymmetry / option convexity — never per-trade
  hit-rate or a flat underlying cost haircut (the M1 trap that mis-killed the
  intraday reversion lane).
- **Option pricing.** S1 ranks with a cheap delta-theta guard; S4 prices with
  Black-Scholes driven by a modeled IV run as a band (flat / mean-revert /
  crush), calibrated and validated against the short real option-chain window
  rather than driven by it.
- **Contract & safety.** The exit profile extends the kernel
  `ManagementPolicySpec` (Tier-1 = fields Bhiksha already runs; Tier-2 = new
  rules, capability-gated and shadow-first). Vehicle (DTE/delta) and sizing
  belong to separate specs. `mala_v2` tunes profiles offline; Bhiksha runs a
  frozen named profile. This lane sits alongside the M1-M7 lane, not over it.

The gate specifications below remain the detailed reference for the Wave 2
entry-discovery proof system. Full detail: `docs/EXIT_PROFILE_PLAYBOOKS.md`.

## Why This Exists

The current M-gates are useful for underlying strategy research, but the
intraday mean-reversion playbook showed a mismatch:

- playbooks often begin as conditional surface maps and chart-review leads;
- current M1 asks for rolling, after-cost, underlying OOS evidence immediately;
- a promising playbook pocket can be too sparse for M1 before the intended
  options thesis, exit policy, or chart-quality filters are even encoded;
- a flat underlying cost haircut can kill or distort a strategy whose economic
  reality is option spread, delta, timing, liquidity, and exit behavior.

S-gates are the proposed proof system for playbook-native, options-aware
strategy lanes.

## Doctrine

- Playbooks are thesis and design language.
- Strategies are deterministic executable contracts.
- S-gates prove whether a thesis deserves promotion, not whether it was
  interesting in a chart review.
- Parameters must be predeclared before optimization.
- Exit policies are part of the thesis, not an afterthought.
- Options economics must enter before publication readiness.
- Regime should first be a readout, not a fragmentation engine.
- Provider and broker parity are operational gates, not alpha-discovery gates.

## Gate Map

| Gate | Name | Question Answered | Default Human Role |
| --- | --- | --- | --- |
| S0 | Strategy Spec Lock | What exactly are we allowed to test? | Required audit |
| S1 | Design Surface | Which predeclared parameter/exit combinations look viable? | Review metric and search surface |
| S2 | OOS + Regime Readout | Does the candidate survive outside the design sample and across coarse regimes? | Review ambiguous splits |
| S3 | Timing Robustness | Does the idea survive realistic one-minute trigger/fill uncertainty? | Usually no audit |
| S4 | Option Translation + Exit Economics | Does the options version work with realistic vehicle, spread, and exit assumptions? | Required audit before publication |
| S5 | Provider / Broker Parity | Does the backtest signal translate to broker-observable inputs? | Required before shadow/runtime adoption |

## S0: Strategy Spec Lock

Purpose: freeze the thesis before optimization.

Inputs:

- thesis name and source playbook;
- symbols;
- direction scope;
- time window;
- setup definition;
- allowed parameters;
- forbidden degrees of freedom;
- required filters or disqualifiers;
- option intent;
- candidate exit policies;
- target sample scope;
- expected publication path.

Exit criteria:

- the strategy can be described deterministically;
- hidden chart judgment is either encoded, deferred as risk, or rejected;
- the search surface is bounded enough to test honestly.

## S1: Design Surface

Purpose: evaluate the locked parameter and exit surface.

S1 should optimize a thesis-appropriate metric, not blindly maximize generic
underlying expectancy. For options-intended strategies, S1 may still use
underlying paths as setup evidence, but the score must preserve the eventual
option question.

Candidate S1 metrics:

- gross underlying R for setup quality;
- after-delay underlying R;
- hit rate at thesis exit;
- adverse excursion before thesis confirmation;
- option-proxy expected value using delta/DTE/spread assumptions;
- capital-at-risk adjusted payoff;
- stability-adjusted score penalizing thin samples and too many knobs.

Exit criteria:

- selected candidate rows are explainable;
- winners are not just artifacts of one exit or one tiny pocket;
- weak rows are preserved as evidence, not silently discarded.

## S2: OOS + Regime Readout

Purpose: validate candidate behavior outside the design surface.

Default structure:

- train/design sample selected in S1;
- out-of-sample period kept separate;
- coarse regime tags reported for every row;
- regime is initially observational unless sample size supports a gated split.

Initial coarse regime tags:

- bullish: price above 200-day moving average;
- bearish: price below 200-day moving average;
- neutral: near the 200-day average or unresolved;
- optional volatility tag can be added later if it clearly helps.

Open question:

- whether every strategy lane should share one calendar split, one rolling
  split, or a thesis-specific split declared in S0.

Exit criteria:

- candidate remains viable OOS;
- regime behavior is known, even if not gated;
- any regime-specific promotion is justified by enough samples.

## S3: Timing Robustness

Purpose: test whether one-minute backtest mechanics are too optimistic.

Perturbations:

- trigger occurs inside the bar but entry waits until bar close;
- entry slips by one or more bars;
- entry price worsens;
- stop/exit price worsens;
- missed-fill probability;
- delayed option fill or widened spread.

Exit criteria:

- candidate survives a thin, realistic perturbation layer;
- fragility is named before option publication.

## S4: Option Translation + Exit Economics

Purpose: prove the actual economic object we intend to trade.

Inputs:

- DTE range;
- delta or strike selection;
- single-leg or spread intent;
- liquidity/spread assumptions;
- stop and thesis-exit behavior;
- option entry/exit timing;
- capital and risk model.

Exit criteria:

- option expected value and drawdown are acceptable;
- selected exit policy is explicit;
- Bhiksha support requirements are known;
- candidate can be represented in Mala evidence without hidden assumptions.

## S5: Provider / Broker Parity

Purpose: prove the backtest can be observed by the broker/runtime path.

Checks:

- signal overlap between research provider and broker-observable provider;
- feature parity for required inputs;
- timestamp/session alignment;
- option-chain availability;
- supported runtime fields;
- fail-closed behavior when provider parity is missing.

Exit criteria:

- S5 artifacts prove or reject shadow readiness;
- no active strategy or live authorization changes occur automatically.

## Workbook-First Operating Mode

The first planning artifact for S-gates is an Excel workbook. It should let
Suman inspect and challenge the system in plain language before code hardens.

The workbook should contain at least:

- S0-S5 process map;
- thesis register;
- allowed parameter and degree-of-freedom register;
- metric design register;
- open questions and decisions.

The workbook is not the executable source of truth forever. It is the design
and audit surface until the framework stabilizes, after which the accepted
contract should become code, tests, and versioned configuration.

## First Implementation Target

Wave 1 (exit-first) precedes any S-gate entry discovery — see "Current
Direction" above. It hardens `exit_optimizer.py` with the profile exit families
and re-optimizes exits for strategies already validated by the M-gates, with no
entry rediscovery (`scripts/reoptimize_exits.py`).

The first S-gate (Wave 2) implementation should then answer, on a real,
operator-owned play (not the killed intraday mean-reversion experiment):

- can S0 lock a playbook thesis **and its exit profile** cleanly?
- can S1 evaluate the stretch/exit surface with the delta-theta guard without
  pretending options are underlying shares?
- can S2 report OOS and coarse-regime behavior without over-fragmenting?
- what additional code is required before S3-S5 (and the S4 option band) are
  honest?

## Non-Goals

- Do not copy the repo into Mala v3 yet.
- Do not bypass Mala_Evidence_v1.
- Do not publish to Google Sheets automatically.
- Do not mutate `active_strategy`.
- Do not sync oldmac or change Bhiksha runtime state.
- Do not make regime, options, or provider parity invisible defaults.
- Do not let Bhiksha tune profiles at runtime — tuning is offline in `mala_v2`;
  Bhiksha runs a frozen named profile.
- Do not score per-trade hit-rate or a flat-cost underlying haircut as the
  options gate; score payoff asymmetry / option EV.

