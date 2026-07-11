---
title: What actually blocks a triage candidate from shadow/publish — IV is not a gate, provider parity is doable, exit profiles are already mapped, adversarial is a promote-gate
type: gotcha + operating-model
area: mala_v2 triage → bhiksha publish path
date: 2026-07-10
tags: [triage, publication, provider-parity, iv, exit-profiles, shadow, adversarial, complete-triage-program]
refs: [docs/COMPLETE_TRIAGE_PROGRAM.md, scripts/classify_explore_propose.py, src/research/provider_replay_m7.py, config/m7_provider_translation.yaml, "oldmac:bhiksha/config/providers.yaml"]
---

# What actually blocks a triage candidate from shadow — and what doesn't

## Context
During the Complete Triage Program (2026-07-10) I framed three things as "blockers" to publishing
candidates. Two were wrong or overweighted. This note fixes the mental model so future sessions
don't re-derive it or stall on non-blockers. Verified against bhiksha runtime on oldmac.

## The corrected model — what each gate really is

**1. IV modeling is a BACKTEST-only construct. It is NOT used anywhere in bhiksha runtime.**
- Verified: `grep -riE "implied_vol|black_scholes|option_translation|kamandal" bhiksha/` (excluding
  test/research) returns **nothing**. Bhiksha places real option orders and manages exits on
  **price / R / time** (`stop_anchor`, `target_r`, `hard_flat_time_et`, `option_stop_fallback_pct`).
- Therefore modeled IV only affects how much to trust the *backtest's* option-path expectancy when
  ranking candidates. It is **not required to shadow or to go live.** Once shadowing, real fills ARE
  real IV.
- Corollary: do NOT treat "real-IV validation" or the degenerate adverse-IV band as a publication
  blocker. The IV-band bugs (funnel never calls `score_profile_band`; `use_real_iv` collapses the
  cheap/rich scenarios) are real research-scorer defects worth fixing, but they argue **for**
  shadowing (get real data), not against it. Don't sink hours here before shadow.

**2. M6/M7 provider parity is DOABLE, not a wall.** It is per-symbol work, not a missing capability.
- Bhiksha runtime provider = **Schwab** (`oldmac:bhiksha/config/providers.yaml:underlying_live_primary: schwab`);
  mala research data = **Polygon**. M7 parity = Schwab-vs-Polygon signal overlap for the symbol.
- Path: fetch ~**1 month** of Schwab bars on oldmac for the chosen symbols → run
  `provider_replay_m7` → get `signal_overlap`. M7 is a translation-fidelity check, NOT a history
  check, so **a month is sufficient** (don't wait for years of data).
- "Nothing in-repo auto-fetches a provider panel" is true but irrelevant — the fetch is scriptable
  via the runtime provider on oldmac. This is the ONE real step between "shadow-able" and
  "activation candidate".

**3. Exit profiles are already mapped by the pipeline — don't re-derive.**
- `classify_explore_propose.py` classifies each candidate into a profile (TREND_CONTINUATION /
  EXHAUSTION_REVERSAL / RANGE_EXPANSION / FLASH_REVERSAL) and emits a full `management_policy_spec`
  (policy_id, stop_family, stop_anchor, exit_family, target_model, target_r, hard_flat_time_et,
  option_stop_fallback_pct) — the SAME native profile-exit construct the live lanes use and the field
  the bhiksha compiler consumes. If a candidate has `chosen=profile` + a spec, its exit is mapped.

**4. The adversarial disprove pass is a PROMOTE/live gate, not a SHADOW gate.**
- It actively tries to prove a gate-passing candidate is fake (edge = a few lucky trades /
  single-regime / data artifact). It earned its keep — it killed AMD/DHI/TSLA short (recent-regime
  artifacts that passed FDR). But shadow is low-stakes tracking where real fills become the adversary.
- So: run it before promotion/live; it is NOT required to publish a shadow row.

## The thinning model (why "more shadow candidates" is fine)
A candidate already survives: multi-regime + direction-consistency + yardstick (win/payoff/mc_prob) +
profile-exit option-path>0. Then **shadow itself is the next thinning stage** (real fills, real IV,
real provider) before any promotion. So being inclusive of honest gate-passers at the shadow stage is
correct — there is ample thinning downstream. Do not over-prune the shadow shortlist; be true to the
gates and let shadow + the promote-time adversarial pass do the rest.

## The single human stop point
The whole triage runs autonomously to completion. The ONLY mandatory operator stop is the **real
publish** (writing `Mala_Evidence_v1` / adding shadow rows to `active_strategy`) — the operator sits,
reviews the finished shortlist + parity, and publishes. Everything upstream (waves, acceptance,
Tier-B, M7 parity via Schwab fetch, artifacts) is driver-owned and reversible.
