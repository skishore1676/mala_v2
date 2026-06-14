---
title: Option-exit profiles can't be judged on the underlying — score them on the option path with direction-aware modeled IV
type: how-it-works
area: research/exit (S4 option translation)
date: 2026-06-14
tags: [options, exits, profiles, black-scholes, iv, S4]
refs: [src/research/option_translation.py, src/research/exit_profiles.py, src/oracle/trade_simulator.py:ProfileExitPolicy, src/research/kamandal_iv.py, docs/EXIT_PROFILE_PLAYBOOKS.md, ecf13fc, ef024aa]
---

# Option-exit profiles need option-path scoring

## Context
The operator trades 4 playbooks, each 1:1 with a named option-exit profile (R-multiple
partials, high-water giveback, no-progress/max-hold time stops). We tried to validate
those profiles by adding them to the underlying exit optimizer (`exit_optimizer.py`) and
ranking by underlying expectancy. They lost — badly — to simple "hold to EOD"/time stops.

## What we learned
**The underlying exit optimizer structurally cannot value an option-exit profile.** The
profiles bank their edge from *option convexity* (take 75% at 1R on a premium that moved
~10×, ride a cheap runner, give back little). On the *linear underlying*, a partial +
giveback just cuts the move short, so the optimizer always prefers "hold." The profiles
only show their value when scored on the **reconstructed option-premium path**.

Two corollaries that took an adversarial round to get right:
1. **IV must be direction-aware.** A direction-agnostic "crush" model (IV deflates on any
   favorable move) is WRONG for puts-in-selloffs, where IV actually *rises*. Use the equity
   spot-vol leverage effect: `sigma_t = entry_iv * (1 + vol_beta * (-return_since_entry))`,
   clamped. Keyed off the signed return, puts-in-selloffs correctly get IV expansion and
   calls-in-melt-ups get IV bleed. (option_translation.py `_iv`.)
2. **Real IV calibrates, it doesn't drive.** Public's chain history is too short to drive a
   multi-year backtest, but kamandal captures real ATM IV (`kamandal_v2.db:iv_snapshots`,
   metric `atm_30_45_mean_iv`). The empirical `iv_premium_factor = real_IV/realized_vol` came
   to ~1.07, validating the modeled 1.2 default (slightly conservative). See `kamandal_iv.py`.

## Why / when it applies
Any time you're evaluating an *option* trading rule with an *underlying* backtest. The
linear underlying is the wrong yardstick whenever the edge is convex (premium leverage,
theta, vega). It also re-explains why the M1 gate mis-killed the intraday-reversion lane:
it scored per-trade after-cost underlying expectancy, blind to option convexity.

## Specifics
- S4 scorer: `option_translation.py:score_profile_on_options` reconstructs the premium path
  via Black-Scholes (`src/oracle/black_scholes.py`) + modeled IV, then runs the SAME
  `ProfileExitPolicy` on the premium (entry = entry premium). Run as a band over the two real
  unknowns (entry-IV richness × vol_beta) so the spread names the fragility.
- Result on the holdout: profiles that LOST on the underlying were robustly POSITIVE on the
  option path (PLTR/TSLA/AMD/NVDA etc., +1.6% to +10.5%/trade), with win rates ~.46–.58 and
  payoff ratios >1 — i.e. asymmetric-payoff edge, not hit-rate. Reversion (EXHAUSTION) works
  on IWM (index) but not META (single name).
- The underlying optimizer's profile candidates are gated OFF by default
  (`MALA_EXIT_PROFILE_CANDIDATES`, exit_optimizer.py) precisely because the underlying is the
  wrong judge.

## Apply it next time
Tell: you're ranking an options strategy/exit and "hold longer" keeps winning, or hit-rate
looks mediocre. Don't trust the underlying number — score it on the option path
(`score_profile_on_options`), keep the IV model direction-aware, and calibrate the entry-IV
factor against kamandal's `iv_snapshots`. See [live-trading-shadow-first-adversarial-audit].

## Dead ends
- Flat-IV Black-Scholes: misprices the vega/IV dynamics that ARE the edge — gives false confidence.
- Direction-agnostic crush: spuriously turned short/put profiles negative (−20%); it was a
  model artifact, not a real result.
