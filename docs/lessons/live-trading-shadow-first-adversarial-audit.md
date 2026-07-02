---
title: Shipping to live-trading code — shadow-first dual-run + an independent adversarial re-audit loop catches footguns the implementer's own tests miss
type: pattern
area: bhiksha execution (live exit path) / cross-repo adoption
date: 2026-06-14
tags: [live-trading, safety, adversarial-audit, shadow-mode, supervisor-lane]
refs: [bhiksha/src/bhiksha/execution/profile_exit.py, bhiksha/src/bhiksha/execution/profile_exit_shadow.py, bhiksha/src/bhiksha/execution/supervisor.py, "bhiksha 15fadad..f8e2527"]
---

# Shadow-first + adversarial re-audit for live-trading code

## Context
Adopting the operator exit profiles into bhiksha's LIVE execution path (the highest-stakes
code in the system). Built across a supervised lane: kernel contract → bhiksha evaluator →
mala emission → live-monitor wiring. The operator flips live himself.

## What we learned
Two reusable disciplines, both load-bearing:

**1. Shadow-first = provable zero-change, not "we'll be careful."** Make a new live-path
capability safe BY CONSTRUCTION: (a) a default-OFF operator flag is the single switch;
(b) the new logic runs as a RECORD-ONLY dual-run — it computes its decision and appends a
shadow event, but the existing path stays the sole authority; (c) the evaluator is
import-isolated from the live loop (nothing in the monitor imports it). Then a single
invariant test proves byte-for-byte identical broker calls with the flag off. The operator's
live flip is one line + wiring the dormant route — nothing hidden.

**2. The fix → INDEPENDENT adversarial re-audit loop is mandatory for live code.** The
implementing worker's own tests passed at every stage (35, then 411, then 422...), yet each
adversarial re-audit (a fresh agent told to DISPROVE the safety claim, read-only) found REAL
live-path footguns the author's tests missed — and the *fixes themselves introduced new ones*.
Re-audit until an audit comes back clean OR only known-tradeoffs/latent-behind-the-gate
remain. Cap the loop, but don't skip it.

## Why / when it applies
Any change that could, when wired, place/cancel/resize a real order or move money. The cost
of a footgun is asymmetric (a "partial" that flattens a position is a real loss), so the
verification must be adversarial and independent of the implementer.

## Specifics — the footgun catalog these re-audits caught (recognize on sight)
- **Partial that flattens everything:** a PARTIAL_SCALE mapped to `square_off`, but the
  supervisor sized the close with full `position.quantity` and ignored `exit_quantity`. Fix:
  honor the partial qty, keep the residual open, and a guard that RAISES rather than flatten.
- **Gate fail-OPEN denylist:** the dispatch gate was a denylist on a multi-valued field —
  `runtime_mode=None`/unknown/typo and broker-recovered position sources all passed. Fix:
  strict allowlist, fail-closed on every dimension (`profile_exit.py:profile_exit_dispatch_allowed`).
- **`live_automated` admitted:** the gate allowed the no-approval auto mode every other gate forbids.
- **Naked residual / double stop:** a partial cancelled protection but only re-armed if a prior
  stop existed (naked residual); or re-armed without checking a live stop (double stop).
- **Cancel-OK/place-fail window:** cancel succeeds, replacement stop fails → naked until next
  tick. Mitigate: retry once, then mark for the monitor's missing-protection re-arm.
- **Phantom `stop_price`:** persisting a stop price while `stop_order_id is None` fools any
  `stop_price is not None` "is-protected" check.
- **State not persisted / leaked:** per-trade ladder state (`ProfileExitState`) needs a lifecycle
  owner + clear-on-EVERY-terminal-close (incl. reconciliation paths), else re-banked partials.
- **Hardcoded gate input (flip-seam):** `runtime_mode="live_approval_gated"` passed as a literal
  rather than the deployment's real mode — harmless flag-off, a footgun the moment you flip.
- **Shadow-advanced state on a mid-position flip:** shadow mutates ladder state every tick;
  flipping live mid-position hands the evaluator state that thinks a partial was banked.

## Apply it next time
Building anything that touches the live exit/order path: (1) ship it OFF behind a flag, as a
record-only dual-run, import-isolated, with a byte-for-byte invariant test; (2) before you
believe "live-ready," spawn a fresh agent whose ONLY job is to disprove the safety claim and
trace every path to the broker; (3) repeat after each fix. "Tests pass" ≠ "safe to flip."

## Dead ends
- Trusting the implementing worker's green suite as live-readiness evidence — it missed every
  footgun above.
- "It's deferred / nothing calls it yet" as a substitute for a structural guard — fine for
  shadow, not for the eventual flip; the flip is exactly when the deferral bites.

## Addendum (2026-07-02, exits-live-scale-riskmgr lane)
The pattern held and sharpened when the profile exits actually went live. Three
audit rounds on the bhiksha money path each caught a different bug class —
including a fix-introduced regression whose unit test encoded the bug as
expected behavior. Full write-ups now live in the bhiksha repo:
`bhiksha/docs/lessons/money-path-audit-rounds-catch-different-bug-classes.md`,
`armed-config-needs-gate-input-telemetry.md`,
`sheet-is-the-operator-control-surface.md`. Key addition to this note's
pattern: after fixing audit findings, send the DELTA back to the SAME auditor
to re-run its own repros — round 2 found the worst bug both times.
