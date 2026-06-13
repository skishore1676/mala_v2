# Exit Profiles, Playbooks & Build Plan

Status: active direction, aligned with the operator 2026-06-13.

This is the canonical, diffable record of the exit-profile lane. The working /
audit surface is the Excel workbook
`docs/Strategy_Lane_V2_S_Gates_Planning.xlsx`; the S-gate proof system is
specified in `docs/STRATEGY_LANE_V2_S_GATES.md`. This lane sits **alongside**
the existing M1-M7 strategy lane — it does not replace it.

---

## The bet

For options, **the exit is the differentiator, not the entry.** Evidence: ~4
years / 17,002 personal round-trips (`data/personal_imports/`) — fast,
short-dated (median 2 DTE, ~23-minute holds), with an **asymmetric-payoff**
edge: enter early, take many small defined losses, occasionally win big on
convexity. The automated loop must score and trade *that*, not per-trade
hit-rate.

## The gap

The loop (`mala_v2` research → `mala-bhiksha-kernel` contract → `bhiksha`
runtime) validates underlying entries with M1-M7 and hands Bhiksha a
**mechanical** exit (kernel `ManagementPolicySpec`: single `target_r`, flat
stop). The operator's real exit DNA lives only in the manual bot
`public_api_trading_v3` (`src/domain/trading/exit_policies/profiles.py`). This
lane makes those profiles first-class across the loop.

---

## The 4 playbooks × profiles (1:1)

Each play maps 1:1 to a named, operator-calibrated option exit profile.

| Play | Profile | Thesis | Entry (option-buyer) | Invalidation | Symbols |
|---|---|---|---|---|---|
| Flash Reversal | `FLASH_REVERSAL` | A violent flash (any symbol, any time) looks over-stretched | Clear reversal break, lean early; fade toward VWAP | Prior flush extreme re-breached; no bounce shortly after | IWM, SPY, QQQ + TSLA, NVDA, AMD |
| Exhaustion Reversal | `EXHAUSTION_REVERSAL` | A mature, unsupported extension may reverse after more pain | Clear break + more patience; still early-biased | Acceptance beyond the reference / prior extreme re-breached | IWM, SPY + TSLA, NVDA, AMD |
| Trend Continuation | `TREND_CONTINUATION` | An established trend resumes after a pullback | **Anticipate** — enter slightly *before* the reclaim | Pullback becomes a structure break / stage flip | TSLA, NVDA, AMD, META, GOOGL, AAPL, AMZN, QQQ |
| Range Expansion | `RANGE_EXPANSION` | A stabilized base starts a larger move | **Anticipate** — enter into the coil before the break | Close back into the base; no follow-through | TSLA, NVDA, AVGO, ARM, semis, SMH |

Entry model (operator, confirmed): as an option buyer with defined risk, he
enters **early and accepts frequent small losses** for better premium and
convex upside. Breakouts/trend → anticipate before the break; reversals →
wait for the clear break. Invalidation is always **re-breach of the prior
extreme** — then take the loss. Flash Reversal is **any symbol / any time** (it
is NOT the old intraday-open mean-reversion case study).

## Exit profile dials

Lifted from `public_api_trading_v3/.../profiles.py` (DRAFT — "tune from the
`exit_decisions.db` log"). `mala_v2` becomes that offline calibration bench.

| Profile | T1 R | T2 R | T1 size% | Init stop% | Disaster% | Max hold | No-progress | Giveback | Theta% | GDS | Runner | EOD flat | DTE (min–pref–max) | Delta | Max cap% |
|---|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
| FLASH_REVERSAL | 1.0 | 2.0 | 75 | 25 | 30 | 90 min | 15 min | STRICT | 10 | HIGH | No | Yes | 1–2–7 | 0.45–0.60 | 5 |
| EXHAUSTION_REVERSAL | 1.0 | 2.5 | 50 | 40 | 45 | 4 hr | 75 min | MODERATE | 8 | MEDIUM | Yes | Yes | 7–8–14 | 0.30–0.40 | 4 |
| TREND_CONTINUATION | 1.0 | 2.0 | 60 | 30 | 35 | 3 hr | 45 min | MODERATE | 15 | HIGH | Yes | Yes | 5–7–14 | 0.28–0.38 | 6 |
| RANGE_EXPANSION | 1.0 | 2.0 | 40 | 35 | 40 | 5 days | 2 hr | LOOSE | 25 | LOW | Yes | No | 14–16–21 | 0.25–0.35 | 5 |

---

## Build plan

### Wave 1 — exits first, on strategies already live in Bhiksha

Low-risk, fast, proves the exit + execution rails on entries that are already
validated by the M-gates.

1. **Add profile exit families** to `src/research/exit_optimizer.py`:
   high-water giveback, no-progress time stop, R-multiple two-target partials,
   plus the 4 named profiles.
2. **Group each live strategy into a profile** (first pass; confirm):
   - Elastic Band Reversion, Intraday Mean Reversion → Flash / Exhaustion Reversal (reversion)
   - Opening Drive Classifier, Jerk-Pivot Momentum → Trend Continuation (momentum)
   - Compression Expansion Breakout, Market Impulse (Cross & Reclaim) + descendants → Range Expansion / Trend Continuation (breakout/impulse; MI descendants need eyeballing — Shallow Spring is reversal-flavored, Push Through is breakout)
   - If a strategy won't fit → a 5th/6th profile (cap ~4-6).
3. **Re-optimize exits** on existing M5 candidates via
   `scripts/reoptimize_exits.py` — exit-only, no entry rediscovery (the script
   already exists for exactly this).
4. **Republish** `Mala_Evidence_v1` (reload the Google Sheet) via
   `src/research/mala_handoff.py`.
5. **Bhiksha consumes the profiles**: extend its `ExitSpec` + capability
   manifest, Tier-1 fields first.

### Wave 2 — entry discovery for the 4 playbooks (S0-S5)

The hard, open-ended part: discover **where** each play's edge lives (symbol ×
stretch metric × regime). Tackled after Wave 1 proves the rails. See
`docs/STRATEGY_LANE_V2_S_GATES.md`.

**Reading a negative result — a "no" is a fork, not a wall:**
- **No edge** — even the best metric/threshold/regime pays nothing → drop it, or try another symbol.
- **Wrong metric** — the visual "too stretched" isn't captured → add features (MarketPulse stage, jerk/exhaustion) until the machine's "stretched" matches the eye.
- **Wrong yardstick** — it pays in option convexity but we scored hit-rate / flat-cost expectancy → score payoff asymmetry / option EV (the M1 trap).

Deliverable is a **terrain map** of where the edge lives, not a yes/no.

---

## Scoring, option pricing, contract & safety

- **Scoring**: reward payoff asymmetry / option-convexity EV. Never per-trade
  hit-rate or a flat underlying cost haircut.
- **Option pricing**: S1 ranks with a cheap delta-theta guard; S4 prices with
  Black-Scholes driven by a **modeled IV run as a band** (flat / mean-revert /
  IV-crush), calibrated and validated against the short real option-chain
  window rather than driven by it. IV anchor: realized vol (scaled ATR/CC) +
  VIX for index names + a per-symbol IV-premium offset.
- **Contract**: the exit profile **extends** the kernel `ManagementPolicySpec`
  (Tier-1 = fields Bhiksha already runs; Tier-2 = new rules, capability-gated +
  shadow-first). Vehicle (DTE/delta) and sizing (max capital) belong to
  **separate** specs.
- **Safety**: `mala_v2` tunes profiles offline; **Bhiksha runs a frozen named
  profile** (it never tunes itself). No auto-publish to Sheets, no
  `active_strategy` mutation, no oldmac sync.
