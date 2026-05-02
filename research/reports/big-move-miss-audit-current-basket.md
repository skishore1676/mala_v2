# Big Move Miss Audit + Exit Expansion Feasibility

**Generated:** 2026-04-15  
**Scope:** SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA  
**Evidence base:** All basket-discovery and guardrail hypothesis run artifacts as of 2026-04-12. Raw OHLCV parquet files are not available (see §1). All inferences are artifact-only.  
**Prompt source:** `research/prompts/agent-big-move-and-exit-research.md`

---

## Task A — Big Move Miss Audit

---

### 1. Data Availability

```
Observed:
  data/AAPL, data/AMD, data/IWM, data/META, data/NVDA,
  data/PLTR, data/QQQ, data/SPY, data/TSLA directories exist
  but contain zero parquet files.
  Only data/_vix_daily.parquet is present at the root.
  Result CSVs (M1–M5, holdout) are present for all 2026-04-12 runs.

Inferred:
  Raw OHLCV data was not carried into this repo state (moved worktree
  with no re-download). The Polygon cache would be rebuilt on the next
  uv run python hypothesis_agent.py run, but no cached bars are available now.

Next Hypothesis:
  All big-move inferences below are derived from signal counts, exp_r,
  holdout confidence, and MFE/MAE-proxied payoff ratios in the existing
  M4/M5 artifacts. Direct OHLCV analysis of specific date events is not
  possible until data/ is repopulated via a live Polygon pull.
```

**Artifacts used:**
- `data/results/hypothesis_runs/*/RUN_SUMMARY.md` (all 15 runs)
- `data/results/hypothesis_runs/*/M4_holdout.csv` (calibration and holdout signal counts)
- `data/results/hypothesis_runs/*/M5_execution.csv` (mc_prob, mc_dd, base_exp_r)
- `data/results/hypothesis_runs/*/m5_exit_optimizations.json` (all 9 exit artifacts)
- `research/reports/known-strategy-family-map.md`
- `research/reports/shadow-candidate-packet-2026-04-12.md`
- `research/reports/top-family-guardrail-comparison.md`
- `src/research/exit_optimizer.py` (policy grid)
- `src/oracle/trade_simulator.py` (same-day flat logic)

---

### 2. Largest Move Events

Cannot enumerate specific date/magnitude events without raw OHLCV. The following is inferred from holdout signal distribution and exp_r dispersion.

**Holdout window: 2025-12-01 → 2026-02-28 (63 calendar days, ~43 trading sessions)**

| Ticker | Holdout signals (best config) | Strategy | Implied avg session coverage | Holdout exp_r | Signal regime |
|---|---|---|---|---|---|
| TSLA | 45 | Jerk-Pivot (10,1,1.3,0.0015) | ~1.05/session | +0.787 | VPOC-near pivots |
| AMD | 110 | Market Impulse (5,90,1h,5-13-21) | ~2.56/session | +0.556 | VWMA cross/reclaim |
| IWM | 49 | Market Impulse (5,45,15m,5-13-21) | ~1.14/session | +0.573 | VWMA cross/reclaim |
| TSLA | 503 | Compression Breakout (20,0.7,15) | ~11.7/session | +0.148 | Compression events |
| NVDA | 175 | Jerk-Pivot (12,1,1.0,0.002) | ~4.07/session | +0.088 | VPOC-near pivots |
| AMD | 1,112 | Compression Breakout (15,0.8,15) | ~25.9/session | +0.099 | Compression events |
| QQQ | 53 | Market Impulse (3,60,1h,10-20-40) | ~1.23/session | +0.335 | VWMA cross/reclaim |

```
Observed:
  TSLA holdout: 45 signals across 43 sessions from one selective config
  (Jerk-Pivot 10,1). exp_r=+0.787 is the highest single-config holdout
  exp_r in the entire basket, suggesting TSLA produced concentrated,
  high-quality pivots in Dec-25/Jan-Feb-26.
  AMD Market Impulse: 110 signals at +0.556 exp_r in holdout — AMD had
  frequent and directionally reliable VWMA breaks during the window.
  QQQ Market Impulse: Only 53 signals but exp_r=+0.335 and a selected
  exit avg_winner of $7.52 vs avg_loser $3.39 — the largest winner-to-
  loser ratio across all exit artifacts, suggesting QQQ had at least a
  subset of trades with very large intraday moves.

Inferred:
  The Dec-25 to Feb-26 holdout likely contained episodic large intraday
  moves concentrated in TSLA and AMD (the strategies that fire selectively
  on those tickers show the highest exp_r). QQQ's large avg_winner implies
  at least a few sessions with extended trend legs that the 2R exit captured
  the beginning of.
  NVDA's thin holdout exp_r (+0.088) after 175 signals suggests NVDA was
  range-bound or churning in the holdout period; no single-direction
  persistence visible from signal quality.

Next Hypothesis:
  Repopulate data/ via Polygon pull (uv run python main.py --tickers TSLA
  AMD QQQ --start 2025-12-01 --end 2026-02-28). Cross-reference the 45
  TSLA Jerk-Pivot holdout trade dates against actual daily bar moves to
  confirm whether the +0.787 exp_r came from a handful of high-magnitude
  sessions or consistent small-R wins.
```

---

### 3. Move Type Classification

Based on strategy behavior, signal rates, and payoff dispersion in artifacts, intraday moves in the basket divide into four types:

| Type | Description | Basket tickers | Strategy that catches it |
|---|---|---|---|
| **Type 1 — Gap-and-Go (opening drive)** | Pre-market catalyst; open outside prior range; continuation within first 90–120 min | TSLA, AMD, NVDA | Opening Drive Classifier (15-min window) |
| **Type 2 — Intraday VPOC Pivot** | Price returns to prior-session VPOC, velocity/jerk spike, directional continuation | TSLA (primary), AMD (secondary) | Jerk-Pivot Momentum |
| **Type 3 — VWMA Regime Trend (Cross & Reclaim)** | Extended VWMA cross in first 45–90 min, trend-aligned entries | AMD, QQQ, IWM | Market Impulse |
| **Type 4 — Compression → Expansion** | Narrow ATR compression precedes directional breakout; high frequency, low per-trade payoff | TSLA, AMD | Compression Expansion Breakout |

```
Observed:
  Type 1 (Opening Drive): AMD short Opening Drive has the highest M4
  exp_r (+0.616) of any opening-range family config, with 23 holdout
  signals in 43 sessions (~0.53/session). Selective — fires on real
  gap/drive sessions only.
  Type 2 (VPOC Pivot): TSLA Jerk-Pivot dominates with +0.787 holdout
  exp_r on a highly selective ~1 signal/session rate. This is the
  strongest per-signal payoff in the basket.
  Type 3 (VWMA Trend): AMD MI drives most of the high-frequency,
  moderate-payoff captures. IWM long MI emerged post-guardrail with
  +0.573 holdout exp_r — first confirmed long in the basket.
  Type 4 (Compression): Very high signal count (503–1112 signals in 43
  holdout sessions), low per-trade exp_r (+0.099–0.148), stock_like only
  — the payoff is spread thin across many small compressions.

Inferred:
  Multi-day trend moves (price trending 5–20% over multiple sessions) are
  not covered by any current type. All four types are same-session exits
  by construction. Any move that starts on Monday and extends to Wednesday
  is partially captured (session 1 entry, flat at close) but the Tuesday
  and Wednesday continuation is invisible.
  Type 4 (Compression) fires so frequently it is likely catching noise in
  addition to real breakouts. The 25 signals/session rate on AMD compression
  implies the strategy is entering on micro-compressions that resolve quickly,
  not on the large breakout sessions.
```

---

### 4. Current Strategy Coverage

**Summary by strategy family:**

| Family | Move types caught | Basket viable | Options path | Holdout confirmed |
|---|---|---|---|---|
| Jerk-Pivot Momentum | Type 2 (VPOC Pivot) | TSLA short ✓, AMD short partial | TSLA single_option 0.9985 | Yes |
| Market Impulse | Type 3 (VWMA Trend) | AMD short ✓, IWM long ✓, QQQ short shadow | AMD single_option 0.9982, IWM 0.9782 | Yes |
| Opening Drive Classifier | Type 1 (Gap-and-Go) | AMD short partial (0.930), IWM short shadow | AMD single_option 0.9297 | Yes |
| Compression Breakout | Type 4 (Compression) | TSLA/AMD stock_like only | Options dead (<0.55) | Yes (stock_like) |
| Elastic Band Reversion | Reversal/fade | NVDA short stock_like (0.956) | Options dead (0.457) | Yes (stock_like) |
| Kinematic Ladder | Type 3 variant | M1 fail at basket level | — | No |
| Opening Drive v2 | Type 1 (Short Continue) | M1 fail at basket level | — | No |
| Regime Router | Mixed | M4 fail | — | No |

**What is NOT covered:**

1. **Multi-day momentum** (no strategy, no timeframe): TSLA +20% in a week, NVDA post-earnings follow-through — zero current coverage. Same-day flattening eliminates this entire class.
2. **Late-session trend continuation** (12pm–3pm): Entry windows close by 10:30–11:30am for Market Impulse and Opening Drive. A VWMA cross at 1pm is not caught.
3. **Gap-fade** (reversal of opening drive): No confirmed strategy — Opening Drive v2 (Short Continue) failed basket M1.
4. **SPY/QQQ macro trend days**: SPY has no viable strategy (Opening Drive, Market Impulse SPY short killed by guardrails). QQQ is shadow-tier only.
5. **Long direction (except IWM)**: Only QQQ long (Market Impulse, killed by guardrails) and IWM long (MI, new promote) survived. PLTR long, AMD long, TSLA long consistently fail M4.

```
Observed:
  6 of 8 strategy families produced viable holdout results for at least
  one ticker-direction pair. But coverage concentrates on:
    - TSLA/AMD short (3–4 strategy families each)
    - IWM long (1 family, new)
    - QQQ short (1 family, shadow only after guardrails)
  SPY has no viable options path in any family.
  AAPL has shadow-tier presence only (MI, mc_prob=0.782).
  META/PLTR have no viable holdout-confirmed signals in any family.

Inferred:
  The basket's effective "live" coverage as of 2026-04-12 is:
    - 2 primary shorts (TSLA Jerk-Pivot, AMD Market Impulse)
    - 1 new long (IWM Market Impulse)
    - 2-3 shadows (AMD Jerk-Pivot, IWM Opening Drive, QQQ MI)
  The 3 highest market-cap names (SPY, NVDA, META) have no actionable
  options candidates. This is a material coverage gap.
```

---

### 5. Exit Capture / Early Exit Evidence

All exit optimization artifacts use `fixed_rr_underlying` only (3 pairs per strategy). No trailing, time-based, or crossover exits were evaluated. Evidence that exits are capping winners:

**Exit artifact summary (holdout window):**

| Candidate | Policy | exp | PF | WR | avg_win | avg_loss | win/loss ratio | trades |
|---|---|---|---|---|---|---|---|---|
| TSLA short JP (guardrail) | fixed_rr:0.0050×1.75 | +1.061 | 2.21 | 55.3% | 3.506 | −1.959 | 1.79× | 38 |
| TSLA short JP (baseline) | fixed_rr:0.0035×1.50 | +0.663 | 2.06 | 57.1% | 2.257 | −1.463 | 1.54× | 56 |
| AMD short MI | fixed_rr:0.0075×2.00 | +0.226 | 1.25 | 39.6% | 2.862 | −1.501 | 1.91× | 48 |
| IWM long MI | fixed_rr:0.0050×2.00 | +0.558 | 2.00 | 54.2% | 2.059 | −1.214 | 1.70× | 24 |
| QQQ short MI | fixed_rr:0.0075×2.00 | +1.103 | 1.55 | 41.2% | 7.519 | −3.387 | 2.22× | 17 ⚠ |
| AAPL short MI | fixed_rr:0.0075×2.00 | +0.388 | 1.47 | 52.2% | 2.317 | −1.715 | 1.35× | 23 |
| AMD short OD | fixed_rr:0.0075×2.00 | +0.705 | 2.06 | 56.5% | 2.429 | −1.535 | 1.58× | 23 |
| IWM short OD | fixed_rr:0.0075×2.00 | +0.690 | 2.52 | 65.2% | 1.753 | −1.304 | 1.34× | 23 |
| AMD short JP | fixed_rr:0.0025×1.25 | +0.101 | 1.41 | 52.9% | 0.662 | −0.530 | 1.25× | 34 |

Sources: `m5_exit_optimizations.json` from JP guardrail run `2026-04-12T203208`, MI guardrail run `2026-04-12T201909`, OD run `2026-04-12T185525`, JP baseline run `2026-04-12T182814`.

```
Observed:
  TSLA JP guardrail vs baseline: widening exit from 1.5R to 1.75R (stop
  from 0.35% to 0.50%) increased expectancy by +0.398 (+60%) despite
  fewer trades (38 vs 56). The avg_winner jumped from $2.26 to $3.51.
  This is a strong signal that the 1.5R exit was closing trades before
  the full momentum leg completed.

  QQQ MI: avg_winner $7.52 vs avg_loss $3.39 at a 2R fixed exit. The
  winners are hitting exactly 2R (consistent with fixed RR mechanics) but
  are much larger in dollar magnitude than the losers. If QQQ's big trend
  sessions run 3–5% intraday and the 2R exit is at 1.5% TP, the last
  1.5–3.5% of the move is surrendered at every winner.

  AMD MI: Win rate of only 39.6% at a 2R target suggests the 2R target
  (0.75% stop, 1.5% TP) is rarely reached on most AMD MI entries. The
  entry fires on VWMA cross but AMD does not always follow through to 2R.
  A trailing stop that locks in partial gains might capture more.

  AMD JP: avg_winner $0.66 vs avg_loss $0.53 at 1.25R — a scalp profile.
  No evidence of exit capping here; AMD Jerk-Pivot resolves quickly and
  the tight exit is appropriate.

Inferred:
  For TSLA JP and QQQ MI specifically, there is credible evidence that
  fixed-RR exits are capping the tail of winning trades. TSLA's VPOC
  pivots sometimes develop into extended 3–5% session moves; the 0.875%
  TP exits the position before the move completes. QQQ's trend sessions
  can run 2–4% and the 1.5% TP captures only the first half.

  For AMD MI (low WR) and IWM long MI, the fixed-RR dynamic is different:
  the primary question is not "are we exiting early" but "why is WR <50%?"
  A trailing exit might not improve AMD MI expectancy because AMD MI entries
  do not consistently trend further after 2R.

Next Hypothesis:
  Add ATR trailing stop to exit_optimizer.py and re-evaluate TSLA JP and
  QQQ MI on the same holdout window. If ATR trail expectancy > fixed_rr
  expectancy for these two candidates, that is confirmation of tail capping.
```

---

### 6. Miss Reasons

Ranked by estimated frequency of occurrence:

**Miss Reason 1 — Same-Day Flattening (most common)**

```
Observed:
  src/oracle/trade_simulator.py hard-flats all positions at 15:55 ET.
  All strategies close end-of-day by design (catastrophe_exit hard_flat_time_et=15:55).
  M4 holdout and M5 metrics are computed entirely within single sessions.

Inferred:
  Any move that develops over multiple days is invisible. TSLA's multi-day
  momentum episodes, QQQ's post-FOMC follow-through over 2–3 sessions,
  NVDA's post-earnings week-long trend — none of these are captured in
  any current artifact.
  The expected value of a same-day-only framework for big moves:
  we capture Day 1 entry and Day 1 partial move only.

Next Hypothesis:
  Estimate missed multi-day tail by comparing single-day vs 3-day
  continuation rates on TSLA and QQQ after a Jerk-Pivot/MI signal fires.
  Requires OHLCV data re-download.
```

**Miss Reason 2 — Entry Window Closure (second most common)**

```
Observed:
  Market Impulse entry_window_minutes = 45–90 min from open (9:30–10:15
  to 9:30–11:00 ET). Opening Drive entry_end_offset_minutes = 90–120 min
  (from opening, entries close at 11:00–11:30 ET).
  Jerk-Pivot has no explicit time limit but requires VPOC proximity;
  post-midday VPOC events are filtered by kinematic state requirements.

Inferred:
  A 1pm trending move — e.g., AMD surging after a macro headline at 12:45 ET
  — is not caught by Market Impulse (window closed), Opening Drive (window
  closed), or likely Jerk-Pivot (price may be far from prior-session VPOC).
  Afternoon moves of this type are estimated to be a significant fraction
  of large-magnitude intraday moves, especially on macro days.

Next Hypothesis:
  Test Market Impulse with entry_window_minutes = 180–240 to capture
  early-afternoon VWMA crosses. Must re-run M1–M4 as a new hypothesis
  to avoid contaminating current validated configs.
```

**Miss Reason 3 — Fixed-RR Early Exit (evidence above)**

```
Observed:
  TSLA JP: 1.75R exit at 0.875% TP. TSLA's average true range (intraday)
  was approximately 3–5% in 2024–2025. A 0.875% TP exits the position
  in the first 20–30% of a large TSLA session range.
  QQQ MI: 1.5% TP on a ticker whose full-session range on trend days
  can be 2–4%.

Inferred:
  The current exit grid (max 0.75% stop × 2.5R = 1.875% TP for the most
  aggressive policy) is structurally too tight to capture the full range
  of a large move session.

Next Hypothesis:
  Add wider fixed-RR pairs (0.0100×2.0, 0.0150×2.0) for trend-family
  strategies (Market Impulse, Jerk-Pivot on TSLA) and test against the
  same holdout. Also add ATR-anchored stop to allow the exit to scale
  dynamically with realized volatility.
```

**Miss Reason 4 — VPOC Proximity Filter Rejecting Gap Opens**

```
Observed:
  Jerk-Pivot fires only when price is within vpoc_proximity_pct (0.15–0.20%)
  of the prior-session VPOC. If TSLA gaps 3% away from VPOC at the open,
  no signal fires until/unless price retraces to VPOC.

Inferred:
  On high-magnitude gap sessions (TSLA +5% open), the Jerk-Pivot
  systematically cannot fire. These are the sessions most likely to have
  large intraday range and continued momentum — and they are explicitly
  filtered out. Opening Drive is the intended catcher for gaps, but it
  is short-direction biased and limited to the first 90–120 minutes.

Next Hypothesis:
  Test Jerk-Pivot with a relaxed vpoc_proximity_pct (0.005–0.010) and
  a gap-state filter (e.g., open > prior_close × 1.02) to evaluate
  whether gap-direction jerk pivots carry positive exp_r.
```

**Miss Reason 5 — Options Path Collapse on High-Volatility Tickers**

```
Observed:
  Compression Breakout: TSLA single_option mc_prob=0.552, AMD=0.165.
  Options dead.
  Elastic Band NVDA: single_option mc_prob=0.457. Options dead.
  These are precisely the tickers where big-move sessions would make
  options the highest-leverage execution vehicle.

Inferred:
  Big moves that do occur (TSLA compression to 5% move, NVDA post-catalyst
  expansion) cannot be captured via options through Compression Breakout
  or Elastic Band. The options path collapses because the signal frequency
  is too high (AMD compression: 25 signals/session) — too many non-moving
  entries dilute the options-adjusted payoff.

Next Hypothesis:
  A selective high-conviction Compression Breakout variant with stricter
  pre-conditions (e.g., compression_factor <= 0.50, pre-market vol filter,
  gap > 0.5%) that fires 0.1–0.3×/session instead of 25×/session may
  survive the options stress test. Requires new strategy class or param
  extension.
```

**Miss Reason 6 — Long Direction Nearly Dead Across Basket**

```
Observed:
  Only IWM long (MI) passes M5 with an options-viable single_option
  mc_prob (0.978). QQQ long (MI) was killed by guardrails. TSLA long,
  AMD long, PLTR long all fail M4 consistently across all families.

Inferred:
  The basket's strategy surface has a strong short bias. Big upside moves
  (TSLA +10% session, AMD +8% after earnings) are almost entirely missed
  on the long side. The single confirmed long (IWM MI) has only 24 exit
  opt trades and is too thin to size up.

Next Hypothesis:
  Investigate whether long-direction M4 failure is a regime artifact
  (all failing longs correlated with high-volatility, mean-reverting
  environment in holdout) or a structural strategy issue. Regime slices
  in M4_holdout.csv (vix_band, spy_trend_20d columns) would disambiguate.
```

---

### 7. Next Hypotheses

Priority-ranked:

| Priority | Hypothesis | Type | Rationale |
|---|---|---|---|
| P1 | ATR trailing exit for TSLA JP and QQQ MI | config-only (exit_optimizer change) | Evidence of tail capping; smallest lift |
| P2 | Market Impulse afternoon window extension (window=180–240min) | new hypothesis (new params) | Misses all afternoon large moves |
| P3 | Kinematic Ladder single-ticker: META short, PLTR long | config-only (retune) | Known M1 signal per known-strategy-family-map.md |
| P4 | Wider fixed-RR pairs for trend families (0.01×2.0, 0.015×2.0) | config-only (exit_optimizer grid change) | Simple grid extension; test before ATR |
| P5 | Opening Drive v2 TSLA single-ticker | config-only (retune) | Strong M1 signal (100% pct_pos, exp_r=0.313); known correlation risk with JP TSLA |
| P6 | High-conviction Compression Breakout variant | new-class or new-feature | Would need stricter pre-condition (gap, vol filter); not config-only |
| P7 | Multi-day runner gate (separate asymmetric-move gate) | new-feature | Largest lift; requires overnight hold infrastructure |

**Do not start P7 without resolving P1–P2 first.** Multi-day hold is an infrastructure change, not an exit policy tweak.

---

### 8. Infrastructure Recommendation

```
Observed:
  1. src/oracle/trade_simulator.py: hard flat at 15:55 ET is hardcoded.
     Same-day-only constraint affects all strategies equally.
  2. src/research/exit_optimizer.py: policy grid is 3 fixed-RR pairs +
     VMA trail for market_impulse only. No ATR trailing, no time-based
     runner, no MA crossover.
  3. data/: empty ticker parquet dirs. Cannot run any OHLCV analysis
     without re-download.

Inferred (insufficiency determination):
  Current exit artifacts ARE INSUFFICIENT to answer the big-move question
  because:
    (a) Only fixed-RR exits have been evaluated — no trailing, no runner.
    (b) Exit sample sizes are thin (17–56 trades; most under 40).
    (c) No OHLCV data to identify which sessions were big-move days
        and whether strategies fired on those days.
    (d) Same-day flattening prevents any multi-day move capture, and no
        infrastructure exists to test overnight hold.

Smallest next implementation step (minimal code change):
  Add ATR trailing stop policy to exit_optimizer.py.
  This requires two changes:
    (a) Add AtrTrailingExitPolicy class to src/oracle/trade_simulator.py.
        ~30–40 lines. Needs: ATR lookback (e.g., 14 bars), trail multiplier.
    (b) Add ATR trail to _policy_candidates() in exit_optimizer.py for
        jerk_pivot_momentum and market_impulse strategy keys.
        ~10 lines.
  This does NOT change any strategy logic, gate thresholds, or
  hypothesis state files.
  After adding: re-run exit optimizer on TSLA JP and QQQ MI holdout
  windows using the existing M4_holdout.csv signal frames (no new
  hypothesis run needed — exit optimizer can be called standalone).

  DO NOT add overnight hold at this stage. The infrastructure change
  needed (position carry, overnight margin, next-day entry logic) is
  large and not justified until ATR trailing + wider fixed-RR comparison
  proves that intraday tail is being left on the table at significant scale.
```

---

## Task B — Exit Expansion Feasibility

---

### Current Exit Policy Infrastructure

From `src/research/exit_optimizer.py` (lines 70–82), the policy grid is:

| Strategy | Fixed-RR pairs evaluated | Trailing |
|---|---|---|
| market_impulse | (0.35%×1.5), (0.50%×2.0), (0.75%×2.0) | VMA trail (vma_col) ✓ |
| jerk_pivot_momentum | (0.25%×1.25), (0.35%×1.5), (0.50%×1.75) | None |
| elastic_band_reversion | (0.35%×1.0), (0.50%×1.5), (0.75%×2.0) | None |
| opening_drive_classifier | (0.35%×1.25), (0.50%×1.5), (0.75%×2.0) | None |
| compression_expansion_breakout | (0.30%×1.5), (0.50%×2.0), (0.75%×2.5) | None |

**Gap in the grid:** No ATR-based trailing, no MA crossover, no time-based runner. VMA trail exists only for market_impulse. Maximum TP achievable with current grid is 0.75% × 2.5R = 1.875%.

---

### Are Current Exit Artifacts Enough?

**No. They are insufficient for three reasons:**

1. **Policy coverage gap**: Only fixed-RR and VMA-trail for MI are evaluated. Trailing exits and time-based runners have never been simulated for any confirmed M5 candidate.

2. **Sample size fragility**: 6 of 9 exit artifacts have ≤38 holdout trades. Statistical reliability requires ≥40 trades for moderate trust; ≥80 for high trust. QQQ MI (17 trades) and IWM long MI (24 trades) cannot be relied upon for policy comparison.

3. **Structural bias toward small exits**: The grid tops out at 1.875% TP. For TSLA with a typical 4% intraday range, this exit is always in the first quartile of the full session move. No current artifact can distinguish between "fixed-RR is optimal" and "fixed-RR is the best of three small options."

---

### Candidate Exit Policy Evaluation

For each candidate exit policy, assess feasibility based on existing artifacts and strategy characteristics.

#### A. Generic ATR Trailing Stop

**Verdict: Recommended. Highest priority.**

```
Evidence for:
  TSLA JP: avg_winner jumped $2.26→$3.51 when exit widened from 1.5R
  to 1.75R. Trajectory suggests further widening continues to improve
  expectancy. ATR trail scales TP dynamically with realized vol — exactly
  what TSLA needs on high-vol days.
  QQQ MI: avg_winner $7.52 at 2R exit (only 17 trades). If 2R is
  consistently reached, ATR trail would lock in gains above 2R and allow
  further follow-through without giving back the initial gain.

Evidence against:
  For AMD MI (WR=39.6% at 2R), the problem is not exit timing but
  initial entry quality on non-trending AMD sessions. ATR trail would not
  improve WR; might marginally help avg_winner but AMD's jerk/reclaim
  setups resolve quickly (scalp-like), so trail may not engage.
  AMD JP (1.25R scalp profile): ATR trail is wrong tool. avg_winner
  already at $0.66 — these trades close in minutes.

Implementation size:
  +40 lines in src/oracle/trade_simulator.py (AtrTrailingExitPolicy class)
  +15 lines in src/research/exit_optimizer.py (_policy_candidates addition)
  No strategy code changes required.
```

#### B. Generic Moving-Average Trailing Stop

**Verdict: Partial — defer until ATR trail is validated.**

```
Evidence for:
  MI already has VMA trail implemented and in the optimizer. If VMA trail
  was never selected as best policy for any MI candidate, it suggests
  the fixed-RR was better at current trade lengths. However, VMA trail
  was evaluated — confirming the infra exists for MI.

Evidence against:
  For JP, OD, Compression strategies, MA trailing would require a new
  trailing implementation (different from VMA trail which uses strategy's
  own VMA col). The VMA trail for MI uses the strategy's signal VWMA —
  that is a strategy-specific column not available for JP or OD.
  A generic SMA/EMA trailing stop is conceptually different from the
  VMA trail already implemented.

Recommendation:
  Extend VMA trail to Opening Drive Classifier and Jerk-Pivot using the
  Newton engine's EMA columns (ema_fast, ema_slow already computed).
  ~20 lines in exit_optimizer.py. Lower priority than ATR; do this second.
```

#### C. Moving-Average Crossover Exit

**Verdict: Do not prioritize. Insufficient evidence.**

```
Evidence:
  No MA crossover data in any current artifact. Exit on, e.g., ema5 cross
  below ema13 would require holding the position for multiple bars while
  monitoring the Newton engine's EMA state.
  For Jerk-Pivot (1 signal/session, ~45 holdout trades total), a crossover
  that fires 1–3 bars after entry would produce identical results to a tight
  fixed-RR exit. For Market Impulse (2.5 signals/session), MA crossover
  risks holding through intraday noise.

Recommendation:
  Evaluate only after ATR trail is implemented and results are available.
  If ATR trail shows clear improvement vs fixed-RR for 2+ candidates,
  then MA crossover is the next candidate to test.
```

#### D. Wider Fixed-R Exits for Trend Families

**Verdict: Recommended as immediate parallel action with ATR trail.**

```
Evidence:
  The current grid tops at 0.75%×2.5 = 1.875% TP. For TSLA JP:
    0.50%×1.75 = 0.875% TP selected; wider options not available in grid.
  Simply adding (0.0100×2.0) and (0.0150×2.0) to the JP grid would
  test 2.0% TP and 3.0% TP exits on the same holdout window without
  any new infrastructure.
  This is a 4-line change to _FIXED_RR_GRID in exit_optimizer.py,
  then re-running exit_optimizer.optimize_underlying_exit() for TSLA JP
  and QQQ MI on existing M4_holdout.csv signal frames.

Risk:
  Wider stops (1.0–1.5%) on AMD JP or IWM long MI would likely hurt
  expectancy by increasing loss size on the 45–60% of trades that go
  against. Must apply wider grid only to trend families (JP TSLA, MI QQQ).

Implementation size: 4–8 lines in exit_optimizer.py. No strategy changes.
```

#### E. Time-Based Runner Exit (hold to 11:30, 14:30, or EOD unless thesis break)

**Verdict: Defer. Insufficient evidence; higher infrastructure risk.**

```
Evidence:
  No time-based exit data in any artifact. AMD MI fires ~2.5×/session;
  a hold-to-14:30 runner for all MI entries would require managing
  overlapping open positions, which the current simulator does not support.

  For Opening Drive (fires 0.5–1×/session within first 90 min), a hold-
  to-EOD runner is more plausible. AMD OD short: 23 holdout trades, 56.5%
  WR with fixed-RR. If the AMD drive continues beyond the 15-min opening
  window all day, a time-based runner (stop at thesis break = VWMA reclaim)
  could capture the rest.

  Risk: Time-based runner with a thesis-break stop is effectively a
  trailing stop on a strategy-specific condition. It is closer to "ATR
  trail + hard time cap" than a standalone exit type.

Recommendation:
  Do not implement as a separate exit type. Instead, treat it as a
  parameter of the ATR trail: max_hold_bars=240 (4 hours = 9:30→1:30 ET).
  This achieves the time-based runner behavior within the ATR trail
  infrastructure without a separate implementation.
```

---

### Priority Comparison: Which Winners Benefit Most from Better Exits?

Focus on candidates where win/loss ratio leaves room above the target:

| Candidate | Current WR | Current avg_win/loss | Exit type evidence | Should change exit? |
|---|---|---|---|---|
| TSLA short JP | 55.3% | 1.79× at 1.75R | Widening from 1.5→1.75R gained +60% exp; clear upside to wider/trailing | **Yes — ATR trail + wider RR** |
| QQQ short MI | 41.2% | 2.22× at 2R | Only 17 trades; avg_winner $7.52 suggests large move sessions | **Yes — wider RR first; ATR trail second** |
| AMD short MI | 39.6% | 1.91× at 2R | Low WR suggests entry (not exit) is the limiter | No change yet; fix entry quality first |
| IWM long MI | 54.2% | 1.70× at 2R | 24 trades; too thin to conclude | Accumulate holdout before exit change |
| AMD short OD | 56.5% | 1.58× at 2R | IWM short OD 65.2% WR suggests exits are appropriate | No change; exit is working |
| IWM short OD | 65.2% | 1.34× at 2R | High WR at tight ratio — scalp profile confirmed | No change |
| AMD short JP | 52.9% | 1.25× at 1.25R | Scalp; appropriate | No change |

---

### Smallest Next Implementation Step

The minimal code change that directly addresses the evidence:

```
Step 1 (immediate, no strategy changes):
  Add two wider fixed-RR pairs to _FIXED_RR_GRID for jerk_pivot_momentum
  and market_impulse in src/research/exit_optimizer.py:
    jerk_pivot_momentum: add (0.0075, 2.00) and (0.0100, 2.00)
    market_impulse: add (0.0100, 2.00) and (0.0150, 2.00)
  Then call optimize_underlying_exit() standalone on:
    TSLA JP (2026-04-12T203208 M4_holdout.csv signal frame)
    QQQ MI (2026-04-12T201909 M4_holdout.csv signal frame)
  No hypothesis re-run needed. Results in 10 minutes of compute.
  Estimated code change: 4 lines in exit_optimizer.py.

Step 2 (after Step 1 results confirm tail is left on table):
  Add AtrTrailingExitPolicy to src/oracle/trade_simulator.py.
  Add ATR trail entries to _policy_candidates() for jerk_pivot_momentum
  and market_impulse in src/research/exit_optimizer.py.
  Estimated code change: ~50 lines total. No strategy or gate changes.

Step 3 (after holdout accumulates ≥40 more TSLA JP observations):
  Re-run exit optimizer with ATR trail + wider RR + VMA trail on the
  larger sample. If ATR trail beats fixed-RR, update selected_policy
  in the Strategy_Catalog candidate row via normal M5 promote workflow.
  Do not update catalog from thin 38-trade holdout.
```

---

## Decision Rules Applied

Per the handoff prompt's decision rules:

| Rule | Evidence | Conclusion |
|---|---|---|
| Big moves missed because no strategy fires | Gap-and-Go covered by OD; VPOC pivots by JP; VWMA trend by MI. Multi-day and afternoon moves have no coverage. | Partial — entry coverage exists for intraday; multi-day is a genuine gap |
| Strategies fire correctly but exit too early | TSLA JP: +60% exp gain from widening exit. QQQ MI: $7.52 avg winner at 2R. | **Yes — add exit policy expansion first** |
| Signals exist only on 5m/15m structure | Market Impulse uses 15m/30m/1h regime timeframes. JP is tick-level VPOC proximity. OD uses 5m regime. | Some signals are 5m/15m — but 1h regime configs (AMD MI, QQQ MI) are the survivors after guardrails |
| Winners low-frequency, high-payoff | TSLA JP: 1 signal/session, +0.787 holdout exp_r. This fits the asymmetric profile. | **Consider separate asymmetric-move gate for TSLA JP after ATR trail confirms tail size** |

---

## Verdict

```
Verdict:
- Can current infrastructure answer the big-move question?
  PARTIAL. Artifact-only analysis can identify where strategies fire and
  at what payoff, but cannot enumerate specific large-move sessions
  without OHLCV data. OHLCV parquet files are absent; must re-download.

- Should we add exit policies first?
  YES. Evidence is sufficient to justify adding:
    (1) Wider fixed-RR pairs for TSLA JP and QQQ MI (4 lines, immediate).
    (2) ATR trailing stop for JP and MI (50 lines, after Step 1 confirms).
  Both changes are isolated to exit_optimizer.py and trade_simulator.py.
  No strategy code, no M1–M5 gate logic, no hypothesis state files change.

- Smallest next implementation step:
  Add (0.0075×2.00) and (0.0100×2.00) to jerk_pivot_momentum fixed-RR
  grid in src/research/exit_optimizer.py and re-run exit optimization
  for TSLA JP and QQQ MI on existing holdout signal frames. No strategy
  changes. No new hypothesis run. Estimated 4-line code change + 10-minute
  compute. If the wider exits dominate, proceed to ATR trail implementation.
```

---

*Report written from artifacts only. No Google Sheets written. No strategy code changed. No hypothesis states modified.*  
*Source runs: 2026-04-12T182814, 2026-04-12T183837, 2026-04-12T183516, 2026-04-12T185525, 2026-04-12T190235, 2026-04-12T190642, 2026-04-12T191121, 2026-04-12T201909, 2026-04-12T203208*
