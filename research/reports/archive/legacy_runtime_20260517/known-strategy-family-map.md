# Known Strategy Family Map

**Generated:** 2026-04-12  
**Source runs:** All basket-discovery and catalog-replay hypotheses as of this date.  
**Scope:** 8 strategy families, 12 hypothesis runs (9 basket-discovery, 3 single-symbol replays).

---

## 1. Which Families Produced M5 Candidates?

| Family | Run | Reached M5? | M4 promotions | M5 viable? |
|---|---|---|---|---|
| Market Impulse (Cross & Reclaim) | 2026-04-12T152932 | Yes | 22 | Yes — options + stock_like |
| Jerk-Pivot Momentum (tight) | 2026-04-12T182814 | Yes | 6 | Yes — strong options path |
| Compression Expansion Breakout | 2026-04-12T183837 | Yes | 8 | Partial — stock_like only; options dead |
| Opening Drive Classifier | 2026-04-12T185525 | Yes | 9 | Partial — AMD options (mc_prob=0.930), stock_like |
| Elastic Band Reversion | 2026-04-12T191121 | Yes | 1 | Stock_like only (mc_prob=0.956); options dead |
| Kinematic Ladder | 2026-04-12T183516 | No (M1 fail) | — | — |
| Opening Drive v2 (Short Continue) | 2026-04-12T190235 | No (M1 fail) | — | — |
| Regime Router (Kinematic + Compression) | 2026-04-12T190642 | No (M4 fail) | 0 | No |

**Strong options families (>= 1 config with single_option mc_prob ≥ 0.90):**
- Market Impulse — multiple QQQ/AMD/QQQ-long configs
- Jerk-Pivot — TSLA short (mc_prob=0.999–1.000)
- Opening Drive Classifier — AMD short 5m-regime (mc_prob=0.930)

**Stock_like-only families:**
- Compression Breakout — TSLA/AMD stock_like strong, options dead
- Elastic Band Reversion — NVDA short stock_like (mc_prob=0.956), options dead

---

## 2. Which Symbols/Directions Recur Across Families?

| Symbol | Direction | Families with viable signal |
|---|---|---|
| **TSLA** | short | Jerk-Pivot (PROMOTE), Market Impulse (shadow), Compression Breakout (stock_like shadow), Opening Drive v2 (retune candidate) |
| **AMD** | short | Market Impulse (PROMOTE ×2), Opening Drive Classifier (PROMOTE), Compression Breakout (M5 completed, options dead) |
| **QQQ** | short | Market Impulse (PROMOTE ×3) |
| **QQQ** | long | Market Impulse (PROMOTE ×1) — only confirmed long across all families |
| **NVDA** | short | Elastic Band (PROMOTE stock_like), Market Impulse (kill), Jerk-Pivot (kill) |
| **IWM** | short | Opening Drive Classifier (shadow) |
| **SPY** | short | Market Impulse (shadow) |
| **META** | short | Market Impulse (shadow), Kinematic Ladder (retune candidate) |
| **PLTR** | short | Market Impulse (shadow) |
| **PLTR** | long | Kinematic Ladder (retune candidate) — NOT confirmed via full M5 |

**Key recurring edge pairs:**
- **TSLA short** is the most cross-family confirmed signal: 3 separate strategy families show M4+ survival
- **AMD short** is the second: 2 families promote it (Market Impulse, Opening Drive)
- **QQQ short** is confirmed exclusively through Market Impulse — no other family has produced a QQQ short survivor
- **Long direction** is nearly dead: only QQQ long (Market Impulse) survives; PLTR long and AMD long fail consistently across all families

---

## 3. Candidate Status: Promote / Shadow / Watch / Kill

### PROMOTE

| Symbol | Dir | Family | Config | mc_prob (single_option) | Notes |
|---|---|---|---|---|---|
| QQQ | short | Market Impulse | ema5,90,30m,5,13,21 | ~0.98+ | Strongest MI config |
| QQQ | short | Market Impulse | ema3,90,30m,5,13,21 | ~0.96+ | |
| QQQ | short | Market Impulse | ema5,60,1h,5,13,21 | ~0.95+ | |
| QQQ | long | Market Impulse | ema5,90,30m,5,13,21 | ≥0.95 | First confirmed long across all families |
| AMD | short | Market Impulse | ema5,90,30m,3,8,21 | ≥0.95 | |
| AMD | short | Market Impulse | ema3,90,30m,3,8,21 | ≥0.95 | |
| AMD | short | Opening Drive | 5m-regime,0.002,15min | 0.930 | Just below 0.95; stock_like 0.988 |
| TSLA | short | Jerk-Pivot | 10min,1lb,1.2atr,0.002 | 0.999 | Strongest single-config in batch |
| TSLA | short | Jerk-Pivot | 10min,1lb,1.3atr,0.0015 | 1.000 | |
| NVDA | short | Elastic Band | z=1.75,w=360,kin5 | 0.457 (options dead) | **Stock_like only** mc_prob=0.956 |

### SHADOW (live monitoring, not full size)

| Symbol | Dir | Family | Config | mc_prob (single_option) | Notes |
|---|---|---|---|---|---|
| SPY | short | Market Impulse | TBD | ~0.90–0.94 | Below promote threshold |
| TSLA | short | Market Impulse | 15min config | ~0.90 | Correlated with Jerk-Pivot |
| META | short | Market Impulse | ema3,90,30m | ~0.88–0.92 | Thin holdout |
| PLTR | short | Market Impulse | ema3,60,1h | ~0.87 | Thin |
| TSLA | short | Jerk-Pivot | 8min/5min configs | 0.933–0.940 | Shadow alongside promote configs |
| AMD | short | Opening Drive | 5m-regime,0.0015 | 0.899 | Correlated w/ Config A |
| IWM | short | Opening Drive | 20min,0.002 | 0.867 | Retune if holdout reaches 50+ |
| TSLA | short | Compression Breakout | 20,0.7,15 config | 0.503 (options dead) | **Stock_like shadow only** |

### WATCH (passed M5 but below shadow thresholds; re-evaluate after accumulating holdout)

| Symbol | Dir | Family | Config | Notes |
|---|---|---|---|---|
| AMD | combined | Elastic Band | z=3.0,w=360-dm,kin3 | Near-miss M4 pass (exp_r=+0.008 at 12bps); monitor next rerun |
| TSLA | short | Elastic Band | z=2.5,w=240+dm | Failed 12bps by margin; monitor |

### KILL (confirmed non-viable)

| Symbol | Dir | Family | Reason |
|---|---|---|---|
| NVDA | short | Market Impulse | Consistent M4 fail across both runs |
| NVDA | short | Jerk-Pivot | M5 options collapse (mc_prob=0.461) |
| NVDA | long | Opening Drive | mc_prob=0.424 |
| AAPL | short | Opening Drive, Market Impulse | mc_prob <0.50 |
| PLTR | long | Multiple | Negative holdout across Market Impulse, Regime Router, Jerk-Pivot, Opening Drive |
| META | short | Jerk-Pivot, Opening Drive | <15 holdout signals or negative |
| SPY | combined | Opening Drive, Jerk-Pivot | M4 negative or very weak M5 |
| IWM | all | Jerk-Pivot, Market Impulse | No M4 survivor in either |
| QQQ | short | Jerk-Pivot | No M4 survivor |

---

## 4. Dead Families (Do Not Allocate More Research Capacity)

### Regime Router (Kinematic + Compression) — DEAD
- Fixed-parameter meta-strategy. Only 1 config to evaluate.
- M4 FAIL: PLTR long negative holdout (−0.16 to −0.19 exp_r)
- M1 aggregate exp_r=+0.0443 — the weakest M1 pass in the entire batch
- The routing logic (kinematic-in-trend, compression-in-compression) does not concentrate edge; it dilutes it across tickers where the two sub-strategies' edges do not align
- **Do not re-run.** Needs fundamental redesign (dynamic symbol selection, regime-aware weighting) to be worth revisiting.

### Opening Drive v2 (Short Continue) — Basket Level DEAD; Single-Ticker Retune Only
- Basket M1 failure driven by negative-contributing tickers pulling pct_positive below 60%
- Not worth re-running as a basket hypothesis. Retune path is single-ticker TSLA only (see Section 5).

---

## 5. Families That Deserve M1 Retune Work

### Kinematic Ladder — High Priority Retune
**Why:** Strong individual ticker signals at M1 level that the basket aggregate obscures.
- META short (rw=30/aw=20): 340 signals, 5 windows, pct_pos=60%, exp_r=+0.257 — strong
- PLTR long (rw=30/aw=20): 181 signals, 5 windows, pct_pos=60%, exp_r=+0.247 — strong
- Basket aggregate failed because other tickers (SPY, QQQ, IWM, AMD, NVDA) contributed too few signals for the aggregate to clear the basket threshold.

**Retune path:** Single-ticker runs for META short and PLTR long using the winning no-vol/large-window configs (rw=30/aw=20, no volume filter). These should pass M1 individually and advance to M4.

**Risk:** PLTR long has consistently failed M4 in other strategy families (Market Impulse, Regime Router, Opening Drive). The Kinematic Ladder PLTR long M1 signal may not hold through M4 holdout. Run with caution — don't over-invest here without M4 confirmation.

### Opening Drive v2 (Short Continue) — Medium Priority Retune
**Why:** TSLA short shows genuine M1 signal in isolation (5 windows, 96-98 signals, 100% positive, exp_r=0.313–0.391).
- Single-ticker TSLA run with 15min opening window, 120min entry, 3 kin_lb, 0.0005 breakout buffer should clear M1 and advance to M4.
- Secondary: AMD short with 5m regime filter was 3 signals shy of threshold (47 vs 50). Also worth a single-ticker retry.

**Risk:** TSLA short is already covered by Jerk-Pivot (promotes) and Market Impulse (shadow). Adding Opening Drive v2 TSLA short would create a highly correlated position. Evaluate whether the marginal diversification of a third TSLA short strategy justifies the retune effort.

---

## 6. Cross-Family Correlation Risk

Several promoted/shadow candidates are likely highly correlated:

| Signal Cluster | Configs | Concern |
|---|---|---|
| TSLA short | Jerk-Pivot ×2 (promote), Jerk-Pivot ×2 (shadow), Market Impulse (shadow), Opening Drive v2 retune candidate | 5+ correlated TSLA short configs across families |
| AMD short | Market Impulse ×2 (promote), Opening Drive ×2 (promote + shadow), Compression Breakout (shadow) | High correlation; size AMD short carefully across strategies |
| QQQ short | Market Impulse ×3 (promote) | Concentrated in one family — less cross-family risk but within-family correlation |

**Implication:** Running all TSLA short shadows simultaneously at full size is not advisable. Treat the TSLA short signal as one portfolio position allocated across multiple execution vehicles, not independent positions.

---

## 7. Summary Scorecard

| Family | M5 viable | Options path | Stock_like path | Status | Next action |
|---|---|---|---|---|---|
| Market Impulse | Yes | Strong (QQQ, AMD promotes) | Yes | Active | Accumulate holdout; monitor shadows |
| Jerk-Pivot | Yes | Very strong (TSLA 0.999–1.000) | Yes | Active | Run exit opt with more data when holdout accumulates |
| Compression Breakout | Partial | Dead | Strong (mc_dd_p50 high) | Stock_like shadow | Watch TSLA compression-entry; size very conservatively |
| Opening Drive Classifier | Yes | Marginal (AMD 0.930) | Yes | Active | Shadow AMD 5m-regime; 60-day IWM monitor |
| Elastic Band Reversion | Partial | Dead | NVDA short viable | Active (pending exit opt) | **Run exit opt for NVDA short z=1.75/w=360/kin5** |
| Kinematic Ladder | No | — | — | Retune | Single-ticker META short + PLTR long M1 runs |
| Opening Drive v2 | No | — | — | Retune | Single-ticker TSLA, then AMD with 5m regime |
| Regime Router | No | — | — | **Kill** | Do not revisit without fundamental redesign |

---

*Generated from: 12 hypothesis runs across 8 strategy families, 2026-04-12.*
*See individual FINDINGS_REPORT.md files in `data/results/hypothesis_runs/` for per-run detail.*
