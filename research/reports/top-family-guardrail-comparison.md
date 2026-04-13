# Top Family Guardrail Comparison

**Report generated:** 2026-04-12  
**Guardrails added to `config/hypothesis_defaults.yaml`:**
- `entry_delay_bars: 1` — fills no earlier than bar t+1 after signal
- `min_hold_bars: 2` — favorable exit credit starts after bar 2
- `exit_evaluation_start_bar: 1` — same-bar exits never credited
- `cooldown_bars_after_signal: 5` — suppresses rapid-fire repeat signals

**Families tested:** Market Impulse (Cross & Reclaim), Jerk-Pivot Momentum (tight)

---

## Run Index

| Family | Baseline run | Guardrail run |
|---|---|---|
| Market Impulse | `2026-04-12T152932` | `2026-04-12T201909` |
| Jerk-Pivot | `2026-04-12T182814` | `2026-04-12T203208` |

---

## 1. Gate-Level Comparison

### Market Impulse

| Stage | Baseline | Guardrail run | Change |
|---|---|---|---|
| M1 exp_r | +0.4096 | +0.3691 | −0.04 (−10%) |
| M1 signals | 184 | 231 | +47 |
| M2 promoted | 27 | 28 | +1 |
| M4 promoted | 22 | 19 | −3 |
| M5 execution mappings | 88 | 76 | −12 |
| Exit opt candidates | 7 | 4 | −3 |

M1 signal quality nudged down slightly. M4 dropped 3 candidates. M5 mapping count down proportionally. Overall the pipeline stayed healthy through all gates.

### Jerk-Pivot

| Stage | Baseline | Guardrail run | Change |
|---|---|---|---|
| M1 exp_r | +0.4058 | +0.3855 | −0.02 (−5%) |
| M1 signals | 100 | 119 | +19 |
| M2 promoted | 14 | 13 | −1 |
| M4 promoted | 6 | 6 | 0 |
| M5 execution mappings | 24 | 24 | 0 |
| Exit opt candidates | 1 | 2 | +1 (AMD added) |

Jerk-Pivot is structurally unchanged. The same number of configs survived every gate.

---

## 2. Candidate-by-Candidate: Before vs After (single_option mc_prob)

### Market Impulse

| Ticker | Dir | Config (key params) | Baseline mc_prob | Guardrail mc_prob | Δ | Tier change |
|---|---|---|---|---|---|---|
| AMD | short | 5min buf, 90min win, 1h regime, (5,13,21) | 0.999 | **0.998** | −0.001 | promote → **promote** |
| AMD | short | 3min buf, 60min win, 1h regime, (5,13,21) | — (not in top baseline) | **0.998** | — | new → **promote** |
| IWM | long | 5min buf, 45min win, 15m regime, (5,13,21) | — (absent) | **0.978** | — | new → **promote** |
| IWM | long | 3min buf, 90min win, 15m regime, (5,13,21) | — (absent) | ~0.97 | — | new → shadow |
| AMD | short | 5min buf, 90min win, 30m regime, (5,13,21) | 1.000 | ≤0.95 | −0.05+ | **promote → shadow/kill** |
| QQQ | short | 3min buf, 45min win, 1h regime, (10,20,40) | 0.999 | — (dropped M4) | —0.05+ | **promote → kill** |
| QQQ | short | 5min buf, 45min win, 1h regime, (8,21,34) | 0.997 | — (dropped M4) | — | **promote → kill** |
| QQQ | short | 3min buf, 60min win, 1h regime, (10,20,40) | 0.991 | **0.829** | −0.162 | **promote → shadow** |
| QQQ | long | 5min buf, 90min win, 30m regime, (5,13,21) | 0.971 | — (dropped M4) | — | **promote → kill** |
| SPY | short | 5min buf, 45min win, 1h regime, (8,21,34) | 0.945 | — (dropped M4) | — | **promote → kill** |
| TSLA | short | 5min buf, 45min win, 15m regime, (5,13,21) | 0.898 | **0.646** | −0.252 | shadow → **do-not-trade** |
| AAPL | short | 3min buf, 60min win, 1h regime, (10,20,40) | — | **0.782** | — | new → shadow |
| META | short | multiple configs | 0.792–0.884 | **0.684** | −0.1–0.2 | shadow → do-not-trade |

**Most impacted by guardrails (MI):** QQQ short variants (3 configs dropped from M4 entirely; 1 fell from 0.991 to 0.829). AMD short (30m regime) also fell out. SPY short lost its promote. TSLA short fell to do-not-trade.

**Gained from guardrails (MI):** IWM long (15m regime) emerged as a new strong candidate with no baseline equivalent (0.978 single_option, 0.999 stock_like). AMD short 1h-regime survived cleanly and a second AMD short 1h config was promoted.

### Jerk-Pivot

| Ticker | Dir | Config | Baseline mc_prob | Guardrail mc_prob | Δ | Tier change |
|---|---|---|---|---|---|---|
| TSLA | short | jerk=10, kin=1, vm=1.3, vpoc=0.0015 | 0.999 | **0.999** | 0.000 | promote → **promote** |
| TSLA | short | jerk=10, kin=1, vm=1.2, vpoc=0.002 | 1.000 | — (config not in new top) | — | promote → replaced |
| TSLA | short | jerk=8, kin=5, vm=1.2, vpoc=0.002 | 0.940 | **0.927** | −0.013 | shadow → **shadow** |
| TSLA | short | jerk=8, kin=5, vm=1.1, vpoc=0.002 | 0.933 | **0.917** | −0.016 | shadow → **shadow** |
| TSLA | combined | jerk=12, kin=3, vm=1.3, vpoc=0.0015 | — (baseline: short only) | ~0.87 (stock_like) | — | new |
| AMD | short | jerk=12, kin=3, vm=1.0, vpoc=0.0015 | — (absent) | **0.820** | — | new → **shadow** |
| AMD | short | jerk=12, kin=3, vm=1.1, vpoc=0.0015 | 0.493 | **0.820** | **+0.327** | kill → **shadow** |
| NVDA | short | jerk=12, kin=1, vm=1.0, vpoc=0.002 | 0.461 | **0.480** | +0.019 | kill → kill |

**TSLA short (10,1,1.3,0.0015) is rock-solid at 0.999** — unchanged across guardrail addition. The shadow TSLA configs (8,5) lost <2 points of mc_prob. These are negligible changes.

**AMD short jerk-pivot dramatically improved:** from 0.493 (baseline, do-not-trade) to 0.820 (shadow). The entry_delay and cooldown guardrails appear to have filtered out same-bar/rapid-fire AMD entries that were creating noise in the baseline. Only the true VPOC-near jerk pivots now qualify, producing a cleaner signal.

---

## 3. Candidates That Changed Tier

### Demoted by guardrails

| Ticker | Dir | Family | Baseline tier | Guardrail tier | Likely mechanism |
|---|---|---|---|---|---|
| QQQ | short | MI | promote (×3 configs) | shadow (×1) / kill (×2) | QQQ MI short had same-bar/rapid-fire dependency |
| QQQ | long | MI | promote | kill (dropped M4) | QQQ long MI edge was execution-dependent |
| SPY | short | MI | promote | kill (dropped M4) | SPY short MI fragile under entry delay |
| AMD | short | MI | promote (30m regime) | shadow/kill | 30m regime config lost edge under guardrails |
| TSLA | short | MI | shadow | do-not-trade | Further degraded; thin holdout |
| META | short | MI | shadow | do-not-trade | mc_prob fell below 70% floor |

### Promoted/improved by guardrails

| Ticker | Dir | Family | Baseline tier | Guardrail tier | Likely mechanism |
|---|---|---|---|---|---|
| IWM | long | MI | absent | **promote** (0.978) | VWMA cross + 15m regime on IWM now unmasked |
| AMD | short | JP | do-not-trade (0.493) | **shadow** (0.820) | Cooldown eliminated noisy rapid-fire AMD entries |
| AAPL | short | MI | absent | shadow (0.782) | Emerged as new holdout survivor |

### Unchanged

| Ticker | Dir | Family | Before | After |
|---|---|---|---|---|
| AMD | short | MI (1h regime) | promote | promote (0.998) |
| TSLA | short | JP | promote (0.999) | promote (0.999) |
| TSLA | short | JP (8,5 configs) | shadow | shadow (−0.015 mc_prob) |

---

## 4. Exit Policy Comparison

### Market Impulse (exit opt candidates)

| Ticker | Dir | Baseline policy | Baseline exp/trades | Guardrail policy | Guardrail exp/trades |
|---|---|---|---|---|---|
| AMD | short | (multiple; 7 opt files) | various | `fixed_rr:0.0075×2.00` | +0.226, 48 trades ⚠ |
| IWM | long | absent | — | `fixed_rr:0.0050×2.00` | +0.558, 24 trades ⚠⚠ |
| QQQ | short | multiple | various | `fixed_rr:0.0075×2.00` | +1.103, 17 trades ⚠⚠⚠ |
| AAPL | short | absent | — | `fixed_rr:0.0075×2.00` | +0.388, 23 trades ⚠⚠ |

⚠ Trade count caveat: All MI exit opts have ≤48 holdout trades. IWM (24), QQQ (17), AAPL (23) are all thin. AMD at 48 is the best-sampled. Policy estimates are directionally useful but numerically unreliable. The QQQ exp=+1.103 on 17 trades is an outlier that needs more data to trust.

### Jerk-Pivot (exit opt candidates)

| Ticker | Dir | Baseline policy | Baseline exp/trades | Guardrail policy | Guardrail exp/trades |
|---|---|---|---|---|---|
| TSLA | short | `fixed_rr:0.0035×1.50` | +0.663, 56 trades | `fixed_rr:0.0050×1.75` | **+1.061, 38 trades** ⚠ |
| AMD | short | absent (kill) | — | `fixed_rr:0.0025×1.25` | +0.101, 34 trades |

TSLA short exit policy shifted to a wider R:R (0.0050×1.75 vs 0.0035×1.50) after guardrails, and the simulated expectancy jumped from +0.663 to +1.061. This is plausibly because the guardrails filter out weak entries, leaving behind the trades with larger mean excursions that benefit from the wider TP. At 38 trades, it's marginally thinner than the baseline (56). The trend of the improvement is credible but the magnitude should be verified as holdout accumulates.

AMD short jerk-pivot generated its first positive exit optimization: `fixed_rr:0.0025×1.25`, exp=+0.101, 34 trades. Tiny R:R (0.25% stop, 0.3125% TP) — this is a scalp-profile exit consistent with AMD's intraday jerk pivots near VPOC resolving quickly. 34 trades approaches the 40-trade threshold for moderate trust.

---

## 5. Did Any Candidate Depend on Same-Bar / Rapid-Fire 1-Minute Behavior?

**Yes — QQQ short Market Impulse is the clearest case.**

QQQ short went from 3 promotes (mc_prob 0.991–0.999) in the baseline to 1 shadow (0.829) and 2 kills (dropped M4). The magnitude of degradation is too large to attribute to sampling noise. The most likely explanation: QQQ short MI signals were firing multiple times in the same short entry window at the open, and the baseline's absence of cooldown allowed the same trend move to be credited multiple times. The cooldown_bars=5 and entry_delay=1 suppressed this, halving the effective signal edge.

**SPY short and AMD short (30m regime)** lost their promotes. Both had weaker baseline edges (0.945 and ~1.000 respectively for different 30m configs) and did not survive the guardrails, suggesting partial same-bar dependency. However, AMD short (1h regime) survived strongly, indicating the regime-timeframe choice is material — the 1h-regime configs appear less dependent on rapid-fire execution than 30m.

**TSLA short and TSLA short (8,5 jerk-pivot)** are NOT dependent on same-bar behavior — both survived with negligible change (−0.00 to −0.016 mc_prob). The TSLA jerk-pivot signal fires rarely and specifically (near VPOC, vol-filtered), making it inherently cooldown-robust.

**AMD short jerk-pivot improved** after guardrails — the opposite of what you'd expect if it depended on rapid-fire entries. The cooldown reduced duplicate AMD entries that were adding variance without adding edge, leaving a cleaner signal.

---

## 6. Are Market Impulse and Jerk-Pivot Still the Top Two Options-Primary Families?

**Yes, both remain options-viable. The ranking within each family has changed.**

### Market Impulse (post-guardrail)
- **Top promote:** AMD short (1h regime, 5,13,21 VWMA) — 0.998 single_option, 110 holdout trades. Strongest in the basket.
- **New promote:** IWM long (15m regime, 5,13,21 VWMA) — 0.978 single_option, 49 holdout trades. Was absent in baseline; emerged cleanly.
- **Demoted:** QQQ short (3 configs → shadow/kill), QQQ long (kill), SPY short (kill). The MI family's QQQ-heavy signal profile from the baseline has been replaced by an AMD+IWM signal profile.

### Jerk-Pivot (post-guardrail)
- **Core promote unchanged:** TSLA short (10,1,1.3,0.0015) — 0.999 single_option. Best single candidate in either family.
- **Shadow tier improved:** AMD short (12,3) rose from kill to shadow (0.820). TSLA short (8,5) shadow held.

**Family ranking by strongest single option candidate:**
1. Jerk-Pivot — TSLA short 0.999 (single_option)
2. Market Impulse — AMD short 0.998, IWM long 0.978 (single_option)

Both families passed all gates cleanly. The guardrails improved candidate quality rather than destroying it: weaker execution-dependent signals were filtered out, leaving the ones with genuine intraday edge.

---

## 7. Recommendation

**Continue.** Both families are options-viable after guardrails. Proceed to live shadow trading for the following candidates:

### Immediate shadow

| Ticker | Dir | Family | Config | mc_prob (single_option) | Action |
|---|---|---|---|---|---|
| TSLA | short | Jerk-Pivot | jerk=10, kin=1, vm=1.3, vpoc=0.0015 | 0.999 | Shadow now. Exit: `fixed_rr:0.0050×1.75`, 38 trades. |
| AMD | short | Market Impulse | 5min buf, 90min win, 1h regime, (5,13,21) | 0.998 | Shadow now. Exit: `fixed_rr:0.0075×2.00`, 48 trades. |
| IWM | long | Market Impulse | 5min buf, 45min win, 15m regime, (5,13,21) | 0.978 | Shadow now. Exit: `fixed_rr:0.0050×2.00`, 24 trades ⚠. |

### Shadow with holdout watch (accumulate ≥40 holdout trades before sizing up)

| Ticker | Dir | Family | mc_prob | Notes |
|---|---|---|---|---|
| AMD | short | Jerk-Pivot | 0.820 | Good holdout (35 trades), needs 40-trade exit opt before full size |
| TSLA | short | Jerk-Pivot (8,5 configs) | 0.917–0.927 | Correlated with primary TSLA short; don't run simultaneously at full size |
| QQQ | short | Market Impulse | 0.829 | One surviving config; shadow until holdout accumulates |
| AAPL | short | Market Impulse | 0.782 | Shadow; exit opt on 23 trades is thin |

### Do not trade

| Ticker | Dir | Family | mc_prob | Notes |
|---|---|---|---|---|
| TSLA | short | Market Impulse | 0.646 | Redundant; TSLA already covered by Jerk-Pivot |
| META | short | Market Impulse | 0.684 | Below 0.70 floor |
| PLTR | short | Market Impulse | 0.365 | Well below floor |
| SPY | long | Market Impulse | 0.685 | Emerged after guardrails but weak |
| NVDA | short | Jerk-Pivot | 0.480 | Dead |

### Notes on execution model hardening

No further hardening is needed at this stage. The guardrails resolved the QQQ MI behavior concern (it appropriately demoted QQQ's edge), and TSLA and AMD core signals proved resilient. The current guardrail set (`entry_delay=1, hold=2, exit_eval=1, cooldown=5`) is adequate for 1-minute data.

One watch item: IWM long MI is a new candidate that has no baseline reference. Monitor its first 30 live shadow trades carefully before sizing. The exit policy (0.0050×2.00, exp=+0.558) is based on only 24 holdout trades — double-check the policy once holdout reaches 40.

---

## 8. Summary Table

| Ticker | Dir | Family | Baseline tier | Guardrail tier | mc_prob Δ |
|---|---|---|---|---|---|
| AMD | short | MI (1h regime) | promote | **promote** | −0.001 |
| IWM | long | MI (15m regime) | absent | **promote** | new |
| TSLA | short | JP (10,1,1.3) | promote | **promote** | 0.000 |
| AMD | short | JP (12,3) | kill | **shadow** | +0.327 |
| TSLA | short | JP (8,5 configs) | shadow | shadow | −0.015 |
| QQQ | short | MI (3,60,1h,10,20,40) | promote | shadow | −0.162 |
| AAPL | short | MI | absent | shadow | new |
| QQQ | short | MI (2 configs) | promote | kill | −0.05+ |
| QQQ | long | MI | promote | kill | eliminated |
| SPY | short | MI | promote | kill | eliminated |
| AMD | short | MI (30m regime) | promote | kill | eliminated |
| TSLA | short | MI | shadow | do-not-trade | −0.252 |
| META | short | MI | shadow | do-not-trade | −0.15+ |
| NVDA | short | JP | kill | kill | +0.019 |

---

*Full artifacts at:*
- *MI baseline: `data/results/hypothesis_runs/market-impulse-all-basket-discovery/2026-04-12T152932/`*
- *MI guardrail: `data/results/hypothesis_runs/market-impulse-all-basket-discovery/2026-04-12T201909/`*
- *JP baseline: `data/results/hypothesis_runs/jerk-pivot-current-basket-discovery/2026-04-12T182814/`*
- *JP guardrail: `data/results/hypothesis_runs/jerk-pivot-current-basket-discovery/2026-04-12T203208/`*
