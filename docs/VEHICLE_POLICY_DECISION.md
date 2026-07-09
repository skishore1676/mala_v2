# Vehicle Policy Decision: OI Floor, Premium Cap, Risk-Rail Pair

**Read-only pull. No code/config/DB changes. Data as of 2026-07-08, queried live over ssh from `oldmac`.**
Sources: `~/Documents/bhiksha/bhiksha.db` (events, trade_sessions, cash_budget_days), `~/Documents/bhiksha/artifacts/playbook/active_plan.json` (compiled plan, generated 2026-07-08T13:20:11Z), `~/Documents/kamandal_v2/data/kamandal_v2.db` (`chain_snapshots`, public-quote source only).

---

## Summary (what the data says)

- **Selector rejections since 2026-07-02: 12 events, only 3 on LIVE lanes** (SMH `live_row_6` once on 07-07, AMD `live_row_2` twice on 07-08 — the two AMD events are 25 minutes apart with near-identical breakdowns, almost certainly the same missed signal re-checked, not two independent missed trades). **`open_interest_below_min` is the largest recorded bucket in 11 of 12 events** (48–97% of the in-window candidate pool), but there's a structural blind spot in the selector's own waterfall (detailed below) that means the logs **cannot tell us how many of those OI-blocked contracts would also have failed delta/spread** — so "OI is the dominant killer" is true of the *logged bucket sizes*, not proven to be the sole fix.
- **Live entries since 07-02: 6 fills, total premium deployed ≈ $7,232.** Corrected for a DB inconsistency found in `trade_sessions` (see caveats — the QQQ row reads `quantity=1` but the broker-confirmed fill was `quantity=3`), **3 of 6 (50%) are genuine 1-lot entries** (SMH, AMD×2), and both AMD fills used only 55–61% of their lane's premium cap because the next lot doesn't fit under $1,700 — that's unused cap sitting idle, not a floor problem.
- **1-lot entries structurally break the ladder exit.** The exit profile's `target_1_quantity=0.6` (sell 60% at T1) is not executable on a 1-contract position — SMH and both AMD fills could not partial-exit as designed. NVDA (12), IWM (12), and QQQ (3, corrected) could.
- **Illustrative chain data has a real gap, not just staleness.** `kamandal`'s public chain snapshot for AMD only carries monthly-cycle expirations (23/30/37/44/72/100 DTE); the live rejections needed 3–7 DTE (weeklies), which don't exist in that data source at all — zero overlap, not just "market moved since." SMH has no chain data in kamandal at all. Section 3 is illustrative-only for this reason, clearly bounded.
- **The 5%/7.5% rail pair is confirmed live** (`risk_manager_startup` event: `max_daily_drawdown_pct=5.0`, `flatten_daily_drawdown_pct=7.5`), against today's actual `usable_budget=$10,236.94`. At current per-lane caps, a single worst-case stop-out on AMD, SMH, or NVDA's lane **already crosses the 5% halt threshold alone** — raising any cap to $2,500 or $3,000 uniformly would make that true for every live lane, and at $3,000 a single stop-out gets close to also tripping the 7.5% flatten-everything tier.

---

## 1. Selector rejections since 2026-07-02

### 1a. All 12 events (measured)

| id | date | symbol | lane | live? | window*<br>(right type+DTE) | OI-blocked | delta_below | delta_above | spread | dominant filter |
|---|---|---|---|---|---|---|---|---|---|---|
| 441639 | 07-02 13:52 | AMZN | `..._amzn_short_shadow_row_10` | shadow | 63 | 37 (59%) | 23 (37%) | 3 (5%) | 0 | **OI** |
| 442283 | 07-02 13:56 | AVGO | `..._avgo_opening_drive_avgo_long_shadow_row_11` | shadow | 195 | 164 (84%) | 18 (9%) | 9 (5%) | 4 (2%) | **OI** |
| 465005 | 07-02 16:23 | AVGO | `..._avgo_elastic_band_avgo_long_shadow_row_14` | shadow | 125 | 60 (48%) | 57 (46%) | 7 (6%) | 1 (1%) | OI ≈ **delta_below** (co-dominant) |
| 470570 | 07-02 16:59 | AVGO | same row_14 | shadow | 125 | 60 (48%) | 58 (46%) | 6 (5%) | 1 (1%) | OI ≈ **delta_below** |
| 474007 | 07-02 17:20 | AVGO | same row_14 | shadow | 125 | 60 (48%) | 58 (46%) | 6 (5%) | 1 (1%) | OI ≈ **delta_below** |
| 499111 | 07-06 13:39 | META | `..._meta_short_shadow_row_20` | shadow | 191 | 134 (70%) | 36 (19%) | 19 (10%) | 2 (1%) | **OI** |
| 503444 | 07-06 14:10 | META | same row_20 | shadow | 191 | 134 (70%) | 36 (19%) | 19 (10%) | 2 (1%) | **OI** |
| **557687** | **07-07 14:00** | **SMH** | **`..._smh_short_live_row_6`** | **LIVE** | 266 | 168 (63%) | 55 (21%) | 34 (13%) | 9 (3%) | **OI** |
| **608803** | **07-08 13:39** | **AMD** | **`..._amd_short_live_row_2`** | **LIVE** | 195 | 190 (97%) | 1 (0.5%) | 2 (1%) | 2 (1%) | **OI** |
| 610559 | 07-08 13:52 | AVGO | same row_11 | shadow | 180 | 167 (93%) | 2 (1%) | 9 (5%) | 2 (1%) | **OI** |
| **612183** | **07-08 14:04** | **AMD** | **same row_2** | **LIVE** | 195 | 190 (97%) | 1 (0.5%) | 1 (1%) | 3 (1.5%) | **OI** |
| 617635 | 07-08 14:43 | AMD | `..._amd_short_shadow_row_25` | shadow | 195 | 190 (97%) | 1 (0.5%) | 2 (1%) | 2 (1%) | **OI** |

\* "window" = candidates that already have the right contract type (call/put per direction) and land inside the requested DTE range — i.e., what's left before OI/delta/spread are applied. All 6 lanes shown request `min_open_interest=100` and `max_bid_ask_spread_pct=0.08` (uniform across the whole compiled plan).

**LIVE entries lost: 2 distinct incidents (3 logged events)** — SMH on 07-07 once, AMD on 07-08 twice (same ~25-min window, likely one missed trade re-logged). **Dominant filter: OI in every lane except AVGO's elastic-band row (`row_14`), where OI and `delta_below_min` are roughly tied.**

### 1b. Important measurement limitation — read this before trusting "OI is the fix"

The selector evaluates filters in a **waterfall**: each candidate is bucketed into the *first* filter it fails, in the order **OI → delta_below → delta_above → spread**. Proof: in every one of the 12 events, `delta_below + delta_above + spread` exactly accounts for `window − OI_blocked` (e.g. SMH: 266 − 168 = 98, and 55+34+9 = 98). This means:

- Contracts that fail OI are **never checked against delta or spread** — we have zero visibility into whether they'd also have failed those filters.
- In **every single one of the 12 events**, the entire non-OI-blocked pool *also* fails delta or spread at current thresholds (100% match, to the row). That means the logs show **zero contracts, in any of these 12 events, that would have executed even at OI floor = 0** using the visible (non-OI) population — the only way a lower floor produces a fill is if some of the *OI-blocked* contracts happen to sit inside the delta/spread window, which these aggregate counts cannot confirm or rule out.

**This is the central honesty gap in the ask**: answering "would a lower OI floor have produced a fill" precisely requires the actual per-contract chain at rejection time (strike, OI, delta, spread together), which bhiksha does not log. Section 3 below is the best available proxy and it has its own gap.

**One useful cross-check we do have**: on the same lanes, the operator's own **live fills** show what "successful" contracts look like at the point of entry (from `trade_plan` events, not the rejection logs):

| lane | fill date | contract OI at entry | spread_pct at entry | vs. min_oi=100 / max_spread=8% |
|---|---|---|---|---|
| AMD `live_row_2` | 07-02 | 163 | 7.61% | both filters barely cleared |
| AMD `live_row_2` | 07-08 | 175 | 7.84% | both filters barely cleared |
| SMH `live_row_6` | 07-02 | 6,795 | 7.73% | OI not a constraint that day; spread near the cap |
| NVDA `live_row_7` | 07-02 | 18,203 | 3.75% | neither near a constraint |
| IWM `live_row_3` | 07-06 | 2,203 | 6.62% | neither near a constraint |
| QQQ `live_row_5` | 07-08 | 4,945 | 1.75% | neither near a constraint |

This is a meaningful, if indirect, signal: **AMD's own successful live fills sit right on top of both the OI floor (163, 175 vs. 100 min) and the spread cap (7.6–7.8% vs. 8% max)** — AMD genuinely trades thin/wide options at the strikes this lane wants, on both good and bad days. That's consistent with (but does not prove) OI relaxation helping AMD specifically. SMH's rejection happened on a day the DTE window forced a fallback to 10 DTE (see `available_dtes` in the raw event) with a much larger, thinner candidate pool (266 vs. its typical live-fill environment) — that's a different mechanism (DTE-window/liquidity mismatch on a specific day) than a chronic thin-OI problem.

---

## 2. Live entry sizing since 2026-07-02

### 2a. Data correction (read first)

`trade_sessions.quantity` for trade `cf39b657-defa-4e6f-b9c9-da8c8b86ffcf` (QQQ, `live_row_5`, 07-08) reads **1**, but the authoritative broker-fill events (`trade_plan`, `entry_fill_check`: `"quantity": 3`, `"filledQuantity": "3"`) confirm the real fill was **3 contracts**. This looks like a reconciliation/write-timing bug in the trade_sessions row, not a broker-side partial fill — the fill payload shows a clean single fill at qty 3. **All numbers below use the broker-confirmed quantity for QQQ; flag this row separately for a DB reconciliation check** (out of scope for this read-only pull).

### 2b. All live entries, corrected

| symbol | lane (`live_row_N`) | date | qty | entry price | premium deployed | lane premium cap (today's plan) | cap used | 1-lot? |
|---|---|---|---|---|---|---|---|---|
| SMH | 6 | 07-02 | 1 | $15.20 | **$1,520.00** | $1,800 | 84.4% | **yes** |
| NVDA | 7 | 07-02 | 12 | $1.62 | **$1,944.00** | $2,000 | 97.2% | no |
| AMD | 2 | 07-02 | 1 | $9.40 | **$940.00** | $1,700 | 55.3% | **yes** |
| IWM | 3 | 07-06 | 12 | $0.7698 | **$923.76** | $1,000 | 92.4% | no |
| QQQ | 5 | 07-08 | 3 (corrected) | $2.88 | **$864.00** | $900 | 96.0% | no |
| AMD | 2 | 07-08 | 1 | $10.40 | **$1,040.00** | $1,700 | 61.2% | **yes** |
| | | | | **Total** | **$7,231.76** | | | **3/6 = 50%** |

**Sizing formula confirmed**: across all 6 fills, `quantity = floor(max_trade_premium_usd / (entry_price × 100))` holds exactly (this only became clean once the QQQ quantity was corrected to 3 — at the DB's stale value of 1 it looked like an anomaly; it wasn't). This means the premium cap is the actual quantity-setting lever for every live lane observed — there is no separate liquidity-based sizing throttle visible in these 6 trades.

**can_ladder implication**: the exit profile's `target_1_quantity=0.6` (partial exit 60% at T1, hold 40% to T2) requires at least ~2–3 contracts to execute as a real partial fill. At qty=1 (SMH, both AMD fills — 3 of 6, 50%), **the T1/T2 ladder cannot fire**; the position exits as a single all-or-nothing fill on whichever exit condition triggers first. NVDA (12), IWM (12), and QQQ (3, corrected) can ladder.

**Cap headroom**: SMH, NVDA, IWM, QQQ are already using 84–97% of their lane's cap at their current lot count — raising the cap wouldn't add a lot without also raising it substantially (see 4b). Both AMD fills sit at 55–61% of cap specifically because the *next* lot ($1,880–$2,080) doesn't fit under $1,700 — this is unused cap, not a missing-liquidity problem.

---

## 3. Illustrative chain snapshot — SMH and AMD

**Labeled clearly: sampled now (2026-07-08 16:55 UTC, public/delayed quote source), not at rejection time. Read the gap below before using these numbers for anything quantitative.**

- **SMH: no chain data in kamandal at all** (not in the 71-symbol `chain_snapshots` coverage list). Skipped, no broker API was queried per instructions.
- **AMD: chain data exists, but has a structural (not just timing) gap.** `kamandal`'s `public_chain_AMD_2026-07-08` snapshot only carries expirations at **23/30/37/44/72/100 DTE** (all monthly-cycle). The live rejection events for AMD on the same day recorded `available_dtes: [2, 5, 7, 9, 12, 14]` in the **real broker feed** — weeklies bhiksha actually trades. There is **zero overlap** between the two data sources' expiration ladders. This isn't "the market moved" — kamandal's public source simply doesn't carry the weekly AMD options this lane needs, at any time of day. **Any illustrative numbers below are for the wrong DTE bucket and cannot be used to size the actual live-rejection scenario.**

For qualitative shape only, the nearest available AMD put expiration (2026-07-31, 23 DTE — not the 3–7 DTE the live lane needs) in the delta band 0.15–0.35 abs:

| strike | delta | OI | bid/ask | spread_pct |
|---|---|---|---|---|
| 425 | -0.162 | 424 | 9.80 / 11.00 | 11.5% |
| 430 | -0.174 | 295 | 10.70 / 11.95 | 11.0% |
| 435 | -0.189 | 483 | 11.85 / 13.20 | 10.8% |
| 440 | -0.203 | 889 | 13.00 / 14.30 | 9.5% |
| 445 | -0.218 | 137 | 14.15 / 15.80 | 11.0% |
| 450 | -0.232 | 1,674 | 15.35 / 16.95 | 9.9% |
| 455 | -0.249 | 119 | 16.85 / 18.65 | 10.1% |
| 460 | -0.265 | 323 | 18.30 / 19.95 | 8.6% |
| 465 | -0.283 | 403 | 19.85 / 22.20 | 11.2% |
| 470 | -0.299 | 397 | 21.55 / 23.45 | 8.4% |
| 475 | -0.317 | 222 | 23.35 / 25.45 | 8.6% |
| 480 | -0.335 | 343 | 25.20 / 27.20 | 7.6% |

At this (wrong) expiration: **11 of 12 strikes in the delta band already clear OI ≥ 100** — OI is *not* the visible constraint here. **Only 1 of 12 clears spread ≤ 8%** — the public/delayed quote source's bid-ask is structurally wider than a live NBBO feed, so this can't be read as "spread is the real constraint" either; it's evidence the public source isn't representative of what the live broker sees. This table should be read as "OI is not obviously scarce for AMD at a nearby monthly expiration" and nothing more precise than that — it does not confirm or refute the OI-floor hypothesis for the actual 3–7 DTE weekly window.

---

## 4. Decision table

### 4a. OI floor: 100 (current) vs 50 vs 25 vs 0+spread-gate

| OI floor | measured effect on the 12 logged rejections | estimate quality |
|---|---|---|
| **100 (current)** | 0/12 events traded (that's why they were logged) | measured |
| **50** | Unmeasurable from these logs. Upper bound = same OI-blocked pool (e.g. up to 168 SMH, up to 190×2 AMD contracts move into delta/spread evaluation) — **actual pass count unknown**, see §1b | bounded, not estimated |
| **25** | Same upper bound as above; even more contracts move into evaluation, actual pass count still unknown | bounded, not estimated |
| **0 + spread-gate only** | Removes OI as a filter entirely, keeps `max_bid_ask_spread_pct=0.08`. Same structural gap: cannot confirm any of the 12 rejections would have produced a fill without contract-level data at rejection time | bounded, not estimated |

**What we can say with confidence**: lowering the OI floor cannot make any of the 12 recorded rejections *worse* (it only ever removes a filter), and AMD's own live fills sitting at OI 163/175 (§1b) is real evidence that AMD trades thin contracts near the current floor on good days — so a lower floor is directionally plausible for AMD specifically. It is **not provable, and not quantifiable, from the data pulled here** for any lane. A genuine answer requires either (a) logging the full per-contract chain at rejection time going forward, or (b) a live A/B at floor=50 for one cycle.

### 4b. Premium cap: current vs $2,500 vs $3,000

Using the confirmed formula `quantity = floor(cap / (entry_price × 100))` against the 6 actual fills:

| entry | price/contract | current cap → qty | $2,500 cap → qty | $3,000 cap → qty |
|---|---|---|---|---|
| SMH 07-02 | $1,520 | $1,800 → **1** | **1** (need $3,040 for 2) | **1** (need $3,040 for 2 — $3,000 still $40 short) |
| NVDA 07-02 | $162 | $2,000 → **12** | **15** | **18** |
| AMD 07-02 | $940 | $1,700 → **1** | **2** | **3** |
| IWM 07-06 | $76.98 | $1,000 → **12** | **32** | **38** |
| QQQ 07-08 | $288 | $900 → **3** | **8** | **10** |
| AMD 07-08 | $1,040 | $1,700 → **1** | **2** | **2** |

**Of the 3 actual 1-lot entries: raising the cap to $2,500 upgrades 2 of 3 (both AMD fills) to 2-lot; SMH stays 1-lot at both $2,500 and $3,000** (its per-contract price is high enough that it needs >$3,040 for a second lot — a uniform cap bump doesn't fix SMH, only AMD). This is measured arithmetic on real fills, not modeled.

### 4c. Risk-rail arithmetic

Confirmed live config (`risk_manager_startup` event, most recent): `rail_a_enabled=true`, **tier-1 halt = 5.0%** (stop new entries), **tier-2 flatten = 7.5%** (close everything), against today's actual `usable_budget = $10,236.94` (from `cash_budget_days`, 2026-07-08; the "~$10.5k" framing in the ask is a close approximation of this real number).

Worst single stop-out = `stop_loss_pct (0.35) × cap`. Rail dollar values computed off the real $10,236.94 budget:

| rail pair | tier-1 halt ($) | tier-2 flatten ($) |
|---|---|---|
| 5% / 7.5% (current, confirmed live) | $511.85 | $767.77 |
| 7.5% / 11% | $767.77 | $1,126.06 |
| 10% / 15% | $1,023.69 | $1,535.54 |

Worst-case single stop-out by cap, and what it means against each rail pair:

| cap | worst stop (0.35×cap) | vs 5%/7.5% | vs 7.5%/11% | vs 10%/15% |
|---|---|---|---|---|
| $900 (QQQ, current) | $315 | survives one (2nd halts) | **survives two** | **survives two** |
| $1,000 (IWM, current) | $350 | survives one (2nd halts) | **survives two** | **survives two** |
| $1,700 (AMD, current) | $595 | **one loss halts the day** | survives one (2nd halts) | **survives two** |
| $1,800 (SMH, current) | $630 | **one loss halts the day** | survives one (2nd halts) | **survives two** |
| $2,000 (NVDA, current) | $700 | **one loss halts the day** | survives one (2nd halts) | **survives two** |
| $2,500 (proposed uniform) | $875 | **one loss halts the day — and also crosses the 7.5% flatten tier solo** | **one loss halts the day** | survives one (2nd halts) |
| $3,000 (proposed uniform) | $1,050 | **one loss halts the day — and also crosses the 7.5% flatten tier solo** | **one loss halts the day** | **one loss halts the day** (barely: $1,050 vs $1,023.69) |

**Reading this**: at the *current* per-lane caps and the *current* 5%/7.5% rail, AMD/SMH/NVDA's lanes already sit in "one loss halts the day" territory on a single worst-case stop — only QQQ and IWM's smaller caps currently survive a first loss. **Raising the cap to $2,500 or $3,000 uniformly, without also loosening the rail pair, pushes every live lane into "one loss halts the day," and at $2,500–$3,000 a single stop-out on the widened cap can also trip the 7.5% flatten tier by itself** — i.e., one bad AMD/SMH-style entry at a $2,500+ cap could end the trading day entirely, not just pause new entries. Loosening the rail to 7.5%/11% or 10%/15% is what actually buys room for a bigger cap to survive a first loss; a cap increase alone tightens the effective risk budget.

---

## 5. Today's actual per-lane premium caps (from the compiled plan, 2026-07-08)

| deployment | symbol | live/shadow | max_trade_premium_usd | min_open_interest | max_bid_ask_spread_pct | dte window | delta window |
|---|---|---|---|---|---|---|---|
| `strategy_expand30_amd_mi_01_amd_short_live_row_2` | AMD | **live** | $1,700 | 100 | 8% | 3–7 | 0.15–0.35 |
| `strategy_market_impulse_all_basket_discovery_iwm_long_live_row_3` | IWM | **live** | $1,000 | 100 | 8% | 3–7 | 0.15–0.35 |
| `strategy_market_impulse_all_basket_discovery_qqq_short_live_row_5` | QQQ | **live** | $900 | 100 | 8% | 0–3 | 0.15–0.35 |
| `strategy_expand30_w1_b1_p3_market_impulse_smh_smh_short_live_row_6` | SMH | **live** | $1,800 | 100 | 8% | 3–7 | 0.15–0.35 |
| `strategy_opening_drive_current_basket_discovery_nvda_short_live_row_7` | NVDA | **live** | $2,000 | 100 | 8% | 7–14 | 0.15–0.35 |
| all 13 shadow lanes | various | shadow | $500 (default) | 100 | 8% | varies | 0.15–0.35 |
| `..._mu_market_impulse_mu_long_shadow_row_4` | MU | shadow | **$4,500** (outlier override) | 100 | 8% | 7–14 | 0.15–0.35 |

`min_open_interest=100` and `max_bid_ask_spread_pct=0.08` are uniform across **all 19 deployments** — there is no per-lane OI/spread override today; any policy change at that level is plan-wide unless the operator adds per-lane overrides.

---

## Caveats (read before deciding)

1. **QQQ live fill quantity was corrected from 1 to 3** using broker-confirmed `trade_plan`/`entry_fill_check` events — the `trade_sessions.quantity` column disagreed with the actual fill for this one row. Worth a separate reconciliation check; not fixed here (read-only pull).
2. **§1b is the load-bearing caveat for the whole OI-floor question**: the selector's waterfall filter order means the aggregate rejection logs cannot show whether a lower OI floor would have produced a fill, only that OI is the largest *counted* bucket. Treat all OI-floor conclusions as directional, not quantified.
3. **§3's illustrative chain data has a structural DTE-coverage gap** (public source carries only monthly expirations; the live lanes trade weeklies) on top of being sampled well after the actual rejection times. It is not usable to size the actual missed-entry scenarios — included only for qualitative shape, and flagged inline everywhere it's used.
4. **Only 2 distinct LIVE selector-rejection incidents exist since 07-02** (SMH once, AMD twice as likely-one-incident) — this is a very small sample. Any policy chosen off this data should be treated as a hypothesis to validate over the next 1–2 weeks of live rejections, not a settled conclusion.
5. All dollar/percentage rail math in §4c uses today's (07-08) actual `usable_budget=$10,236.94`; this number moves day to day with realized P&L and will shift the exact dollar thresholds even though the underlying logic holds.
