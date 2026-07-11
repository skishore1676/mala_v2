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

---

## P0 Spec-Lock — operator-corrected playbook definitions (2026-07-04)

> Gate P0 of `docs/PLAYBOOK_DISCOVERY_PROGRAM.md`, **CLOSED 2026-07-04**. Source: the
> questionnaire (`docs/PLAYBOOK_SPEC_LOCK_QUESTIONNAIRE.md`) reviewed by the operator via Lathi bus
> (decision: revise, 5 comments), plus his handwritten rules
> (`data/personal_imports/operator_notes/Rules_2026-07-04.pdf`, local-only) as **context, not
> spec**. Every P1 tagging rule cites this section. Items marked *(default)* are questionnaire
> proposals the operator left uncorrected — accepted as working defaults, lower authority than his
> quoted words. Two open confirmations at the bottom; neither blocks P1.

### FLASH_REVERSAL — operator: "makes sense, not much to add"

- Violent flash *(default)*: mostly one-directional move ≥ ~2.5× the normal (time-of-day-adjusted)
  15-min range completing in ≤ ~15 min, usually with volume ≥ ~2.5× the norm and a run of 4+
  same-direction 1-min closes.
- **Operator's word: the flash is AGAINST the recent trend** (a counter-trend flush — e.g. the
  flash sale inside an uptrend). This is a tagging feature, not decoration.
- Stretch reference *(default)*: ≥ ~1.5 day-ATR beyond VWAP; the flush's launch point secondary.
- Entry *(default)*: first 1-min close back through the extreme bar's midpoint (or micro-trendline
  break), within ~10 min of the extreme print; fade toward VWAP.
- **Operator's word: doubling down into the flash is part of the play** (cost-basis averaging),
  BUT no greed — "take what you got… time is not a friend here." Consequences: (1) scale-in adds
  are one episode, not separate trades (see X2); (2) validates STRICT giveback + 15-min
  no-progress dials.
- Invalidation *(default)*: prior flush extreme re-breached → out immediately; no bounce within
  ~10–15 min → out.
- Time-of-day *(default)*: any time incl. open and last hour; excl. first 3 min after 08:30 CT.

### EXHAUSTION_REVERSAL — operator REFRAMED (supersedes the proposals)

- **Operator's word: "very mathematical" — the stretch sits at the ~85–90th PERCENTILE for that
  symbol (vs its own history), then it's a probability play — wait and let it play out.**
- **Symbol-AGNOSTIC: "I do not even care what symbol as long as it's really stretched."**
  (Supersedes the proposed index-first restriction. Tension to TEST in P3: June option-path
  evidence said index-yes/single-name-no — but that judged the automated elastic-band, not this
  definition.)
- **Timeframe-flexible: works on multi-day bars OR intraday 5-min / 1-min.**
- **Entry signal (his word): the move gets "met with jerk" and CANNOT cross the previous high/low
  easily — the inability to cross the prior extreme IS the entry.** (Failed retest, not first
  counter-bar.)
- Invalidation *(default, consistent with his "acceptance" language)*: ~2 consecutive 5-min closes
  beyond the prior extreme = acceptance = out.
- **FLASH ↔ EXHAUSTION boundary (rewritten from his comments):** FLASH = fast, fresh flush
  *against* the recent trend, faded quickly toward VWAP; EXHAUSTION = percentile-extreme stretch
  (any speed, any age, any symbol) + a failed retest of the extreme. Age/speed of the move is now a
  secondary signal, not the primary cut.

### TREND_CONTINUATION — operator adjusted the anchor

- **Operator's word: symbol clearly in a trend WITH RELATIVE STRENGTH; the pullback tell is a
  touch of the 10 VMA** (or the mirror in a bearish move). **The anchor is the 10-period moving
  average, NOT VWAP** as proposed.
- **His word: "when we know we are in a bear or a bull market for the underlying, the first move
  is not the final move — it presents multiple opportunities to ride."** (Regime awareness is part
  of the thesis; also matches his handwritten two-touch W-play: first touch small, second touch
  big, with stop.)
- **Invalidation (his word): the reversal attempt HOLDS in your timeframe — a couple of tests at
  the last low/high range and price "never runs from it"** → thesis dead.
- Anticipatory entry *(default)*: enter as the pullback visibly stalls near the reclaim level,
  before the reclaim prints.
- Established-trend / RANGE boundary *(default)*: trend established earlier the same session →
  coil is a pullback (TREND); directionless compressed session with a coil at the range edge →
  RANGE.

### RANGE_EXPANSION — operator extended the scope

- **Operator's word: typically a breakout from a HORIZONTAL RECTANGLE or a NARROWING FLAG, plus —
  "price/earnings gap continue to run forward."** So TWO sub-cases: (a) pattern-base breakout,
  (b) post-earnings/news gap continuation. Sub-case (b) widens the universe beyond the semis list
  to any name with a strong gap.
- Base definition *(default)*: ≥ ~90 min (intraday) or 2+ days (swing) of contracting range, width
  in the bottom ~30% vs recent history, declining volume inside.
- Entry *(default)*: inside the base near the edge, direction from higher-timeframe bias, not
  chasing the breakout bar.
- Invalidation *(default)*: 15-min close back inside the base; far edge re-breached = full stop.

### Cross-cutting (tagging mechanics)

- X1 *(default)*: the 22 short-premium trades are parked (excluded).
- X2 *(default, now REQUIRED by the FLASH doubling-down rule)*: same-symbol/same-direction entries
  within ~10 min cluster into one episode; episodes are the tagging unit.
- X4 *(default)*: long call = bullish, long put = bearish; no systematic exceptions.
- X5 *(default)*: index playbooks run on all days; single-name earnings-day trades likely fall in
  the OTHER bucket.

### Operator's handwritten rules (context, NOT spec)

Distilled from `Rules_2026-07-04.pdf` — his manual thought process. Not build targets; use as
feature/dial hypotheses:

- **Regime conditions the reversal plays**: bull → flash SALES are the aggressive fade ("do not
  get in front" of squeezes); bear → flash moves UP are the opportunity; contrarian in bear = low
  probability. → P2 candidate feature: FLASH direction conditioned on market regime.
- **High-IV squeeze regime**: contrarians get killed when IV is already high and the move is
  squeezing. → P2 candidate gating feature (IV percentile).
- "Top and bottom prediction are low-probability trades given the length of trends" — his own
  prior on reversals: small size, quick profits, unwind fast.
- **Contrarian anatomy**: theta+delta decay punish hesitation; "risk-return on contrarians with
  less DTE is not worth it"; keep size low. → **Open dial question for P3**: the FLASH profile
  prefers DTE 1–2, his note cautions against short-DTE contrarians — test DTE sensitivity on the
  option path.
- **Pattern failure is a bigger signal on the next path of least resistance** (ARM/SMCI Apr-24).
  → P2 candidate trigger variant for the reversal playbooks.
- Discipline rules (automation targets, already the loop's spirit): stops automated (OCO) within
  10 min of entry; premarket outsize loss → if the market doesn't move your way in 15 seconds,
  take the loss; cooling period +15% = 1 BD / −10% = 2 BD; size ≤33%, max loss 10% of position
  budget.
- **"Morning Chores"** (market what-ifs bull/bear/tussle, two big themes, bucket-stock patterns,
  day-bias vs LT-bias, outsize-move anticipation, overnight-vs-day intent) = the draft input spec
  for workplan item #4 (morning bias overlay).
- Unique days (2024-08-05 VIX-60 Nikkei crash reversal; 2025-04-09 tariff-pause SPX +10% in
  30 min): only anticipated/resting orders could work → scenario planning, not reaction.

### Adjudication round-1 amendments (2026-07-11, operator's words from the card pass)

- **EXHAUSTION (operator's phrase, used on 8 cards): "was running in one direction without taking
  a breath."** The signature is run persistence at ANY scale — intraday leg, all-day grind, or the
  prior 3–5 sessions (near-open exhaustion entries fade overnight/multi-day runs). This extends
  E1' beyond the percentile-stretch framing.
- **FLASH entries often print AFTER the turn starts** (consistent with P0 "reversals: wait for the
  clear break") — so at entry the last leg may already point the operator's way; the faded flush
  is the leg before it. Detection must be thesis-aware (a call fades the drop into a recent low).
- **FLASH lives inside bigger runs**: several confirmed flash fades sat within multi-day slides —
  a bigger-scale run does NOT automatically make the trade exhaustion.
- **Known collision**: a small dip to the 10-VMA in a trend is feature-identical to a weak flash
  fade; structure (established trend + shallow pullback) decides, and only OUTSIZED flushes
  outrank trend structure. The precise FLASH↔EXHAUSTION boundary is round-2's targeted question.
- **RANGE confirmed rare**: 0 of 8 machine-RANGE cards survived; compression alone never tags.
- **OTHER is real**: Fed-event and "boredom" trades ≈ 23% of commented cards — near the ≤25%
  unclassified target (partially answers X3).

### Open confirmations (non-blocking)

1. **R4 — is RANGE deliberately the overnight/multi-day play** (eod_flat No, 5-day max hold, the
   only profile allowed to hold overnight)? Dials table says yes; operator hasn't said the word.
   (Re-asked in adjudication round 2.)
2. **X3 — operator's guess at the "none of the four" share** of his own trades (left blank);
   ≤25% unclassified stays the working target. (Re-asked in adjudication round 2.)
3. **FLASH↔EXHAUSTION boundary in the operator's words** when a sharp drop is also part of a
   bigger run — asked directly in adjudication round 2.
