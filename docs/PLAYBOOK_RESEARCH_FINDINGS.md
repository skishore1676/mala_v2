# Playbook Discovery — Findings & How We Leverage Them

> Plain-language synthesis of the distill-the-operator research program
> (2026-07-03 → 2026-07-11). Companions: `PLAYBOOK_DISCOVERY_PROGRAM.md` (the
> gate board / spec), `EXIT_PROFILE_PLAYBOOKS.md` (the playbook×profile spec),
> `LIVE_LOOP_WORKPLAN.md` (dated diary). This doc is the "what we learned and
> what we do with it" layer.

## Why we did this

The live automated book trades only ONE of the operator's four playbooks
(trend continuation). Flash reversal, exhaustion reversal, and range expansion
— the operator's actual bread and butter — had **no strategies in the loop at
all**. Rather than invent strategies and hope they match how he trades, we
inverted the usual discovery: **start from his own 4 years of fills, learn what
he actually does, teach a machine to see the same setups, then test whether the
mechanical version makes money.**

The whole program is one question decomposed into gates: *can we mechanize the
operator's edge, honestly?* Each gate below either advanced that or told us
exactly where the edge refuses to mechanize — both are useful.

---

## What we found, gate by gate

### P0 — Spec-lock: his playbooks, made precise
We turned the 4 playbooks from prose into taggable definitions, corrected by the
operator himself (via phone review). The material corrections mattered:
- **Exhaustion** = a symbol stretched to its ~85–90th percentile, faded when it
  "can't cross the prior extreme easily" — *symbol-agnostic, any timeframe.*
- **Trend continuation** anchors on the **10-period VMA**, not VWAP.
- **Range expansion** includes **earnings-gap continuation**, not just
  rectangle/flag breakouts.
- **Flash** is explicitly **counter-trend** (fading a violent flush against the
  recent move), and doubling-down into it is part of the play.

### P1 — Tagged history: he is a reversal trader
1,443 timestamped IWM/SPY fills → **390 trade episodes**, each labeled by
playbook, corrected by the operator across two review rounds until the machine
agreed with his eye **94% of the time on confident calls**. The distribution is
the headline:

| Playbook | Episodes | Read |
|---|---|---|
| Flash reversal | 142 | his base mode |
| Exhaustion reversal | 117 | his second instrument |
| Trend continuation | 51 | the one already live |
| None-of-the-four | ~80 (20%) | "bored / Fed-event" trades — where losses cluster |

The machine's first instinct was trend-heavy; his corrections revealed the
truth — **reversals dominate**, and the money-losing trades are the ones that
fit no playbook (his own written rule: "not worth it if you cannot follow
rules").

### P2 — Detectors: two of the three playbooks mechanize cleanly
Can a mechanical rule fire *where he fired* without firing all day? Tested by
sweeping every 5 minutes of every IWM/SPY session for a year:
- **Flash — YES.** "A ≥0.15-ATR flush into a fresh extreme within 15 minutes"
  fires at his flash entries **~7× more than at random**, ~4 alerts/day.
- **Trend — YES.** "Established trend + an active, stalling pullback" — **~9×**
  selectivity once we required the pullback to be real (not just "near the VMA").
- **Exhaustion — NOT YET (honest fork).** The *context* (a market that ran too
  far) is easy to detect but fires half of all bars; his *entry timing* varies
  too much across only 5 clean examples to pin mechanically. Recorded as "eye
  not yet captured," never as "no edge."

### P3 — Backtest: the edge is real but thin, and selection is the missing layer
Ran the detectors over **five years** of IWM/SPY (2021–2026, ~48k fires) as real
option trades with his native exit profiles, IV-banded, cost-haircut, across
three market regimes. The result is the most important finding of the program:

- **The gross edge is real and broad**: +0.5% to +3.4% per trade before costs,
  positive in **44 of 48** regime/detector cells. His setups genuinely tilt the
  odds.
- **But taking every fire loses money.** The detectors fire ~12×/day; he takes
  1–2. Spread across all fires, the thin edge dies under transaction costs.
- **The edge concentrates exactly where he'd concentrate.** Picking the
  strongest fire per day *doubled* the gross edge — but that filter uses
  hindsight (you can't know at 10:00 that a stronger fire comes at 14:00). Every
  honest, live-executable filter gives most of that back.

**The named finding — the "selection gap":** there is a **2–4%-per-trade gap**
between what the mechanical setups earn and what his real trading earns, and
**that gap is his discretionary choice of which fires to take.** We measured the
thing that was always folklore: *the setups are mechanizable; his picking wasn't
— yet.*

---

## The strategic reframe

A trader doesn't pick "better setups" OR "better selection" OR "cheaper
execution" — they're one P&L stack and they **multiply**:

```
  SETUP            SELECTION              EXECUTION
  (detectors)   ×  (his fire-picking)  −  (costs)
  +1.5–3%/trade    +2–4%/trade            −2–4%/trade
```

P3 proved each term exists and roughly sized it. No single term clears the bar
alone. So the plan attacks all three at once, wired so each feeds the next — the
**Flywheel**.

---

## The Flywheel — how we leverage the research

### Phase A — turn the research into a daily habit *(BUILT 2026-07-11)*
The detectors become a **daily Telegram consultation feed** (`flywheel_daily.py`):
FLASH/EXHAUSTION/TREND fires on IWM/SPY, each morning — **signals only, no orders,
no money surface.** Underneath, two quiet engines do the real work:
- **Fire ledger**: every fire logged with features + its realized option-path
  outcome computed each evening using **real same-day IV** (kamandal) — live
  evidence now replaces modeled backtests.
- **Zero-effort labeling**: his manual fills are auto-matched to fires (a fill
  within ±10 min of a fire = "took it"). *His normal trading becomes the
  training data — he journals nothing.*

The card he sees applies **his own thresholds** (exhaustion ≥ p85, flash ≥ 0.20
ATR, drop whipsaw pairs) so it's trader-credible, while the ledger keeps every
fire for learning.

**A-phase findings (honest):**
- **Coverage: his flash entries land on detector fires 93% of the time** — the
  flash feed is trustworthy. Book-wide coverage is 61–70% (dragged down by
  exhaustion's fork and trend's narrow detector). Non-playbook trades match 0%,
  which is correct.
- **He takes ~1 fire in 33 (3%)** — the selection gap, now quantified as a rate.

### Phase B — learn his selection *(done — and it redirected the program)*
Mine the take/skip data for what separates the fires he takes:
- **We CAN predict his clicks.** The dominant selector is time-of-day — he takes
  early-session fires (earliest quartile taken 8% of the time, latest 1%),
  strength secondary. Out-of-sample a simple scorer cleanly separates the fires
  he'd take (8.6%) from the ones he'd skip (0.3%).
- **But predicting his clicks does NOT make money.** The economic test — does
  taking his high-score fires beat his low-score fires on the option path? — came
  back **negative**: top-selection-quartile net −2.8%/trade vs bottom −1.3%, a
  **−1.5%** separation where we needed +2%. Selecting like him made the
  mechanical outcome *worse*, not better.
- **The one exception is the tell: TREND.** Trend's top-quartile beats its bottom
  by **+1.0%/trade** — his trend selection *does* help mechanically. Flash and
  exhaustion (the fast plays) are where selecting like him hurts (−0.7%, −3.0%).

**What this means (the real finding):** his edge on the fast plays is **not in
WHICH setup he picks — it's in the parts the backtest strips out: his entry
execution and his live exit management.** Three mutually-reinforcing reasons,
all pointing the same way:
1. He legs into better-than-mid prices; the sim enters at mid.
2. His real hold is a **28-minute median scalp**; the mechanical profile runs a
   1R–2R ladder out to 90 minutes — it holds his fast plays too long.
3. Early-session option-buying faces a real open-IV / theta headwind that a
   *hold* can't beat but a *fast scalp* does.

Trend is the exception because it's the slowest, least execution-sensitive play
— and, not coincidentally, the one already live and working. **Conclusion: do
NOT try to mechanize the fast plays into autonomous lanes. The detector surfaces
the candidate; the human supplies the selection + execution + fast exit — and
that human layer is exactly what we now measure.**

### Phase C — measure the human layer *(next — redirected by the B finding)*
The economic result moved the target. The 2–4% gap isn't in setup selection;
it's in execution and exit timing. So Phase C measures exactly those, from his
own fills:
- **Entry-execution alpha**: his real fill price vs the bar's mid/VWAP at his
  entry minute — how many % better than mid does he leg in? (Measurable from his
  fills + the bar cache, no new infra.)
- **Exit-timing alpha**: his real hold + exit vs what the fixed profile would
  have done on the same trade — is his 28-min scalp beating the 90-min ladder?
  This loops straight back to the **original exit-profile program**: the FLASH
  profile dials may be mis-calibrated to how fast he actually trades.
- **Real costs**: replace the 2–4% guess with bhiksha's measured passive/urgent
  fill quality per symbol × DTE.
- **Habitat** (still worthwhile): exhaustion on single names, range on semis —
  where the IWM/SPY corpus never looked.

### The exit condition (revised by the evidence)
Two honest roads out, and the evidence now favors the first:
1. **Consultation stays the product.** The detectors are a genuinely good
   *candidate surface* (flash coverage 93%); the human supplies the alpha the
   backtest can't. We deploy the feed, measure his execution/exit alpha, and the
   research succeeds by making his discretionary trading faster and better-armed
   — not by replacing it.
2. **A mechanical lane still ships IF** Phase C recalibrates a profile (e.g. a
   fast-scalp FLASH exit) that clears the P3 bar out-of-sample on his real
   hold/exit behavior — then it goes to a bhiksha shadow lane with that
   recalibrated native profile. Trend's +1.0% says the slower plays are the
   likeliest first mechanical candidates.

Either way the consultation feed is the live surface now, and the flywheel banks
execution + exit data every trading day — which is what Phase C needs.

---

## Honest caveats (so we don't fool ourselves)

- **Sample size**: the selection analysis rests on 260 taken fires (IWM/SPY
  only). Directional, not final — Phase A grows it daily.
- **Modeled vs real IV**: historical backtests use modeled IV; the going-forward
  ledger uses real kamandal IV, so evidence quality improves from here.
- **Exhaustion is an open fork** — carried as a flagged screen, not a validated
  detector. More labeled examples (from the live feed) are the fix.
- **Coverage below 90%** book-wide means the detectors don't yet see all his
  entries — a real limit, fed into Phase B/C rather than smoothed over.

## The one-line version

*His setups are real and mechanizable; but his 2–4%/trade edge over the raw
setups is NOT setup-selection (mechanizing that made it worse) — it's execution
and fast exit management that a fixed-profile backtest can't see. So the research
pays off as a consultation tool that arms his discretion, plus a measurement of
his execution/exit alpha that may recalibrate the exit profiles — with the slow
plays (trend) the likeliest to mechanize first.*

## What changed our mind (the honest turn)

We started the Flywheel believing: *learn which fires he takes → filter the
detector → mechanical profit.* Phase B disproved the last arrow. Predicting his
clicks worked (8.6% vs 0.3% OOS); turning that into mechanical P&L failed
(−1.5%/trade). The negative result is more valuable than a marginal pass would
have been — it tells us the edge is human (execution + fast exits), redirects
the build toward measuring that, and stops us from shipping an autonomous
flash/exhaustion lane that would have quietly lost money. The yardstick was
right (option-path, cost-aware, out-of-sample, lookahead-audited) — no repeat of
the old M1 mis-kill.

---

## Where the artifacts live (for the next session / agent)

| What | Path |
|---|---|
| Detector + tagger logic | `src/research/playbook_tagging.py` |
| P2 detector scorecard | `scripts/p2_detector_scorecard.py` |
| P3 backtest + selection | `scripts/p3_option_path_backtest.py`, `p3_selection_rerun.py` |
| Flywheel daily feed + ledger | `scripts/flywheel_daily.py` |
| Fill↔fire matching (A3) | `scripts/flywheel_fill_match.py` |
| Selection mining (B1) | `scripts/flywheel_selection_mine.py`, `flywheel_selection_economic.py` |
| Frozen tagged corpus | `data/personal_imports/tagged/round_trips_tagged_FROZEN.csv` (gitignored) |
| Take/skip training set | `data/personal_imports/tagged/flywheel_take_skip.csv` (gitignored) |
| Reports (coverage, selection, backtest) | `data/personal_imports/tagged/*_report.md`, `*_coverage.md` |
