# P0 Spec-Lock Questionnaire — the 4 playbooks, taggable

> **STATUS: COLLECTED + FOLDED 2026-07-04.** Operator reviewed via Lathi bus (decision: revise,
> 5 comments). The corrected spec now lives in `EXIT_PROFILE_PLAYBOOKS.md` § "P0 Spec-Lock" — that
> section is authoritative; this file remains as the question record.

> Gate P0 of `docs/PLAYBOOK_DISCOVERY_PROGRAM.md`. Every answer below is **PROPOSED by Claude** from
> `docs/EXIT_PROFILE_PLAYBOOKS.md`, the profile dials, and your fill fingerprints — **none of it is
> your word yet**. Correct inline with pointy brackets (`<like this>`) — a bare "ok" on a question
> accepts the proposal. Numbers are anchors to react to, not precision claims; ranges and "it
> depends, e.g. …" answers are welcome. When this comes back, the corrected version becomes the
> spec section in `EXIT_PROFILE_PLAYBOOKS.md` and every P1 tagging rule will cite it.
>
> Time budget: ~1 hour. 4 playbooks × ~5 questions + 5 cross-cutting.

---

## 1 · FLASH_REVERSAL — "a violent flash looks over-stretched"

**F1. What qualifies as a "violent flash"?**
PROPOSED: a mostly one-directional move of ≥ ~2.5× the normal 15-minute range (time-of-day
adjusted), completing in ≤ 15 minutes — e.g. IWM/SPY ≥ ~0.35%, TSLA/NVDA ≥ ~1.0% in that window —
usually with a volume spike ≥ ~2.5× the norm for that time of day and a run of 4+ same-direction
1-min closes.

**F2. "Over-stretched" is measured against what reference?**
PROPOSED: primarily distance from VWAP — roughly ≥ 1.5× the day's ATR beyond it — with the flush's
own launch point as a secondary reference. (Alternative framings to pick from: distance from
opening range; z-score vs a rolling window; "it's the speed, not the distance".)

**F3. Entry trigger — "clear reversal break, lean early" means exactly what?**
PROPOSED: after the extreme prints, first 1-min close back through the extreme bar's midpoint (or a
micro-trendline break of the flush), entered within ~10 minutes of the extreme; target direction =
back toward VWAP.

**F4. Invalidation — "no bounce shortly after": how short is shortly?**
PROPOSED: if the prior flush extreme is re-breached, out immediately; if there's simply no bounce
within ~10–15 minutes, also out (consistent with the profile's 15-min no-progress stop).

**F5. Time-of-day limits?**
PROPOSED: any time of session, INCLUDING the open and the last hour, but NOT the first 3 minutes
after 08:30 CT (too disorderly to define "stretched").

---

## 2 · EXHAUSTION_REVERSAL — "a mature, unsupported extension"

**E1. What makes an extension "mature / unsupported"?**
PROPOSED: ≥ ~60–90 minutes of directional drift, stretched ≥ ~1.5–2.0 day-ATRs from VWAP (or from
session open), where the later legs show fading participation — smaller pushes, declining volume,
momentum divergence.

**E2. "Clear break + more patience" — what do you wait for that FLASH doesn't?**
PROPOSED: a failed new extreme (e.g. lower high after an up-extension) PLUS a break of the last
small consolidation — not just the first counter-move bar.

**E3. Invalidation — "acceptance beyond the reference": what counts as acceptance?**
PROPOSED: ~2 consecutive 5-minute closes beyond the prior extreme (vs a single wick, which is
tolerable).

**E4. Is EXHAUSTION index-first by design?**
PROPOSED: yes — IWM/SPY primary (matches both your fills — 89% of timestamped trades are IWM/SPY —
and the June option-path evidence: worked on IWM, failed on META). Single-name exhaustion is
allowed but secondary.

**E5. The FLASH ↔ EXHAUSTION boundary (the tagger's hardest cut).**
PROPOSED: it's the *age and speed* of the move being faded — extreme printed within the last
~15 minutes after a fast flush → FLASH; a grind that's been building ≥ ~45 minutes → EXHAUSTION;
in between, whichever the entry behavior matches (immediate fade vs waiting for a failed retest).

---

## 3 · TREND_CONTINUATION — "an established trend resumes after a pullback"

**T1. What makes a trend "established" intraday?**
PROPOSED: price persistently on one side of a rising/falling VWAP for ≥ ~60 minutes (or cleanly
beyond the opening range for most of the session), higher-lows/lower-highs structure intact.

**T2. How deep can a "pullback" go before it's no longer a pullback?**
PROPOSED: retraces ≤ ~50% of the prior leg and may TOUCH VWAP but not CLOSE through it (5-min
basis).

**T3. "Anticipate — enter slightly before the reclaim": how early, on what tell?**
PROPOSED: when the pullback visibly stalls (1-min basing / drying volume) within ~0.25 ATR of the
reclaim level — entry before the reclaim actually prints.

**T4. Invalidation — "pullback becomes a structure break": which structure?**
PROPOSED: close through the prior higher-low (or VWAP with acceptance), i.e. the same "re-breach of
the prior extreme" rule applied to the pullback's protected low/high.

**T5. The TREND ↔ RANGE boundary.**
PROPOSED: if a directional trend was established earlier the SAME session, a coil is a pullback →
TREND; if the session never established direction (day range compressed vs normal) and price is
coiling near a range edge → RANGE.

---

## 4 · RANGE_EXPANSION — "a stabilized base starts a larger move"

**R1. What is a "stabilized base / coil", measurably?**
PROPOSED: ≥ ~90 minutes (intraday) or 2+ days (swing) of contracting range — width in the bottom
~30% vs recent history — with declining volume inside the base.

**R2. Entry "into the coil before the break": positioned how?**
PROPOSED: entered inside the base near the edge in the anticipated break direction, direction
chosen from higher-timeframe bias (prior trend / daily structure), NOT chasing the breakout bar
itself.

**R3. Invalidation — "close back into the base": on what timeframe?**
PROPOSED: a 15-minute close back inside the base after the expansion starts (or the base's far edge
re-breached = full stop).

**R4. Is RANGE the deliberate overnight/multi-day play?**
PROPOSED: yes — this is your ~1% multi-day tail: eod_flat does NOT apply by design, longer DTE,
LOOSE giveback, sized smaller (max cap% lowest of the four). Confirm this is intentional, since it
makes RANGE the only profile that can hold overnight.

**R5. Universe: semis-heavy per your table (TSLA, NVDA, AVGO, ARM, SMH)?**
PROPOSED: confirmed as listed — notably the one playbook where IWM/SPY are NOT primary.

---

## 5 · Cross-cutting (tagging mechanics)

**X1. Short-premium trades** (22 of 1,465 timestamped): park them — excluded from tagging?
PROPOSED: yes, park.

**X2. Scale-ins.** When you add to a thesis, the export shows multiple round-trips. Tag each fill
separately, or cluster same-symbol/same-direction entries within ~10 minutes into one "episode" and
tag the episode?
PROPOSED: cluster into episodes (closer to how you actually traded the thesis).

**X3. A fifth bucket.** Some trades will be none of the four (news scalps, earnings lotto, hedges,
boredom). OK for the tagger to output OTHER/UNCLASSIFIED rather than force-fit?
PROPOSED: yes — and if you can guess what share of your trades are "none of the four", say it
(anchors the ≤25% unclassified target). <your guess: __%>

**X4. Direction inference.** Long call = bullish thesis, long put = bearish. Any systematic
exceptions in your history (e.g. puts as hedges on longs held elsewhere)?
PROPOSED: no exceptions — treat right+side as thesis direction.

**X5. Regime awareness.** Do any playbooks switch off on certain days by rule (FOMC, CPI, OPEX,
earnings weeks on single names)? Not blocking for tagging — but if yes, it becomes a P2 feature.
PROPOSED: earnings-day single-name trades are their own animal (likely X3 bucket); index playbooks
run on all days including event days.

---

*When corrected: Claude folds this into `EXIT_PROFILE_PLAYBOOKS.md` as the spec-lock section,
gate P0 closes, and P1 tagging starts with every rule citing your words.*
