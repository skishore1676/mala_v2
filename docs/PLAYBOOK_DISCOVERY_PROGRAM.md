# Playbook Discovery Program — entry hypotheses for all 4 exit profiles

> Working document. Born 2026-07-03 (operator direction, day 2 of the live profile-exit month).
> Gate table first; detail sections below. Companions: `docs/EXIT_PROFILE_PLAYBOOKS.md` (the
> playbook×profile taxonomy — the hypotheses this program refines), `docs/LIVE_LOOP_WORKPLAN.md`
> (the live sprint; item #18 points here), `docs/STRATEGY_LANE_V2_S_GATES.md` (S-gate draft this
> program operationalizes for entry discovery).

## Why this program exists

Verified on oldmac 2026-07-03: **17 of the 19 deployed lanes are TREND_CONTINUATION; the other 2
carry no profile at all.** The month-long live test can only ever produce evidence about one of the
operator's four exit profiles. The cause is structural — profile is bound to strategy family
(`src/research/exit_profiles.py: PROFILE_BY_STRATEGY`) and the funnel only delivered trend/momentum
families:

| Profile | Carrier family | Where it died | Blocker type |
|---|---|---|---|
| TREND_CONTINUATION | market_impulse, opening_drive | — LIVE | — |
| EXHAUSTION_REVERSAL | elastic_band_reversion | mala triage KILL (`option_not_tradeable`, `provider_translation_blocked`) | tradability/plumbing — bhiksha already supports the family |
| RANGE_EXPANSION | compression_expansion | `bhiksha_ready=FALSE` (9 rows) | engineering — `runtime_adapter_not_implemented` (workplan #8) |
| FLASH_REVERSAL | (none — intraday_mean_reversion killed at M1) | wrong yardstick (the known mis-kill) | **no carrier exists; operator's base mode** |

Cross-profile counterfactual replay on trend entries was considered and **rejected by the operator
(2026-07-03, endorsed)**: profiles are thesis-coupled to playbooks, so "FLASH dials scored better
on an MI lane" is a finding we would never act on. The manual-bot live stream was likewise rejected
as an evidence engine (3–4 trades only when the operator has screen time). **The program instead
refines the playbook hypotheses themselves: build the research surface (symbols × stretch-metrics ×
time × regime) and surface a few entry candidates per playbook, exits already modeled.**

## The inversion

M1–M7 discovery is unsupervised: enumerate families × symbols × params, backtest, gate. That is how
we got a trend monoculture. This program inverts it: **start from the operator's own fills and work
outward** — tag his historical entries by playbook, build detectors judged first on *"does it fire
where he fired"*, then validate the detectors mechanically on the option path with the native
profile exit attached. His fork framing becomes measurable: a negative result must resolve to
(a) no edge, (b) wrong stretch metric (machine's "stretched" ≠ his eye), or (c) wrong yardstick —
never an ambiguous KILL.

## Data facts (verified 2026-07-03 — corrects earlier claims)

- The personal-fills corpus is **5,760 unique round-trips** (2022-04-29 → 2026-04-29), NOT the
  previously cited 17,002 — the three processed batches under `data/personal_imports/processed/`
  are overlapping re-runs of the same export (100% fill-id overlap; `20260429_232737` is the strict
  superset — **use only it**). Docs citing 17k (incl. `EXIT_PROFILE_PLAYBOOKS.md`) inherit this
  correction.
- **1,465 trades have intraday entry timestamps** (2025-05 → 2026-04): IWM 962 + SPY 338 = 89%,
  TSLA only 23. LONG premium 1,443/1,465; median hold ≈ 28 min.
- mala minute-bar cache covers IWM/SPY back to **2021-05** → entry-context reconstruction and full
  out-of-sample validation need no data purchase.
- Consequences: (1) the labeled corpus supports **rules + operator adjudication, not trained
  classifiers**; (2) fingerprints are **index-ETF statements** — TSLA/single-name conclusions wait
  for P3 mechanical validation or a timestamped ToS/Schwab re-export (open item).

## Gate board

| Gate | Name | Owner / effort | Status | Success = |
|---|---|---|---|---|
| P0 | Spec-lock: taggable playbook definitions | operator ~1 sitting + Claude prep | **DONE 2026-07-04** — spec section in `EXIT_PROFILE_PLAYBOOKS.md`; 2 open confirmations (R4 overnight intent, X3 unclassified share), non-blocking | see §P0 |
| P1 | Tagged corpus + fingerprints | Claude ~3 days + operator 2 phone sessions | **DONE 2026-07-11** — round-2 adjudication: **87% overall, 94% HIGH tier (bar ≥85% ✓)** after 2 rounds / 28 operator labels; corpus FROZEN (`round_trips_tagged_FROZEN.csv`, 390 episodes: FLASH 142 / EXH 117 / TREND 51 / UNCLASS+OTHER 20.5%, within-context 7.5% ✓≤25%); gate criteria: ≥100 met for FLASH+EXH, **TREND 51 misses the 100 bar** (accepted: TREND is the already-live arm; operator relabeling moved mass to reversals, matching his DNA); 4 residual disagreements = 3 machine abstentions (operator label stands) + 1 boredom-vs-setup (irreducible); no PnL leakage; clustering sidecar: no clean 4-way separation (documented) | see §P1 |
| P2 | Metric library ("fires where he fired") | Claude ~1 week | **DONE 2026-07-11 — 2 PASS + 2 documented forks** (`scripts/p2_detector_scorecard.py`, IWM/SPY sweep, 73.8k bar-dir evals): **FLASH PASS** F-C 0.15ATR/≤15m: 59% HIGH recall, 7.4× lift, 4 fires/day (F-A companion: 96% recall, 6.3×, 6/day); **TREND PASS** T-C active-stalling-pullback: 50% recall, 8.7× lift, 4/day; **EXHAUSTION = fork (b), eye not captured** — run-context recalls 97% at 1.9× lift; FOUR trigger geometries tried (proximity+stall, backed-off event ≤45m, stall windows, confirm variants — best 22%); root cause measured on the 5 informative operator labels: his entry timing is HETEROGENEOUS (at-the-extreme w/ 0 stall, touching-retest after 81m, 0.28-ATR below late-day, and fading a counter-bounce inside a bigger run) — n=5 cannot pin it; P3 uses E-C (73% recall, 2.5×, 4/day) as a SUB-BAR screening detector, flagged, with option-path expectancy as the arbiter; more labels accrue in P3 shadow; **RANGE unmeasurable on IWM/SPY** (0 gold — semis play), P3 goes hypothesis-driven (R-A/R-B base rates clean). Caveat recorded: recall vs machine-tagged gold partially circular for rule-mirror detectors; lift/fires-day independent | see §P2 |
| P3 | Mechanical validation (option-path, native exits) | Claude ~1 week | TODO | see §P3 |
| P4 | Promotion to shadow lanes | existing funnel | TODO | see §P4 |
| F1 | Fast-follow: IWM elastic-band triage unblock (EXHAUSTION) | Claude, days | TODO | first non-TREND lane in shadow |
| F2 | Fast-follow: per-profile columns in weekly report (#5) | Claude, ~1 day inside #5 | TODO | weekly report is a 4-profile scoreboard |
| F3 | Parallel: compression runtime adapter (workplan #8) | bhiksha engineering | TODO | RANGE rows compile |

## §P0 — Spec-lock: make the playbook definitions taggable

The playbook table in `EXIT_PROFILE_PLAYBOOKS.md` (thesis / entry / invalidation / symbols per
playbook) is the starting point — P0 extends it with what a tagging rule needs and me guessing
nothing:

- Per playbook: what "over-stretched", "mature extension", "established trend", "stabilized coil"
  mean to the operator's eye in observable terms (relative to VWAP / prior extreme / opening range /
  speed of move / time-of-day), even as ranges or examples rather than exact thresholds.
- Disambiguation rules for the two hard boundaries: FLASH vs EXHAUSTION (freshness/speed of the move
  being faded) and TREND vs RANGE (pullback-in-trend vs break-from-coil).
- Decision: how to treat the 22 short-premium trades (default: park them).

**Success looks like:** every tagging rule in P1 can cite a sentence of the spec; zero thresholds
invented by Claude without an operator-visible flag; operator signs the spec (one sitting, ~1 hour).
The spec lives as a new section in `EXIT_PROFILE_PLAYBOOKS.md` (keeping one canonical playbook doc).

## §P1 — Tagged corpus + per-playbook fingerprints

Pipeline: dedupe to the superset batch → filter long-premium timestamped (1,443) → join
`entry_time_et` to minute bars → compute an auditable entry-context vector (preceding-move
direction/speed/extension at 5/15/30/60 min, VWAP distance in ATR units, day-range position,
volume-climax ratio, run length, range-width percentile, prior-extreme/opening-range interaction,
time-of-day) → deterministic rule tagger from the P0 spec (tag + confidence tier + one-line reason;
**UNCLASSIFIED is a legal output**) → operator adjudicates stratified review packets (~120 trades,
borderlines oversampled, chart per trade, via Lathi bus → Obsidian, pointy-bracket corrections) →
iterate rules → freeze.

**Success looks like:**
- **Agreement ≥85%** between tagger and operator on the HIGH-confidence tier, measured on a
  ≥100-trade adjudicated gold set, after ≤2 correction rounds.
- **UNCLASSIFIED ≤25%** of the timestamped corpus at freeze (falling across rounds is the progress
  metric; a floor is honest — some trades are genuinely ambiguous).
- **≥100 tagged examples each** for FLASH, EXHAUSTION, TREND. RANGE is exempt (operator's rare
  tail, ~1% of history) — its count is *reported*, not required.
- **No outcome leakage:** PnL is never a tagging input (audited by construction — the tagger's
  feature list is in the artifact).
- Deliverables on disk: `round_trips_tagged.csv` (+ tagger code + gold set), and a fingerprint
  report per playbook: symbol league, time-of-day heat, hold/DTE distributions, payoff shape,
  expectancy by stretch decile.
- **Clustering sidecar verdict documented:** unsupervised clusters on the same features either
  roughly reproduce the 4-playbook taxonomy, or the discrepancy (e.g. FLASH/EXHAUSTION inseparable,
  or a 5th mode) is surfaced to the operator BEFORE P2 builds detectors on a false taxonomy.
- Persistent tagger↔operator disagreement is routed back to P0 as spec refinement, not forced.

## §P2 — Metric library: capture the operator's eye

For each playbook, candidate detector metrics (stretch: z-vs-VWAP, ATR-multiple extension,
run-length, volume climax, prior-extreme distance; compression: range-width percentile, NR-n;
continuation: existing MI/OD features) are scored **first against the tagged entries, not against
PnL**.

**Success looks like (per playbook):**
- ≥1 detector whose firing set **recalls ≥50% of the HIGH-confidence tagged entries**, with
- **≥5× lift** (firing rate at his entry moments vs base rate on random session bars — proves
  selectivity, not an always-on trigger), and
- **bounded fire rate** (median ≤~5 fires/day/symbol — a detector that fires constantly "recalls"
  everything and means nothing).
- **Fork discipline:** a playbook where NO detector reaches this bar is recorded as **"eye not yet
  captured — fork (b)"** with the feature list tried, and goes back for feature expansion — it is
  NOT declared no-edge. (This is the anti-M1-mis-kill rule.)

## §P3 — Mechanical validation: option-path, native exits, full history

Surviving detectors run over the **full 2021→2026 bar history** (all days, not just days the
operator traded — this de-confounds his screen-time availability), on IWM/SPY first plus the
operator universe where bars exist. Scoring = S4 option-path (`src/research/option_translation.py`)
with the **native profile exit attached** (FLASH entries scored with FLASH exits — the pairing
problem that made cross-profile counterfactuals moot does not exist here), direction-aware IV model
with kamandal real-IV calibration where snapshots exist, cost/spread haircut applied.

**Success looks like (per candidate, to pass):**
- **Positive option-path expectancy across the IV band including the adverse scenario**, in
  **≥2 distinct regime windows** (e.g. 2022 bear / 2023–24 / 2025–26) — never a single-regime pass
  (the regime-confound lesson from 2026-06-13).
- **≥100 signals** over the full window (RANGE: documented-rarity exemption with explicit rationale).
- **Payoff shape matches the operator DNA:** payoff ratio > 1 with win-rate ~45–60% (asymmetric,
  convexity-carried) — a candidate that passes on hit-rate alone is suspect, per the yardstick that
  mis-killed intraday reversion.
- **Capital-adjusted expectancy positive** (expectancy × max capital fraction), not just per-trade.
- Every failed candidate gets a recorded fork verdict: (a) no edge / (b) wrong metric / (c) wrong
  yardstick — the terrain map is the deliverable, not a bare verdict.
- Output cap: **2–3 ranked candidates per playbook** enter P4; the rest stay on the map.

## §P4 — Promotion to shadow

Candidates flow through the existing funnel: Mala_Evidence Sheet rows with the profile identity in
`ManagementPolicySpec` → bhiksha compile → shadow lanes. Safety gates (capability, tradability,
KILL) stay unrelaxed; evidence gates may be relaxed for shadow per the established
`evidence_gates_relaxed` pattern.

**Success looks like:**
- Lanes compile carrying the **correct profile identity** (`profile__flash_reversal`,
  `profile__exhaustion_reversal`, `profile__range_expansion` — not everything collapsing to
  trend_continuation again).
- Within the first week each new lane has **non-empty entry selection** (no `entry_selector_empty`
  starvation) and **accruing `profile_exit_shadow` evidence**.
- The weekly report (#5 + F2) shows **per-profile rows for every deployed profile**.
- RANGE additionally gated on F3 (compression adapter) — expected to lag; that lag is honest.

## Program-level success (the "so what")

Within ~4–6 weeks: **at least 3 of the 4 profiles have lanes accruing live-loop evidence** (TREND
live today; EXHAUSTION via F1 and/or P4; FLASH via P4; RANGE pending F3), the weekly report is a
four-profile scoreboard, and every playbook has either promoted candidates or a named fork verdict
with a terrain map. The month-test verdict mechanism then covers the operator's actual exit DNA
instead of one profile.

## Open items / risks

- **Timestamped TSLA gap:** a ToS/Schwab re-export with fill times would be the single
  highest-value data add (operator's most-traded symbol has 23 usable timestamps). Check feasibility.
- **Imitation overfit:** detectors could learn the operator's availability (time-of-day he watches)
  rather than tape structure. Mitigated by design: imitation only *generates* hypotheses (P2);
  acceptance is mechanical, all-days, multi-regime (P3).
- **Modeled IV for the past:** historical IV is still modeled (kamandal real-IV accrues
  forward-only, 949 short-DTE snapshots since 2026-06-15) — P3 magnitudes carry that caveat; the
  IV-band pass requirement is the guard.
- **Operator bandwidth:** total ask ≈ one P0 sitting + two ~20-min phone adjudication sessions +
  gate sign-offs. If adjudication stalls, P1 freezes on machine-only tags at LOWER confidence and
  says so — it does not silently proceed as if adjudicated.
