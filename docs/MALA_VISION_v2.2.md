# Mala — Vision Document v2.2

**Status:** Mala 2.2 doctrine. Captures the strategic direction for an operator-bias-conditioned, playbook-based trading rule system built on top of the useful research infrastructure from Mala v2.


## 1. The journey that led here

This document exists because two months of building Mala v2 and three weeks of shadow deployment surfaced layered problems that were not visible from inside the lab. The journey is documented here because it justifies the architectural choices that follow.

### What Mala v2 was originally designed to do

Mala v2 was built as an autonomous strategy-discovery system. The intended workflow:

- Generate hypotheses mechanically (strategy × symbol × parameter combinations)
- Run them through five sequential gates (M1–M5): walk-forward edge, cost convergence, regime stability, holdout generalization, Monte Carlo execution stress
- Survivors get written to a Strategy_Catalog sheet
- Bhiksha consumes the catalog and executes the strategies, in shadow first, then in live, using options as the trading vehicle (7-21 DTE, 15-35 delta)

The architecture had genuine strengths: clean separation between research (Mala) and execution (Bhiksha), hypothesis-as-markdown lab notebook pattern for state-machine discipline, Monte Carlo bootstrap and holdout gates designed to prevent overfitting.

### What shadow deployment revealed

Three layered problems emerged, each hidden by the layer above it:

**Layer 1 — Option translation mismatch.** Mala validated edges at the underlying level; these translated unevenly to OTM options. Strategies with bounded mean-reversion payoffs bled premium even when underlying signals fired correctly. The uniform 35/35 option stops and targets didn't match strategy geometry.

**Layer 2 — Signal divergence between Mala and Bhiksha.** 19% of live Bhiksha signals did not match Mala same-bar replay. The divergence was concentrated in three buckets: a Jerk Pivot parameter bug (since fixed), Elastic Band z-scores 2-3x higher in runtime vs. replay, and Market Impulse regime classification differing between runtime and replay.

**Layer 3 — The old system had no first-class place for operator bias.** Conditions and parameters could overfit, and the system had no clean way to incorporate the trader's current market read, news context, or outside information. It was technically disciplined, but the discipline was applied to a machine-generated search space rather than to a trader-supplied bias.


---

## 2. The new vision

### What this system is for

This system exists to **amplify a trader's market bias with historical probability calibration and rule discipline**, while keeping the trader as the authority over what to trade and how.

The system:
1. Maintains a small library of *playbooks* (market behavior patterns the trader plays)
2. For each playbook, characterizes historical conditions under which the playbook has worked or failed, parameterized by symbol where appropriate
3. When the trader arrives with a bias toward applying a playbook to a current situation, the system can answer in consultation mode: nearest historical analogs, forward outcome distributions, management menu, and policy-card guidance when configured
4. When a surface region has earned it, the system can also propose an execution rule packet (entry trigger, invalidation conditions, exit conditions, hard stop), and the trader approves, edits, or rejects it
5. The trader either uses the consultation evidence to manage the discretionary trade, or explicitly arms a locked packet
6. If a locked packet is armed, the system executes those rules without further trader involvement

The trader brings: the bias, the conviction, final approval, and the decision to consult versus arm. The system brings: historical analog evidence, conditional analysis, empirical management options, proposed rule packets, vehicle feasibility checks, rule execution, and the discipline of not deviating once committed.

### The crucial design principles

**1. The system filters bias, it doesn't generate alpha.** The trader's bias is the primary input. The system's job is to make sure conviction trades are concentrated in conditions where historical hit rates are favorable. The system does not claim every trade will be a winner; losses are expected.

**2. Rules are system-proposed, trader-approved.** The historical analysis produces patterns and proposes executable rule packets. The trader must understand and approve the rules in language they trust. The system does not blindly optimize for the highest historical win rate; it proposes simple, robust regions that the trader can defend.

**3. Execution is rule-bound, not chart-watched.** Once the trader has committed to a trade with specific rules, execution is automated. The trader does not watch charts during the trade. This is a deliberate elimination of the form of discretion most often associated with losses.

**4. Conviction expresses through sizing.** When a trader's bias matches favorable historical conditions, they trade at standard size. When the bias is strong but conditions partially match, they may take the trade at smaller size — explicitly, as a recognition that conviction is overriding probability. Rules do not change; size does.

**5. Exits are thesis-state-conditional.** Trades exit when the reason for the trade is gone — either resolved (profit) or invalidated (loss). Price-level exits exist only as catastrophic backstops. The primary exit logic asks "is the thesis still alive?" not "has price moved X%?"

**6. Vehicle feasibility is separate from playbook validity.** A playbook can be historically useful on the underlying and still be a bad options trade. Option-overlay survival is checked before option execution, but it should not block the first playbook-surface work.

**7. Provider parity is a gate when features depend on provider-sensitive data.** Price-derived features are usually portable. Volume-derived features such as VWMA, VPOC, directional mass, and volume regime require Schwab-vs-Polygon parity checks before execution.

**8. Consultation and execution packets are separate operating lanes.** The operator-led consultation lane can be useful before any strategy is ready for automation. It answers, "I am looking at this setup now; what did similar historical states do, and how would different management choices have behaved?" The armed execution-packet lane asks a stricter question: "Has this exact rule packet survived enough evidence, stress, and operator review to let the machine manage it?" These lanes share evidence, but they do not share readiness standards.

### What this system is explicitly not

- **Not an alpha-discovery engine.** The trader supplies all biases.
- **Not an autonomous trading system.** No trades initiate without trader thesis input.
- **Not a comprehensive market-monitoring platform.** It evaluates playbook applicability on demand; it does not scan for opportunities.
- **Not a backtest framework competing with Mala v2.** Much of the Mala v2 infrastructure is reused for analysis. The M1-M5 gate pipeline needs to be reimagined because a playbook x symbol that does not pass an autonomous-promotion gate may still provide useful conditional evidence when combined with operator bias.

---

## 2.1 Mala 2.2 boundary

This is not a greenfield rebuild.

The working name for the near-term system is:

> **Mala 2.2 — operator-bias-conditioned playbooks on top of Mala's research base.**

The first proof slice is defined separately in
`docs/MALA_2_2_FIRST_SLICE.md`. That document is the build boundary for the
near-term work: one playbook, one bounded evidence surface, and no runtime
promotion until the proof earns it.

Keep from Mala v2:
- historical cache
- hypothesis markdown discipline
- split / holdout / cost-stress mindset
- research artifacts and evidence receipts
- provider-parity machinery where already built
- the habit of proving ideas before touching live money

Change from Mala v2:
- no broad autonomous strategy discovery as the primary product
- no direct Strategy_Catalog -> Bhiksha promotion without playbook, provider, vehicle, and exit checks
- no confusion between Mala's optimized underlying thesis exits and Bhiksha's option-premium catastrophe defaults; runtime must consume the published `thesis_exit_policy` and use option-premium stops only as backstops
- no broad shadow campaign before one playbook slice is understood

---

## 3. The playbook framework

### What is a playbook

A playbook is a recurring market behavior pattern that the trader plays. It is more atomic than a "thesis type" — a single thesis type might contain one or two playbooks.

A playbook is defined by:
- **The market behavior** it bets on (e.g., "extension reverts in bullish regime")
- **The natural time horizon** of the bet (intraday, swing, multi-week)
- **The asset scope** (single name, sector ETF, broad index, or all of these with per-symbol tuning)
- **The signal language** — the features and operationalizations relevant to this playbook
- **The historical conditional surface** — characterized empirically per symbol

Each playbook has its own set of features that matter, its own time horizon, and its own per-symbol parameter regions. The number of playbooks is deliberately small (target: 7-8) because each one represents real trading judgment, and judgment doesn't scale to dozens of patterns.

### Per-playbook, per-symbol parameterization

Within a single playbook, parameter regions differ by symbol for structural reasons:
- Volatility regimes differ (NVDA's ATR-as-%-of-price is much higher than IWM's)
- Microstructure differs (ETFs have creation/redemption flows; single names have earnings cycles)
- Mean-reversion strength differs (ETFs tend to revert more reliably than high-growth single names)
- Time-of-day patterns differ

So a playbook like "fade-overextension-in-bullish-regime" might have:
- IWM-specific parameter region: extension threshold X, VIX condition Y, time window Z, hit rate H
- NVDA-specific parameter region: extension threshold X', VIX condition Y', time window Z', hit rate H'
- Same playbook structurally; tuned per symbol

The number of *playbooks* is small (~7-8). The number of *playbook × symbol* parameter regions is larger (~7-8 playbooks × ~5-10 symbols per playbook = 35-80 conditional surfaces). This is the right level of decomposition only if it is hierarchical:

1. playbook-level behavior first
2. symbol-family adjustment second (index ETF, sector ETF, mega-cap, high-beta single name)
3. symbol-specific parameter regions only when sample size supports them

This prevents the system from becoming a new Strategy_Catalog with better names. IWM should not trade like NVDA under the same overextension bias, but the system should still preserve the shared structure of the overextension playbook.

### Proposed initial playbooks (subject to trader's playbook-spec exercise)

The list below is a hypothesis. The actual playbooks come from the trader's own trading patterns, surfaced through the playbook-spec exercise (Section 6).

1. **Fade overextension in bullish regime** — short an asset that has run too far in a still-bullish trend, expecting a pullback that doesn't break the trend. The "first drop won't hold" structure.

2. **Fade overextension against trend** — short an asset that has extended in a deteriorating or bearish broader regime, expecting a stronger reversal.

3. **Continuation after pullback** — long an asset in an established uptrend that has pulled back without breaking, expecting trend resumption.

4. **Post-event drift** — long/short an asset that has gapped on news (earnings, macro), expecting continuation in the gap direction over multiple days.

5. **Breakout from compression** — long/short an asset that has compressed in a range with declining volatility, expecting directional resolution.

6. **Mean-reversion to a level** — fade/buy an asset moving away from a structurally important level (prior swing, VPOC), expecting return.

7. **Reaction from a level** — long/short an asset moving close to a structurally important level (prior swing, VPOC), expecting return.

8. **Failed move reversal** — fade an asset that has just made a clean breakout/breakdown move that has failed to follow through, expecting return to the prior range.

This list is illustrative. The trader's actual playbooks may differ, and the playbook-spec exercise is what produces the authoritative list.

---

## 4. Feature philosophy and the blended spec

### Parsimony as a discipline

The core principle for feature selection: the smallest set of interpretable features that produces useful conditional insight. Adding more features does not improve the system; it makes outputs less trustable, harder to interpret, and easier to overfit.

### The blended spec architecture

A criticism that surfaced during vision discussion: relying *only* on trader-stated features leaves value on the table. The trader has blind spots about what features matter, and an agent (or systematic analysis) can broaden the feature space beyond what the trader would naturally reach for. Conversely, letting the agent and data fully drive feature selection re-introduces the multiple-comparisons problem that killed Mala v2.

The resolution: a **three-tier feature architecture** for each playbook.

**Tier 1 — Trader-anchored features (high prior, default visibility):**
Features the trader uses explicitly when reading the market for this playbook. These are always shown in the conditional-surface report and treated as high-prior candidates. They are not automatically included in executable rules unless they show evidence, are needed for the trader's interpretation, or are explicitly approved as a discretionary override.

**Tier 2 — Agent/literature-proposed features (high theoretical relevance, must earn place):**
Features that an external perspective (agent or microstructure literature) suggests are relevant for this playbook. These are *tested*, not automatically included. They earn inclusion by showing conditioning power on historical data, surviving out-of-sample testing, having statistically meaningful sample sizes, and being interpretable enough for the trader to articulate why they matter.

**Tier 3 — Universal-truth filters (always applied as context):**
Background conditions universally recognized as relevant (don't trade FOMC days the same way; be cautious during the last hour of OPEX; account for earnings calendar). These are sanity filters, not conditioning features.

### The discipline that keeps Tier 2 honest

The Tier 2 process imports the discipline that Mala v2 lacked:

- Pre-declare the feature list before historical data is examined
- Test against a primary calibration window
- Validate against an untouched holdout window
- Require holdout results within reasonable bounds of calibration results
- Be transparent about how many features were tested (more features tested = higher bar for individual significance)
- Effect-size thresholds: don't include features whose conditioning effect is small enough to be noise
- Cross-symbol consistency check: features that show conditioning power on multiple symbols are more trustable than ones that only work on a single symbol

This is the multiple-comparisons discipline applied in a context where it can actually be enforced — small number of features, conservative thresholds, explicit accounting for what was tested.

---

## 5. The trade lifecycle

The full lifecycle of a trade in this system, from thesis to closeout.

### Step 1: Playbook selection and bias formation (trader, slow, away from screen)

The trader decides — based on news, charts, intuition, accumulated reading, longer-term observation — that they want to apply a specific playbook to a specific situation. For example: "I think IWM is overextended in a bullish regime; I want to fade it intraday tomorrow."

This step happens away from live screens. It's deliberate thinking, not reactive trading.

### Step 2: System consultation (trader queries, system reports)

The trader queries the system: "For [playbook] on [symbol], what are the historical favorable conditions, and how do current conditions match?"

The system reports an evidence packet, not a single fake-precision score:
- The playbook's conditional surface for this symbol
- Today's feature values at consultation time
- Condition match: favorable, partial, outside, or not enough evidence
- Sample size of the conditional region
- Effect size
- Holdout consistency
- Regime breadth
- Cross-symbol or symbol-family consistency
- Provider-parity status for provider-sensitive features
- Vehicle readiness, if the intended vehicle is options

### Step 3: Trade commitment (trader, structured decision)

The trader commits to:
- Take or skip the trade
- If take: conviction tier (e.g., standard / strong / max), which determines position size
- The execution rule packet (entry trigger, invalidation conditions, exit conditions, hard stop) — proposed by the system, then approved or edited by the trader

Once committed, the rules are frozen. Size is frozen. The trader does not revisit until the trade closes.

### Step 4: Automated execution (system, no trader involvement)

The system:
- Monitors for entry trigger; fires entry when triggered
- Sizes position per commitment
- Places hard stop
- Monitors invalidation conditions during the trade
- Fires exit when invalidation triggers, when profit conditions met, when time stop hits, or when hard stop hits

The trader does not watch this. They may receive a notification on entry and on exit, but not tick-level updates.

### Step 5: Review (trader, end of trade or end of day)

The trader reviews:
- Did the rules fire as expected?
- Was the conviction sizing appropriate?
- Was the bias right (independent of outcome)?
- What does this trade say about the playbook's conditional surface?

This is the feedback loop. It informs both the trader's playbook intuition and the system's conditional surfaces over time.

---

## 6. Build plan

### First target playbook

The first playbook to work end-to-end:

> **Mean-reversion at extremes**

The goal is not to prove that a fixed rule always has alpha. The goal is to build the first operator-bias-conditioned playbook surface:

```text
operator bias:
  "I want to fade this symbol because it looks overextended."

system response:
  "For this playbook and symbol, here are the historical conditions where that bias has worked better,
   here are conditions where it failed, here is current-condition evidence, and here is a proposed
   rule packet if you choose to arm the trade."
```

Initial build scope:

- 2-3 symbols
- both directions
- one playbook only
- entry features and exit/invalidation features agreed before testing
- chart visualization in Thinkorswim or another charting surface only after the first parameter surface produces candidates worth inspecting
- no broad runtime deployment until the trader can visually and statistically recognize the playbook surface

### Two operating lanes

Mala 2.2 has one evidence layer and two downstream operating lanes:

**Lane A — operator-led consultation.** This is the default product. The trader brings a symbol, direction, playbook, and timestamp or live state. Mala returns a cohort of similar historical states, not merely a rule-fired verdict. The useful output is a desk card: read, confidence, cohort size, forward reversion/continuation behavior, empirical management menu, and a consultation-log row that can be closed after the trade. A deterministic policy layer may recommend "take / skip / review" when configured, but it remains a policy card, not an autonomous agent.

**Lane B — armed execution packet.** This is earned, not assumed. A favorable surface region must survive chart review, holdout discipline, multiple-comparisons scrutiny, cost and slippage checks, Monte Carlo stress, provider parity when relevant, and vehicle feasibility if options are used. Only then does Mala propose a locked packet for Bhiksha/Kamandal to shadow or execute with human authorization.

The future LLM-agent layer belongs above the policy card, not inside the core proof engine. Its job is to add context the deterministic policy cannot see: macro events, earnings, unusual news flow, correlated-asset stress, or analog-cohort anomalies. It can caveat or escalate a consultation. It cannot silently promote a play into execution.

### Phase 0: Playbook-spec exercise (trader, ongoing)

Before significant code is written, the trader produces written specifications for each playbook they actually trade. The spec includes:
- Plain-English description
- The market behavior being bet on
- Typical time horizon
- Asset scope
- Tier 1 features (what the trader uses mentally)
- Operationalized definitions of vague concepts
- Plain-English invalidation conditions (what would make the trader say "this isn't working")
- Conviction tiers and what differentiates them

**Critical principle: focus on one playbook at a time, build it end-to-end before starting the next.** Spreading effort across multiple playbooks in parallel produces vague half-built things. Building one playbook fully — through Phases 1-5 — produces a working template that the next playbook can follow.

### Phase 1: Playbook operationalization (trader + agent, per playbook)

For the chosen playbook, work together to:
- Refine vague concepts into operational definitions
- Identify Tier 2 candidate features with justifications
- Specify universe filters, entry conditions, invalidation conditions
- Define the declared feature list for historical analysis

This is the blended-spec construction. The output is a frozen specification ready for historical evaluation.

### Phase 2: Historical conditional surface generation (system, per playbook × symbol)

Develop only the minimum code needed, then run the declared feature set against historical data for each target symbol within the playbook's scope. Apply holdout discipline. For each symbol:
- Identify all historical instances where the playbook was active per the universe filter
- Compute realized outcomes
- Characterize the conditioning relationship between entry-time features and outcomes
- Identify favorable-condition regions and unfavorable-condition regions
- Report multi-dimensional confidence

Output: a conditional surface document per playbook × symbol, with explicit confidence reporting.

### Phase 2A: Operator-led consultation surface

Turn the conditional surface into an operator desk tool before treating it as an execution source. For any trader-supplied timestamp or live state, the tool should:

- Retrieve nearest historical analogs for the same playbook, symbol family, direction, and relevant state features
- Report forward behavior over multiple horizons, including fast scalp windows and longer hold windows
- Show empirical MFE, MAE, time-to-peak, time-to-fail, and management-menu outcomes
- Make thin or split cohorts explicit instead of returning fake certainty
- Write a consultation journal row that captures what the system said, what the trader chose, and what happened after close

This lane is valuable even when no locked entry rule fires. It is the way the system supports trades the operator would actually consider, without forcing every question through an autonomous-strategy gate.

### Phase 2.5: Provider and vehicle feasibility

This phase decides whether the conditional surface is executable through the intended live stack.

**Provider parity gate.** If the playbook uses provider-sensitive features (volume, VWMA, VPOC, directional mass, volume regime), run the existing Mala provider-parity machinery before any Bhiksha execution. A playbook can still be useful as an advisory surface if parity is weak, but it should not be promoted to automated execution until the live provider can reproduce the research signal closely enough.

**Vehicle feasibility gate.** If the intended vehicle is options, run an option-overlay preflight before shadow execution. This is not required before doing the playbook-spec and conditional-surface work; it is required before asking Bhiksha to trade options.

Option-overlay preflight has two layers:

1. **Data availability check.** Confirm whether the current data account can fetch historical option contract metadata and minute bars for the target underlyings and date range.
2. **Small overlay test.** For the first playbook, test only a tiny vehicle policy grid on 2-3 symbols before building a general simulator.

Initial option-selection policy:

```text
Direction:
  bullish play  -> call
  bearish play  -> put

DTE:
  start with 7-21 calendar days

Delta target:
  start with 0.30-0.40 delta

Liquidity:
  require non-empty option bars during entry/exit window
  require spread and open-interest filters when quote/OI data is available

Selection:
  choose the contract closest to target delta if historical Greeks are available
  otherwise choose the nearest strike by moneyness using a conservative delta proxy
```

Historical option data source plan:

- **First choice: Polygon/Massive options data**, because the system already uses Polygon for underlying research and their options APIs expose contract reference data and historical options market data. The first engineering task is an entitlement preflight, not a simulator.
- **Fallback: ThetaData or another OPRA historical provider** if Polygon entitlement or data quality is insufficient for expired contracts and intraday option bars.
- **Last-resort proxy:** Black-Scholes / IV-proxy approximation only for rough feasibility. Proxy results can guide thinking but cannot approve option shadow execution.

Historical option choice cannot be perfect. The goal is not to know the exact contract the trader would have picked in 2024. The goal is to apply a deterministic option-selection policy that is close to how the system will choose contracts live, then measure whether the playbook's underlying edge survives that vehicle.

### Phase 3: Rule packet generation (system + agent, trader-approved)

For regions that survive review strongly enough to move beyond consultation, the system uses the conditional surfaces and feasibility checks to propose an executable rule packet:
- Entry trigger (specific, operationalized)
- Invalidation conditions
- Profit-taking conditions (or absence thereof)
- Hard stop placement
- Conviction-to-sizing mapping
- Vehicle policy, if applicable
- Provider-parity warning, if applicable

The trader approves, edits, or rejects the packet. The rules must be written in language the trader trusts and can defend. They are not directly derived from optimization — they're informed by the analysis, proposed by the system, and approved by the trader.

Passing a historical surface is not enough to arm execution. The packet is the unit that must survive cost/slippage analysis, Monte Carlo stress, provider parity where relevant, and vehicle feasibility. The old M-gates are reused here as readiness tools for a locked packet, not as broad kill gates for every exploratory playbook surface.

### Phase 4: Execution wiring (engineering)

Build execution infrastructure (bhiksha / kamandal) to:
- Accept a thesis commitment (playbook + symbol + conviction + rule set)
- Monitor entry trigger conditions
- Fire entry and place hard stop
- Monitor invalidation conditions during trade
- Fire exit per rules
- Report to trader on entry and exit only

### Phase 4A: Bhiksha operationalization boundary

The operating boundary is now explicit:

```text
Mala = analyst and evidence compiler
Bhiksha = live playbook runtime, feature transformer, option selector, execution manager, and audit logger
Public broker adapter = order-routing substrate behind Bhiksha
Trader Desk = operator cockpit inside or immediately above Bhiksha
```

Mala should not call the broker directly and should not become a broker
runtime. Mala publishes a reviewable, versioned playbook packet. Bhiksha
consumes that packet only after the trader arms it, recomputes the required
features from live data, selects the option contract by the approved vehicle
policy, manages exits, and records both machine feedback and trader/analyst
feedback.

The Mala-to-Bhiksha bridge must be tight enough to expand as Mala grows into
new strategies. That means every executable packet needs a stable contract:

- playbook id, packet version, symbol, direction, and intended horizon
- current-state snapshot and required live feature names
- feature capability requirements, including provider-sensitive flags
- entry trigger, invalidation logic, thesis exit policy, and catastrophe stop
- allowed management policies and the selected policy at arm time
- vehicle constraints: option side, DTE range, delta/liquidity/spread filters,
  premium budget, and sizing/conviction mapping
- audit keys tying consultation, operator decision, Bhiksha trade id, live
  feedback, and post-close review together

Bhiksha must fail closed when it cannot compute a required feature, cannot match
the packet version, cannot satisfy the vehicle policy, or sees unresolved
provider-parity risk. This keeps new Mala strategies from leaking into live
execution as vague recommendations. A new strategy becomes executable only when
Bhiksha has a runtime adapter, feature parity, shadow evidence, and reviewable
feedback.

Bhiksha is the right execution surface because it already owns the active plan,
strategy registry, live bar polling, option selection/preflight, position
monitoring, Public broker adapter, reconciliation, and post-close feedback loop.
The work needed for Mala 2.2 is therefore to make Bhiksha feature-parity capable
for locked playbook packets and to expose a trader-desk surface over that
contract, not to create a second runtime inside Mala.

`public_api_trading_v3` remains valuable as a source of product and execution
lessons, not as a competing playbook runtime. Its strongest pieces are the
operator UI, manual intervention flows, Public-specific order lifecycle,
reconciliation, and GDS option telemetry. Those should be harvested into
Bhiksha:

- Trader Desk controls for take/pass, arm/disarm, square-off, and emergency
  intervention
- GDS-style option health metrics as a management overlay on the selected
  option contract
- live logs, portfolio/position visibility, and stuck-trade intervention
- Public.com lifecycle hardening where Bhiksha's broker adapter is thinner

This keeps the roles clean: Mala decides what the evidence says, Bhiksha turns
armed evidence into controlled execution, and Public-specific mechanics stay
behind Bhiksha's broker/execution boundary.

### Phase 5: Live evaluation (small shadow and then real-money trades)

Run a small number of real trades through the playbook. Evaluate:
- Did the rules fire correctly?
- Did the trader stay away from charts as committed?
- Are realized outcomes consistent with the conditional surface predictions?
- What does the live experience suggest about refinement?

This is the loop-closing equivalent of shadow mode. It generates the evidence that refines the playbook and the conditional surfaces.

### Then repeat for the next playbook

Phases 1-5 are per-playbook. Once one playbook is working end-to-end, repeat for the next. The infrastructure built in Phases 2 and 4 is reused; the per-playbook work in Phases 1, 3, and 5 is the per-playbook iteration cost.

### Intended daily operating loop after first shadow deployment

Once the first playbook reaches shadow mode, the operating loop is:

1. Morning: trader selects symbol + bias + playbook.
2. System generates the consultation card from the playbook surface: analog cohort, read, management menu, and policy guidance if configured.
3. Trader either skips, takes the trade manually with consultation-informed management, or arms a previously locked packet.
4. If a packet is armed, Bhiksha runs the rule packet in shadow mode first.
5. Post-close: system writes or updates one review artifact showing consultation, decision, entry/skip, exit reason, and outcome.

The first few weeks of shadow mode are not a P&L proof. They test:

- whether the playbook surface is useful to the trader
- whether the consultation journal improves the trader's actual entry and management decisions
- whether Bhiksha can consume Mala-produced rule packets
- whether live conditions match the research definitions
- whether the trader can stay out of hot-mind chart-watching
- whether the realized outcomes are directionally consistent with the evidence packet

Future version: the bias-selection step may be agent-assisted or agent-proposed, similar to Kamandal's idea funnel. That is intentionally deferred until the manual operator-bias loop works.



## 7. What success looks like

### Milestone 1

- One playbook fully spec'd, with conditional surfaces characterized for 3-5 symbols
- Trader-written execution rules for that playbook, derived from the conditional surfaces
- Execution wiring functional
- A handful of real trades executed through the system, with the trader honoring the no-chart-watching commitment
- Honest evidence about whether the realized outcomes match the conditional surface predictions

### Milestone 2

- 2-3 playbooks fully built out and used in live trading
- A growing library of conditional surfaces per playbook × symbol
- Refined understanding of which playbooks the trader actually values and which were on paper but not in practice
- Demonstrable evidence about whether this paradigm produces better outcomes than discretionary trading without the system
- Modified or abandoned playbooks where evidence indicates they don't work as designed
- Clear feedback on which playbook x bias combinations work, which are marginal, and which should be retired

### Milestone 3

- 5-8 playbooks built out
- Sufficient real-trade history to evaluate the system's true value
- Clear evidence about whether the system is amplifying the trader's edge or just adding overhead
- Decision point: scale up, refine, or wind down
