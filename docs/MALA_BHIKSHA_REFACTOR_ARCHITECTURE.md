# Mala / Bhiksha Refactor Architecture

Status: proposal draft for review, not an implementation commitment.

## Milestone Log

- 2026-05-16: Created refactor work surfaces: `mala_v2` and `bhiksha` on `codex/shared-contract-refactor`, plus new `mala-bhiksha-kernel` repo.
- 2026-05-16: Landed the first minimal kernel: packet schemas, capability manifest, signal parity primitives, registry helpers, and green kernel tests.
- 2026-05-16: Wired Mala to write the IWM/QQQ mean-reversion playbook packet and generated the first canonical packet registry index.
- 2026-05-16: Wired Bhiksha packet compilation to validate shared-kernel packets and fail closed unless an approved execution packet has declared runtime capability.
- 2026-05-16: Added the first signal-parity artifact path for the reversion playbook; current report is blocked with `runtime_adapter_missing`, not passed.
- 2026-05-16: Verified the refactor path end to end: kernel `3 passed`, Mala `336 passed`, and Bhiksha `253 passed`.
- 2026-05-16: Added Bhiksha RTH feature transforms for `opening_vwap_rth`, prior-RTH-close ATR/gap state, and relative RTH volume; focused Bhiksha tests pass.
- 2026-05-16: Added and registered Bhiksha's `intraday_mean_reversion_extremes` adapter with a focused synthetic signal test.
- 2026-05-16: Added a Bhiksha runtime-event export tool for mean-reversion parity CSVs; focused export test passes.
- 2026-05-16: Added Bhiksha's shared-kernel capability manifest generation; reversion remains explicitly blocked with `signal_parity_not_passed`.
- 2026-05-16: Re-verified after the adapter lift: kernel `3 passed`, Mala `336 passed`, and Bhiksha `260 passed`.
- 2026-05-16: Added comparable Mala and Bhiksha signal-event exporters with config-specific parity keys; focused exporter tests pass.
- 2026-05-16: Ran full IWM/QQQ reversion signal parity: `21,127` Mala events vs `21,127` Bhiksha events, `0` missing, `0` extra; entry shadow capability is now supported.
- 2026-05-16: Created and pushed the private `skishore1676/mala-bhiksha-kernel` repository for the shared kernel.
- 2026-05-16: Added the playbook consultation layer guide and a consultation-log `status` command so replay batches have an explicit next action before promotion.
- 2026-05-16: Added Bhiksha's diagnostic legacy-retirement gate; current old-lane scan blocks on `8` active legacy wires pending retirement or fresh re-promotion.
- 2026-05-16: Drafted the first shadow execution packet for the reversion playbook from the passed parity report; Bhiksha compile correctly blocks it pending operator approval and legacy retirement.
- 2026-05-16: Retired the old Bhiksha strategy/deployment wires from runtime reachability; the legacy-retirement report is now clear and the reversion execution packet blocks only on review status and operator approval.
- 2026-05-16: Approved the reversion execution packet for Bhiksha shadow-only activation; compile now passes with runtime controls forbidding live automation.
- 2026-05-16: Added the Bhiksha-native playbook consultation bridge; Bhiksha can now verify the shadow packet, call Mala's query/policy card, and record the consultation artifact without placing orders.
- 2026-05-16: Added the Bhiksha operator decision layer; consultation artifacts can now become red/green shadow intents with a required allowed management policy and `order_submission_allowed=false`.
- 2026-05-16: Added the Bhiksha option-preview layer; `shadow_intent_ready` artifacts can now resolve an option candidate and run chain/quote/risk checks while still requiring live approval.
- 2026-05-16: Split the post-preview path into parallel shadow and live lanes: shadow records executable option PnL, while live creates an explicit approval ticket that is not yet a broker submission.

## Purpose

This proposal describes how Mala and Bhiksha should look if we had the chance
to refactor them cleanly around the work we have now learned the hard way:

- Mala should remain the research lab, analyst, evidence compiler, and
  playbook-design surface.
- Bhiksha should remain the live runtime, feature recomputation engine, option
  selector, execution manager, and audit logger.
- The bridge between them should not depend on copied feature logic, loose
  strategy names, or trust that a runtime adapter "probably means the same
  thing."

The core refactor is to introduce a small shared contract kernel, make packet
authorization explicit, and make signal-level parity a first-class gate. Feature
extraction should come after parity evidence, not before it.

The refactor is also a clean transition, not a careful preservation. The
M1-M5-promoted strategies currently in shadow have run roughly 30% win rate
with negative expectancy over the last month. There is no runtime behavior to
preserve by default. Re-evaluating those hypotheses against the new contracts
is an unavoidable cost and is treated here as a feature, not a regression. The
only thing that carries over as the first forward target is the IWM/QQQ
mean-reversion playbook, because it is the only piece with clean evidence and
bounded scope today.

Legacy code paths are deleted as families migrate. Dead strategies are retired
loudly, not parked. Human review of a system half-converted is harder and
more dangerous than human review of a clean cutover, so dual-wire periods are
treated as short transition states, not steady-state architecture.

## North Star

```text
Mala = research lab + analyst + evidence compiler
Bhiksha = live runtime + feature recomputation + option/execution manager
Trader Desk = operator cockpit
Shared Contract Kernel = packet, capability, and parity contracts both must obey
```

The important architectural boundary is not "Mala vs Bhiksha." The durable
boundary is:

```text
Research truth
Runtime truth
Execution truth
Operator truth
Feedback truth
```

Mala and Bhiksha should become applications on top of shared contracts rather
than two systems each carrying their own version of Newton, session semantics,
and strategy interpretation.

## Success Criteria

The phases are servants of these invariants. The refactor is done when all
four hold:

1. **No unmeasured handoff.** Every Mala -> Bhiksha promotion has a signal
   parity report attached as evidence. No strategy or playbook enters shadow
   or live without one.
2. **No silent feature drift.** A feature lives in one place, or in two places
   with an explicit adapter+tolerance test that fails the build when the gap
   widens. No third copy is allowed.
3. **No untraceable execution.** No trade fires without an approved packet id
   and version on record. The audit chain runs from operator decision to fill
   to feedback without gaps.
4. **No silent strategy decay.** Negative-expectancy strategies trip an
   explicit sunset rule. They do not keep running quietly while attention is
   elsewhere.

If those four hold, the refactor is done. If they do not, more phases. They
are also the criteria that decide whether agents can take more of the
operator loop over time.

## Strategies Carrying Over

The refactor assumes a clean baseline, not a careful preservation:

- **Carrying over:** the IWM/QQQ mean-reversion playbook. It is the only
  surface with clean evidence and bounded scope, and it is the first parity
  target.
- **Not carrying over:** Market Impulse, Opening Drive, Jerk Pivot, Elastic
  Band, and any other M1-M5-promoted strategy that has not yet re-earned
  promotion against the new kernel.

Every retired strategy that wants to come back must re-run its evidence,
parity, and operator approval against the new contracts. No grandfathering.
Old M1-M5 runs become research leads, not runtime assets.
The shadow campaign for the old strategies is wound down before the refactor
opens, not kept running in parallel.

This is treated as a cost, not a loss. A month of negative-expectancy shadow
is enough signal that the old configurations are not worth carrying. The
research effort to re-run hypotheses is a known, bounded cost. The cost of
carrying ambiguous legacy through a refactor is unbounded and corrodes human
review.

## Five Contracts, Four Promotion Gates

The refactor should be argued as contracts between truths, not as a folder
layout question.

```text
Research truth  --[evidence packet]---------------> Operator truth
Operator truth  --[playbook packet / arm decision]-> Runtime truth
Runtime truth   --[execution packet + parity]-----> Execution truth
Execution truth --[fill / fire / outcome events]--> Feedback truth
Feedback truth  --[review artifact]---------------> Research truth
```

The first four crossings are gates that promote or authorize behavior. The last
crossing is the learning loop back into research.

This framing lets us debate each crossing independently:

- what artifact crosses the boundary
- who is allowed to approve it
- what can block it
- what must be recorded afterward

## Target Diagram

```mermaid
flowchart LR
    subgraph Research["Research Truth"]
        Mala["Mala\nresearch lab, analyst, playbook designer"]
        Evidence["Evidence Builder\nM1-M5, playbook surfaces, policy cards"]
    end

    subgraph Kernel["Shared Contract Kernel"]
        Contracts["Packet + Strategy Contracts\nschemas, versions, feature specs"]
        Capabilities["Capability Manifest\nwhat Bhiksha can recompute"]
        Parity["Signal Parity Harness\nsame bars, same params, same decisions"]
    end

    subgraph Registry["Packet Store"]
        EvidencePacket["Evidence Packet\nM1-M5 lineage"]
        PlaybookPacket["Playbook Packet\nconsultant and review"]
        ExecutionPacket["Execution Packet\nruntime-approved"]
    end

    subgraph Runtime["Runtime Truth"]
        Bhiksha["Bhiksha\nlive feature recompute and orchestration"]
        Adapter["Streaming Adapter\nlive data over batch contract"]
        OptionSelector["Option Selector\nDTE, delta, liquidity, risk"]
    end

    subgraph Execution["Execution Truth"]
        Broker["Broker Adapter\nPublic/other broker mechanics"]
        PositionManager["Position Manager\nstops, targets, exits, reconcile"]
    end

    subgraph Operator["Operator Truth"]
        Desk["Trader Desk\ntake/pass, arm/disarm, management policy"]
        Review["Post-Trade Review\noperator + analyst feedback"]
    end

    Mala --> Evidence
    Evidence --> EvidencePacket
    Evidence --> PlaybookPacket
    Mala --> Contracts
    Bhiksha --> Contracts
    Bhiksha --> Capabilities
    Capabilities --> Parity
    Contracts --> Parity
    EvidencePacket --> Parity
    PlaybookPacket --> Parity
    Parity --> ExecutionPacket
    ExecutionPacket --> Bhiksha
    Bhiksha --> Adapter
    Adapter --> OptionSelector
    OptionSelector --> PositionManager
    PositionManager --> Broker
    Desk --> Bhiksha
    Bhiksha --> Desk
    PositionManager --> Review
    Review --> Mala
```

## Main Refactor Bet

Create a shared contract kernel used by both Mala and Bhiksha.

The minimum viable kernel should own:

- feature names and feature specs
- packet schemas and versioning
- capability manifest schema
- parity report schema
- parity fixtures and comparison tools

It should not initially own all Newton transforms, calendars, provider
normalization, and strategy implementations. Those are extraction candidates,
not starting assumptions.

This keeps the first move small enough to land cleanly and still attacks the
real problem: unversioned handoff plus unmeasured runtime drift.

## Explicit Bets

Before moving code, the proposal rests on three bets:

- **Bet A: drift is a dominant failure mode.** If wrong Bhiksha fires mostly
  came from Mala/Bhiksha recomputation drift, parity will expose it and shared
  extraction will pay off. If wrong fires mostly came from weak research edges,
  the fix is research quality, not architecture.
- **Bet B: consultation is a durable product.** If the trader keeps using the
  playbook consultant lane as its own workflow, playbook packets are worth
  naming separately. If consultation is just a temporary review state, one
  packet type with states may be simpler.
- **Bet C: Trader Desk is load-bearing.** If the desk gates execution decisions,
  it belongs in the refactor path. If it is only a better viewer, it should
  come after parity and packet authorization.

## Proposed Package Shape

The exact repo layout can be decided later, but conceptually:

```text
mala_bhiksha_kernel/
  contracts/
    packets.py
    strategies.py
    playbooks.py
    capabilities.py
  parity/
    signal_compare.py
    feature_diagnostics.py
    replay_fixture.py
    report_schema.py
```

Then:

```text
mala_v2
  imports shared kernel for packet writing, capability checks, and parity reports

bhiksha
  imports shared kernel for packet validation, capability manifest, and parity reports
```

Later, parity evidence may justify moving specific feature modules into the
kernel:

```text
mala_bhiksha_kernel/
  bars/
  sessions/
  features/
```

But that extraction should happen one strategy family at a time.

## Operational Setup

The implementation should start with isolated work surfaces:

```text
mala_v2 branch:    codex/shared-contract-refactor
bhiksha branch:    codex/shared-contract-refactor
new repo/package:  mala-bhiksha-kernel
```

The first working branch should not change live behavior. It should produce
parity reports against current behavior. Once a family is migrated and proven,
the old duplicated path for that family should be deleted rather than kept as a
parallel wire.

## Packet Types

The refactor should make packet names explicit.

### Evidence Packet

Used by the older M1-M5 lane.

```text
Mala hypothesis
  -> M1-M5 gates
  -> thesis-exit optimization
  -> provider review where relevant
  -> evidence_packet
  -> active_strategy authorization
  -> Bhiksha shadow/live
```

The evidence packet says: "Mala has evidence for this strategy configuration."

It does not say: "Bhiksha has proven runtime parity."

### Playbook Packet

Used by the Mala 2.2 consultant lane.

```text
Mala playbook design
  -> chart/replay consultation
  -> cohort evidence and policy card
  -> playbook_packet
  -> Bhiksha adapter/parity work
```

The playbook packet says: "Here is a reviewable playbook state and management
proposal."

It may remain advisory for a long time.

### Execution Packet

Used only after runtime readiness.

```text
evidence_packet or playbook_packet
  -> feature contract passes
  -> Bhiksha runtime adapter exists
  -> same-bars parity passes
  -> operator approves
  -> execution_packet
```

The execution packet says: "Bhiksha may shadow or trade this under the declared
mode and controls."

## Parity As A Gate

Parity pass/fail should be judged at the signal and decision layer:

```text
same input bars + same params
  -> same signal timestamps/directions
  -> same invalidation timestamps
  -> same thesis-exit decisions where applicable
```

Feature diffs are diagnostics for disagreements. They should explain why a
signal missed or fired extra; they should not be the primary pass/fail
primitive. A tiny feature delta can be harmless far from a threshold and
catastrophic at the threshold.

Parity output should be an artifact, not just a unit-test pass:

```text
artifacts/parity/<packet_id>/<timestamp>/
  signal_diff.csv
  feature_diagnostics.csv
  exit_diff.csv
  PARITY_REPORT.md
```

The report should classify signal disagreements:

- `feature_drift`
- `provider_drift`
- `warmup_drift`
- `session_boundary_drift`
- `strategy_semantic_drift`
- `exit_semantic_drift`

Promotion rule:

- No playbook packet becomes an execution packet until parity passes.
- No M1-M5 strategy gets live promotion if its active packet has unresolved
  parity misses or extra Bhiksha fires.

## Packet Registry

Packet registry is load-bearing because packet id + version should become the
unit of authorization.

Recommended default:

```text
canonical packet body: git-tracked JSON
operator index:        generated Google Sheet row
runtime compile:       Bhiksha reads approved packet id + version
```

Sheets remain the human control tower, but the full packet payload should not
be an editable row blob. The sheet should expose status, owner, approval,
summary, and links. The immutable JSON body should preserve what was actually
approved.

If we are not willing to build the registry, then we should not pretend packet
ids are authoritative. In that fallback world, `active_strategy` remains the
authorization unit and the refactor is smaller.

## Streaming Adapter

The runtime adapter should be named as a streaming adapter, not a vague strategy
adapter.

Batch research assumes completed bars and stable historical windows. Live
runtime deals with partial bars, late ticks, missing volume, provider gaps, and
ordering. The adapter's job is to reconcile live streaming data into the batch
contract that parity tested.

That distinction matters:

- semantic mismatch means the strategy or feature contract is wrong
- streaming mismatch means live data shape differs from historical batch shape
- provider mismatch means the same contract is fed different market data

## Trader Desk

Trader Desk should sit inside or immediately above Bhiksha.

It should not be a research dashboard. It is the operating cockpit.

Critical-path surface:

- arm/disarm
- emergency square-off
- packet status
- current position-state dump
- runtime block reason

Product surface, after parity and authorization are stable:

- current packet card
- take/pass
- management-policy selection
- option contract preview and selection rationale
- portfolio and buying-power context
- stop/target/invalidation state
- GDS-style option health metrics
- post-trade feedback capture

For playbooks, the desk should feel like:

```text
Mala: "Here is the analyst card."
Operator: "Take or pass; choose management policy."
Bhiksha: "Here is the option, risk, and live management plan."
Operator: "Arm."
Bhiksha: "Execute, manage, reconcile, record."
Mala: "Consume the feedback later."
```

## Agents And The Operator Loop

The long-term goal is that operator involvement reduces over time. Agents
absorb the routine steps once the contracts, parity, and feedback loops are
strong enough to make their work auditable.

The agent surface already sketched in `openclaw-core/workspace/agents/`
maps onto this architecture:

```text
research_lab    -> research truth      (hypothesis intake, evidence orchestration, journal continuity)
or_research     -> research truth      (external scouting, source vetting, weekly digest)
kamandal_ops    -> runtime + execution (current shadow/audit ops precedent)
trade_lab       -> operator/runtime    (paused reference surface, possible future desk lane)
bhiksha_ops     -> runtime + execution (future worker if Trader Desk becomes Bhiksha-native)
```

Do not reuse `radhe_ops` for trading. It is Radhe-specific. If Trader Desk
becomes its own agent surface, it should be revived through `trade_lab` or a
new `bhiksha_ops` worker with explicit source boundaries.

What is true today:

- agents do not autonomously promote
- agents do not autonomously trade
- agents work behind human approval gates
- agents read and write artifacts the operator can review

What the refactor unlocks for them:

- **Packet ids and versions** give agents a stable thing to reason about.
  Without them, agents either summarize sheets they cannot trust or
  hand-build context from scratch every time.
- **Parity reports** give agents a structured artifact to consume. Approval
  recommendations can be grounded in named drift categories instead of vibes.
- **Feedback artifacts** close the loop so an agent reviewing tomorrow can
  see what happened to a packet today.
- **Capability manifests** let an agent answer "is this executable yet" with
  the same fail-closed rule the runtime uses, rather than guessing.

Agent involvement should expand only when each success-criterion invariant
strengthens. A rough order:

1. **Read-only assist.** Agents summarize packets, parity, and feedback for
   the operator. No state changes. Available the moment Phase 2 lands.
2. **Drafting role.** Agents draft evidence packets, parity-report
   commentary, and post-trade reviews for operator approval. Available once
   feedback artifacts are reliably structured (after Phase 5).
3. **Gated promotion role.** Agents propose promotions from evidence to
   playbook packet or playbook to execution packet, with a mandatory human
   approval step that is itself versioned and auditable. Available only
   after the four success-criterion invariants have held over a meaningful
   window.
4. **Autonomous within bounded scope.** Agents act without per-event human
   approval inside narrow, explicitly-bounded loops (e.g. retuning a single
   parameter within an authorized region, with a kill switch). Earned, not
   assumed.

The north star is that the trader becomes the approver and final reviewer,
not the daily operator. The refactor's contracts, parity, and feedback
artifacts are the substrate that makes that trust possible. Until those
hold, agent autonomy stays narrow.

## Feedback Loop

Bhiksha should write structured feedback for both lanes:

- packet id and version
- operator decision: pass/take/arm/disarm
- feature snapshot at decision time
- option selected and why
- rejected option candidates and why
- entry fill or missed-entry reason
- stop/target/invalidation events
- realized outcome
- operator notes
- analyst notes
- post-close replay link

Mala should consume this as evidence and review input. Mala should not treat
live feedback as proof by itself; it should turn feedback into future research,
playbook refinement, and packet revisions.

## Migration Plan

Each phase has a goal, a deliverable, and a stop condition. A phase is not
done because the deliverable exists; it is done when the stop condition
holds.

### Phase 0: Freeze Vocabulary And Authorization

**Goal.** Make the language unambiguous so later phases do not relitigate it.

**Decide and document:**

- evidence packet
- playbook packet
- execution packet
- active strategy row
- runtime deployment
- Trader Desk
- shared contract kernel
- parity report
- packet id + version as the preferred authorization unit
- canonical packet body vs generated operator index

**Deliverable.** Updated docs only.

**Stop condition.** Operator, research, and ops surfaces all reference the
same vocabulary in their working docs. No competing terms remain in active
use.

### Phase 1: Shadow Wind-Down And Forensic Parity

**Goal.** Stop the bleeding and learn from it before opening the refactor.

The current M1-M5 shadow campaign is wound down. A one-shot forensic parity
run is done against the existing strategies (Market Impulse, Opening Drive,
Jerk Pivot, Elastic Band) to classify last month's wrong fires as feature
drift, provider drift, session drift, strategy drift, or execution drift.
This run is **post-mortem evidence, not migration scaffolding**. Its job is
to inform what changes in the new kernel, not to preserve the old strategies.

**Deliverable.**

- shadow stopped, with state archived
- forensic parity report written and circulated
- explicit list of which drift categories were dominant
- explicit list of what the M-gates should change to not promote those
  edges again

**Stop condition.** Forensic report is written, M-gate calibration changes
are agreed, and there is no live or shadow execution running until the new
authorization path opens.

### Phase 1.5: Legacy Retirement, No Grandfathering

**Goal.** Make it impossible to silently carry an old strategy forward.

Every currently promoted M1-M5 strategy is retired explicitly. None is
grandfathered into the new architecture. Any of them that wants to come back
must:

- be re-hypothesized against the new contracts
- pass M-gates as recalibrated in Phase 1
- have a kernel-importable implementation
- pass signal parity in the new harness
- earn operator approval as a fresh packet id

**Deliverable.**

- written retirement statement per strategy
- archived research artifacts and tunings, marked non-live
- legacy execution code paths removed from Bhiksha as their migration
  candidates open in Phase 4 (not kept indefinitely)

**Stop condition.** The only thing the new-path runtime can compile and
execute is the IWM/QQQ mean-reversion playbook. Everything else is either
retired or re-promoted through the new path. Old code may exist only as
archived source or under active removal; it must not be reachable by operator
authorization.

### Phase 2: Minimal Kernel And Registry

**Goal.** Make packet id + version the unit of authorization, not a sheet
row.

Create the small shared contract kernel:

- feature names
- packet schemas
- capability manifest schema
- parity report schema
- replay fixtures

Create the first packet registry path:

- immutable JSON packet body (git-tracked)
- generated sheet/index row
- Bhiksha reads approved packet id and version

**Deliverable.** One packet (the reversion playbook) can be written,
indexed, approved, and compiled without changing live execution behavior.

**Stop condition.** Bhiksha refuses to compile any packet that is not in
the registry with an approved id and version. The refusal is loud and
named (`packet_not_in_registry`, `packet_not_approved`,
`packet_version_mismatch`).

### Phase 3: First Forward Parity Target

**Goal.** Prove signal parity on the one strategy that matters now.

Build the signal parity harness against the reversion playbook on the
shared kernel. Feature diffs are diagnostics, not the primary criterion.

**Deliverable.** Parity report for the reversion playbook showing zero
unexplained signal disagreements over the test window.

**Stop condition.** The playbook can produce an execution packet (Phase 6
will arm it). All disagreements are either resolved or explicitly
categorized and accepted as adapter tolerance.

### Phase 4: Hard Cutover Per Family

**Goal.** No permanent dual-wire confusion.

For each feature family migrated into the kernel:

```text
old path remains only until replacement passes
new path becomes default
old duplicated wire is deleted from both repos
failures become loud in test and fail-closed in runtime
```

Legacy strategies that have not earned re-promotion do not get a migrated
path; their code is removed when the family they relied on migrates.

**Deliverable.** Each migrated family has exactly one implementation,
imported by both Mala and Bhiksha. Removed strategies are removed, not
gated off.

**Stop condition.** A grep across both repos finds no duplicated feature
implementation, no `# old path` comment, and no disabled-but-present
legacy strategy code.

### Phase 5: Minimal Trader Desk

**Goal.** Operator can authorize, monitor, and intervene without a
terminal.

Critical path only:

- arm/disarm
- packet status (id, version, parity state)
- position-state dump
- emergency square-off
- runtime block reason

**Deliverable.** A working desk over Bhiksha APIs with those controls and
nothing else.

**Stop condition.** Operator completes a full arm -> execute -> square-off
cycle from the desk for the reversion playbook in shadow, and unauthorized
execution attempts are blocked with a named reason. Operator uses the desk
for a continuous shadow window without falling back to manual scripts.

### Phase 6: First Live Playbook And Feedback Loop

**Goal.** One playbook, end to end, with execution earned not assumed.

The reversion playbook becomes the first packet to cross from
research-truth all the way through to feedback-truth on the new
architecture.

**Deliverable.**

- approved execution packet for the reversion playbook
- shadow run on the new path
- feedback artifacts written back to Mala
- post-trade review tied to packet id and version

**Stop condition.** Shadow expectancy is non-negative over a declared
trade-count window, feedback artifacts are flowing without manual fixup,
and the operator signs off that the loop is real. Only then is a supervised,
defined-risk live pilot opened, and only for this packet.

### Phase 7: Agent Read-Only Assist (Earned)

**Goal.** Begin reducing operator load without giving up control.

Once Phases 2 through 6 have produced reliable packets, parity reports,
and feedback artifacts, agents enter the loop in a strictly read-only
role: they summarize, surface anomalies, draft commentary. They do not
change state.

**Deliverable.** `research_lab` and `kamandal_ops` are wired into the new
artifacts and produce one daily and one weekly digest the operator
actually uses.

**Stop condition.** Operator confirms the digests reduce review time
without hiding anomalies. Agents have not silently promoted, retuned, or
authorized anything.

### Phase 8 And Beyond: Earned Agent Autonomy

Agent role expansion (drafting, then gated promotion, then bounded
autonomy) is not committed to in this plan. It is gated on the four
success-criterion invariants holding over a meaningful window of live
operation. Each step earns the next one. Nothing here promises a calendar.

## What This Refactor Avoids

- Mala becoming a broker runtime.
- Bhiksha blindly trusting research rows.
- `public_api_trading_v3` becoming a second execution brain.
- duplicated Newton transforms drifting silently.
- playbook consultation being confused with live authorization.
- M1-M5 evidence being confused with runtime parity.
- legacy strategies parked in disabled-but-present state, making human
  review of the system harder than it needs to be.
- agent involvement that runs ahead of the contracts, parity, and feedback
  artifacts that make agent work auditable.

## Open Questions

1. Should the shared kernel live as its own repo, a package inside `mala_v2`, or
   a package inside `openclaw-core`?
2. Should we commit to git-tracked JSON as canonical packet body and Sheets as
   generated operator index?
3. Is Trader Desk architecture-critical or product-after-parity?
4. Should Trader Desk be a Bhiksha-native UI or a separate surface above
   Bhiksha APIs?
5. Which strategy family should be the first parity harness target:
   Market Impulse, Opening Drive, or the new IWM/QQQ mean-reversion playbook?
6. How should streaming-vs-batch mismatch be represented in parity reports?

## Recommendation

Do not start by extracting Newton. Do not start by preserving the current
shadow.

Start by stopping the shadow, writing the forensic parity report on the
strategies that have been losing, and retiring them explicitly. Then build
the contracts, the registry, and signal-level parity against the one
playbook worth carrying forward.

Extract shared features with evidence, not assumptions. Let agent autonomy
expand only as the four success-criterion invariants earn the next step.
