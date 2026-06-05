# Deep Strategy Implementation Checklist

This document is the reusable operating checklist for turning a trading idea or
playbook thesis into an automated Mala strategy lane with playbook-grade due
diligence.

The goal is not to preserve two promotion systems. A playbook can remain as a
research and consultation reference, but an automated candidate must become a
deterministic strategy family that can pass Mala evidence gates, publish to
`Mala_Evidence_v1`, and be adopted by Bhiksha through fail-closed runtime
contracts.

## Doctrine

- Playbooks are a discovery language.
- Strategies are the executable artifact.
- `Mala_Evidence_v1` is the strategy-row publication surface.
- Bhiksha consumes only frozen, supported, operator-authorized rows or packets.
- Human judgment may seed the design, but automated execution requires
  observable features, deterministic triggers, explicit exits, and reviewable
  evidence.
- Shadow is a feedback mechanism. Live trading remains behind explicit operator
  authorization.

## Readiness Levels

Use these labels in reports and handoffs.

| Level | Meaning |
| --- | --- |
| `concept` | Design is coherent, but no executable strategy exists. |
| `skeleton` | Strategy key and fixtures exist, but no serious evidence run has passed. |
| `fixture_shadow` | Local deterministic proof exists with fixtures or cached data only. |
| `evidence_candidate` | Candidate has passed enough Mala gates to be considered for publication. |
| `published_shadow_candidate` | Row is published or publish-ready in `Mala_Evidence_v1`, with Bhiksha capability checks clean enough for shadow. |
| `runtime_shadow` | Bhiksha is allowed to run the candidate shadow-only and produce feedback artifacts. |
| `live_approval_gated` | Candidate may be prepared for live only through active operator approval and live-ticket controls. |
| `blocked` | Unsafe, underspecified, unsupported, or unproven until named fixes land. |

## Phase 0: Control Baseline

Before building:

- Confirm repo branch and cleanliness for `mala_v2`.
- Confirm whether `bhiksha` and `mala-bhiksha-kernel` are source dependencies
  for this slice or only later adoption surfaces.
- Record the intended branch name.
- Run lightweight baseline tests or a focused smoke if the repo has a known
  fast check.
- Identify active docs, playbook artifacts, strategy factory, search-space
  code, stage gates, evidence publisher, and Bhiksha capability reader.

Do not:

- Mutate Google Sheets.
- Sync oldmac.
- Start, stop, or restart Bhiksha.
- Change live authorization.
- Read or print secrets.

Exit when:

- Baseline is clean enough to branch.
- Source of truth and forbidden surfaces are explicit.

## Phase 1: Idea Or Playbook Extraction

Create a strategy design brief before code.

Extract the source idea into:

- `strategy_key`
- `source_playbook` or source hypothesis
- first symbol scope
- candidate directions
- setup definition
- deterministic entry trigger
- required features
- optional filters
- disqualifiers
- optimization parameters
- thesis exit policies
- option vehicle assumptions
- provider-sensitive features
- human-only judgment notes
- expected Bhiksha runtime needs

Classify every playbook statement as one of:

- required feature
- optional filter
- optimization knob
- disqualifier
- management policy
- provenance note
- human-only judgment

Human audit gate:

- Required if the strategy is derived from a discretionary playbook.
- The operator approves whether the brief faithfully represents the thesis and
  whether any human-only judgment can be omitted, encoded, or deferred.

Exit when:

- The intended automated contract is explicit and deterministic.
- Open discretionary parts are named instead of hidden.

## Phase 2: Target-Complete Local Strategy Contract

Implement the intended local strategy contract, not a deliberately underbuilt
skeleton.

If the destination is already clear, build toward that destination immediately:
the declared features, entry model, disqualifiers, search surface, stop model,
exit model, and provenance hooks should all be represented from the start.
Iteration should harden failures discovered by tests, audits, and evidence runs;
it should not be a wandering path from a toy clone toward the real strategy.

Checklist:

- Add or confirm strategy module.
- Register the strategy in `src/strategy/factory.py`.
- Add a stable strategy key and display name.
- Define typed or documented params.
- Encode the full declared search surface needed for the first serious pass.
- Encode known disqualifiers and human-read translations instead of postponing
  them as vague review notes.
- Include stop, thesis-invalidation, and exit policy hooks when they are part
  of the intended strategy.
- Add strategy-level fixtures for long/short behavior if applicable.
- Add search-space support in `src/research/search_space.py`.
- Add hypothesis template or example config.
- Add unit tests for signal behavior and factory registration.

Design rule:

- Build the target local contract first, then let failures decide what to
  simplify, split, or harden.
- Keep the blast radius local: target-complete does not mean publishing,
  syncing oldmac, authorizing `active_strategy`, or touching live runtime.
- If the full intended surface is too large for one evidence run, split the run
  plan without deleting the declared destination.

Exit when:

- `build_strategy` can construct the family.
- The strategy can run in a dry-run or fixture path.
- Tests prove obvious positive and negative cases across the intended contract.
- Any unimplemented parts of the desired destination are explicitly called out
  as blockers, not silently deferred.

## Phase 3: Adversarial Spec Audit

Run a code-aware or blind audit before broad optimization.

Audit questions:

- Is there lookahead bias?
- Are labels, exits, or filters leaking future information?
- Are provider-sensitive features identified?
- Is the strategy secretly relying on human chart reads?
- Is the search surface too large for the amount of data?
- Are exits carrying the entire edge?
- Can Bhiksha represent the params and exits?
- Are symbol and direction choices precommitted enough?

Exit when:

- Findings are either fixed, accepted as explicit risks, or moved to a named
  later phase.

## Phase 4: First Target-Surface Run

Run the declared target surface on the initial symbol set, bounded only where
needed to keep the run interpretable and computationally sane.

Checklist:

- Use the strategy's declared target parameter surface or a documented slice of
  it.
- Run dry-run first.
- Run M1 or equivalent first-pass discovery.
- Capture signal counts, OOS behavior, positive-window rate, expectancy, and
  failure modes.
- Compare output against the source playbook's intended behavior.

Exit when:

- There is enough evidence to decide whether to expand, revise, or stop.

Human audit gate:

- Required only if the first pass produces ambiguous tradeoffs that change the
  strategy's philosophy, such as rare/high-selectivity versus broad/noisy.

## Phase 5: Failure-Driven Hardening And Boundary Minimization

Use the first target-surface evidence to harden the strategy and minimize
boundaries. Add dimensions only when they were part of the destination or when a
failure reveals that the declared surface is missing a necessary distinction.

Possible dimensions:

- symbol universe
- direction
- regime filters
- time/session filters
- entry variants
- disqualifiers
- management policies
- option DTE and delta bands
- provider-sensitive feature variants

Rules:

- Do not optimize every dimension blindly.
- Prefer family-level robustness over one beautiful row.
- Track what was searched and what was rejected.
- Preserve failed and marginal results as research memory.
- Penalize complexity unless it clearly improves robustness.
- When a large surface fails, prefer named splits or disqualifiers over
  retreating to a toy strategy.

Exit when:

- Candidate rows are stable enough for full gates.
- Search boundaries are documented.

## Phase 6: Full Mala Evidence Gates

Run the candidate through the existing strategy lane.

Gate expectations:

- M1 proves initial OOS signal viability.
- M2 proves cost/slippage stability.
- M3 proves broader walk-forward/OOS behavior.
- M4 proves holdout generalization.
- M5 proves execution robustness.
- M6 proves option translation and thesis-exit tradeability.
- M7 proves provider translation where needed.

Reporting can be compact, but the checks should remain explicit.

Exit when:

- Candidate rows either fail with named reasons or become evidence candidates.

## Phase 7: Publication Contract

Prepare `Mala_Evidence_v1` output.

Checklist:

- Include exact strategy params.
- Include strategy variant.
- Include signal window.
- Include thesis exit policy and params.
- Include M6 option evidence.
- Include M7 provider evidence.
- Include Bhiksha capability status.
- Include activation fields.
- Include source provenance.

For playbook-derived strategies, include:

- `playbook_id`
- `playbook_surface_version`
- `entry_model`
- `management_policy`
- `source_playbook_artifacts`
- `human_only_notes_resolved`

Human audit gate:

- Required before any Google Sheet publication or external mutation.

Exit when:

- Local evidence artifacts are valid.
- Publish-readback plan is clear.

## Phase 8: Bhiksha Adoption Feasibility

Only start this once Mala has a concrete strategy contract.

Checklist:

- Add or verify Bhiksha strategy adapter.
- Add runtime capability manifest support.
- Confirm exact params are supported.
- Confirm thesis exit policy is supported.
- Confirm option vehicle mapping is supported.
- Confirm provider parity requirements are known.
- Confirm active-plan compiler compatibility.
- Add local compile tests.

Do not:

- Sync oldmac.
- Mutate `active_strategy`.
- Change live runtime state.

Exit when:

- A local fixture can compile Mala evidence into a Bhiksha deployment manifest.

## Phase 9: Shadow Authorization Packet

Prepare, but do not execute, the operator-facing shadow proposal.

Checklist:

- Proposed `active_strategy` row.
- Expected active-plan diff.
- Expected deployment id.
- Expected shadow/live mode.
- Known block reasons.
- Runtime capability proof.
- Provider/option caveats.
- Rollback/disable path.

Human audit gate:

- Required before writing operator sheets, syncing oldmac, or enabling Bhiksha
  shadow.

Exit when:

- Operator can approve, reject, or request a bounded retune.

## Phase 10: Shadow Feedback Ingestion

After approved shadow runtime exists, feed results back into Mala.

Feedback artifacts should capture:

- signal fired or skipped
- option contract selected
- quote and spread quality
- fill or hypothetical fill
- lifecycle outcome
- stop/target behavior
- provider divergence
- false positives
- missed setups
- operator notes if any
- promote/retune/kill recommendation

Exit when:

- Shadow feedback becomes first-class Mala evidence rather than an external
  anecdote.

## Supervisor And Worker Pattern

Use the supervisor lane for broad or risky work.

Recommended worker roles:

- design extraction worker
- implementation worker
- adversarial audit worker
- Bhiksha feasibility worker
- E2E proof worker

Workers should:

- use isolated branches or worktrees;
- commit only their slice;
- avoid live mutation and external publication;
- provide exact verification commands;
- leave merge decisions to the supervisor.

The supervisor should:

- keep the integration branch coherent;
- verify worker claims directly;
- merge in deliberate order;
- rerun relevant tests;
- produce the final readiness classification.

## Human Gates

Human intervention is required for:

- approving a discretionary playbook-to-strategy brief;
- choosing between materially different trading philosophies;
- external publication;
- Google Sheet writes;
- `active_strategy` authorization;
- oldmac sync or runtime mutation;
- live trading or money movement;
- auth, secrets, and broker/account changes;
- destructive cleanup.

Human intervention is not required for:

- local source inspection;
- fixture tests;
- local dry-runs;
- local evidence generation;
- code-aware audits;
- bounded retune proposals;
- local Bhiksha compile fixtures.

## Definition Of Done For A New Strategy Family

A strategy family is done enough for shadow review when:

- the source thesis has a documented deterministic strategy brief;
- the strategy key is implemented and tested;
- search-space boundaries are documented;
- candidate rows passed the required Mala gates or failed with named reasons;
- local `Mala_Evidence_v1` artifacts are valid;
- Bhiksha runtime support is proven locally;
- proposed shadow rows are explicit and fail-closed;
- all external mutations are waiting on operator approval.

It is not done merely because:

- a backtest passed once;
- a chart example looked right;
- a worker produced a report;
- a row exists in a local CSV;
- Bhiksha can import the module without proving capability and exits.

## First Application: Intraday Mean Reversion Extremes

Initial target:

- `strategy_key`: `intraday_mean_reversion_extremes`
- source playbook: `mean_reversion_at_extremes_intraday_v1`
- first symbol scope: `IWM`, `QQQ`
- publication target: `Mala_Evidence_v1`
- runtime target: Bhiksha shadow only after explicit operator approval

First pass should prioritize:

- faithful deterministic translation over broad optimization;
- clear disqualifiers over hidden human reads;
- exit-policy evidence as a first-class search dimension;
- provider-sensitive feature identification before runtime adoption.
