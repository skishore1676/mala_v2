# Shadow Campaign Plan

## Purpose

Run a two-stage shadow campaign before any new live promotion decision.

The first two-week pass validates plumbing and execution reality, not alpha.
The campaign should answer whether Bhiksha fires the same setups Mala tested,
whether option selection is tradable, and whether exits behave sanely under
real market conditions. Strategy changes come after the plumbing evidence is
credible.

## Control Surfaces

- `Mala_Evidence_v1`: Mala-owned M5 evidence. Read-only for operators.
- `Operator_Defaults_v1`: operator/Bhiksha defaults for option constraints.
- `active_strategy`: operator authorization. Bhiksha only shadows rows enabled
  here with `authorization_mode=shadow`.
- `artifacts/playbook/active_plan.json`: Bhiksha compiled runtime truth.
- `data/live_feedback/<active_plan_id>/`: Bhiksha post-close feedback exported
  back to Mala.

## Phase 1: Plumbing Shadow Campaign

Duration: 1-2 weeks.

Goals:

- Confirm active rows compile into the Bhiksha active plan.
- Confirm Bhiksha signals match the intended Mala strategy setup and timestamp.
- Confirm selected contracts are calls for long signals and puts for short signals.
- Confirm selected contracts satisfy DTE, delta, spread, open-interest, and premium constraints.
- Confirm skips are legitimate market-quality skips, not runtime bugs.
- Confirm exits, lifecycle state, and cancellations are observable and explainable.
- Produce a daily report with actionable fixes.

Non-goals:

- No live promotion.
- No broad strategy retuning.
- No new strategy families.
- No cockpit/UI dependency.

## Shadow Activation Criteria

Rows are eligible for `active_strategy` shadow when all are true:

- `bhiksha_ready = TRUE`
- `bhiksha_capability_status = supported`
- `recommendation_tier` is `shadow` or `promote`
- `expectancy > 0`
- `execution_robustness >= 0.75`
- `signal_count >= 20`

Rows below the normal robustness floor can be included only as explicit
experiments when they are otherwise supported and at least
`execution_robustness >= 0.65`.

Unsupported rows remain blocked. This currently keeps Market Impulse
descendants and Compression Breakout out of runtime shadow until adapters exist.

## Option Constraints

Use single-leg long premium options during the campaign:

- Direction long: long call
- Direction short: long put
- DTE: 7-21
- Absolute delta: 0.15-0.35
- Max bid/ask spread: 0.08
- Minimum open interest: 100
- Default max premium per trade: 2000 USD
- One open position per symbol

Win rate is tracked but is not the primary promotion statistic. The campaign
cares first about net R, fill quality, skip quality, and exit behavior.

## Daily Routine

After market close on oldmac:

1. Sync/compile the active plan from Google Sheets.
2. Run Bhiksha review to create observation packets and export them to Mala.
3. Run Mala `research_ops shadow-daily-report`.
4. Review the daily report and create fixes for runtime mismatches.
5. Do not tune strategies until plumbing defects are resolved.

Command outline:

```bash
cd ~/Documents/mala_v2
./.venv/bin/python -m src.research.research_ops shadow-activation-packet

cd ~/Documents/bhiksha
./.venv/bin/python -m bhiksha.tools.bionic_session review --mala-root ~/Documents/mala_v2

cd ~/Documents/mala_v2
./.venv/bin/python -m src.research.research_ops shadow-daily-report --with-evidence
```

## Daily Report Questions

- Which enabled deployments produced signal decisions?
- Which fired true signals?
- Which produced trade plans?
- Which were blocked, and why?
- Did option selection satisfy DTE, delta, spread, open interest, and premium constraints?
- Did exits fire as expected?
- Were there lifecycle, cancellation, provider, or runtime issues?
- Did replay find the same recent setup shape?
- What fixes are required before the next session?

## Phase 2: Strategy Overlay Review

Start only after Phase 1 shows the plumbing is credible.

Tasks:

- Compare shadow observations against Mala M5 expectations.
- Adjust option overlay defaults only when observed option-chain behavior demands it.
- Fix runtime adapters for unsupported descriptor variants that earned research interest.
- Run targeted backtests for regime questions revealed by shadow evidence.
- Keep changes bounded and candidate-specific.

## Promotion Decision

No candidate is promoted from Phase 1.

After a second campaign, a live-review candidate should have:

- Sufficient shadow observations for its signal frequency.
- Positive net R after spread/slippage assumptions.
- Acceptable fill quality versus mid/ask.
- No repeated unhandled exit or lifecycle failures.
- No unresolved runtime mismatch between Mala and Bhiksha.
- Human review and explicit active-strategy authorization.
