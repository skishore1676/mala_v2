# Shadow Campaign Plan

**Status:** current strategy-shadow operating plan as of 2026-05-17. The active
book is the 11-row M5.5 option-aware packet, not the older broad
Strategy_Catalog-era campaign.

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
- `option_trade_ready = TRUE`
- `option_adjusted_expectancy_pct > 0`
- `recommendation_tier` is `shadow` or `promote`
- `execution_robustness >= 0.75`
- `signal_count >= 20`
- row-level `execution_overrides` are present for DTE, delta, spread, and open-interest policy

Rows below the normal robustness floor can be included only as explicit
experiments when they are otherwise supported and at least
`execution_robustness >= 0.65`.

Unsupported rows remain blocked. No family is grandfathered because it passed
an older M1-M5 run. Rows that are Bhiksha-capable but option-unready remain in
evidence only; they do not enter active shadow.

## Option Constraints

Use single-leg long premium options during the campaign:

- Direction long: long call
- Direction short: long put
- DTE: row-level M5.5 recommendation, normally 0-14 DTE for the short-term
  packet and 14-21 only as not-ready/diagnostic evidence
- Absolute delta: row-level override, default target range 0.30-0.60
- Max bid/ask spread: row-level override, default no wider than 0.08
- Minimum open interest: row-level override, default at least 100
- Default max premium per trade: 2000 USD
- One open position per symbol

Win rate is tracked but is not the primary promotion statistic. The campaign
cares first about net R, fill quality, skip quality, and exit behavior.

## Daily Routine

After market close on oldmac:

1. Sync/compile the active plan from Google Sheets.
2. Run Bhiksha review to create observation packets and export them to Mala.
3. Run Mala `research_ops shadow-daily-report`.
4. Run Mala `research_ops bhiksha-signal-ev` against Bhiksha SQLite.
5. Review both reports and create fixes for runtime mismatches.
6. Do not tune strategies until plumbing defects are resolved.

The oldmac wrapper for this routine is:

```bash
cd ~/Documents/mala_v2
./scripts/shadow_campaign_daily_oldmac.sh
```

The wrapper filters the daily report to today's active plan by default and
refreshes the Mala Polygon cache for active-plan symbols before replay so
same-bar and counterfactual checks can evaluate the current session. It also
enables Bhiksha counterfactual replay. Override with `ACTIVE_PLAN_ID=...`,
`POLYGON_CACHE_BACKFILL_DAYS=...`, `SHADOW_SKIP_POLYGON_BACKFILL=1`, or
`SIGNAL_EV_COUNTERFACTUAL=0` only for a specific triage run.

Expanded command outline:

```bash
cd ~/Documents/mala_v2
./.venv/bin/python -m src.research.research_ops shadow-activation-packet

cd ~/Documents/bhiksha
./.venv/bin/python -m bhiksha.tools.bionic_session review --mala-root ~/Documents/mala_v2

cd ~/Documents/mala_v2
./.venv/bin/python -m src.research.research_ops shadow-daily-report \
  --with-evidence \
  --active-plan-id "active_plan_$(date +%F)"
./.venv/bin/python -m src.research.research_ops bhiksha-signal-ev \
  --db-path ../bhiksha/bhiksha.db \
  --lookback-days 21 \
  --same-bar-replay \
  --counterfactual-replay
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

## Signal and EV Audit

`bhiksha-signal-ev` answers the two promotion-blocking questions from runtime
truth:

- Did Bhiksha fire through the Mala-sourced deployment, in the expected
  direction, inside the Mala signal window?
- Did a trade plan appear near the true signal timestamp?
- Did the selected option produce realized premium PnL and option-stop-R in the
  same family as the Mala expectancy attached to that deployment?

Without `--same-bar-replay`, the audit is a compiled-runtime concordance check.
With `--same-bar-replay`, it independently reruns the Mala strategy on cached
1-minute bars for each Bhiksha true-signal bar. Missing cached bars should be
treated as a data-backfill gap, not as a strategy mismatch.

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
