# Playbook Replay Consultation SOP

**Purpose:** Use historical chart moments to test whether a Mala playbook
consultation would have helped the trader's real decision and management.

This SOP is playbook-general. The current example run is the Mala 2.2 intraday
mean-reversion slice, but the workflow should remain the same for future
playbooks once they support:

- `playbook_surface_query`
- `playbook_policy_card`
- `playbook_consultation_log replay-close`

## Principle

The trader supplies judgment. Mala supplies historical accounting.

The trader answers only:

- Was this a real timestamp where I would have considered the trade?
- Would I take it or pass?
- If I would take it, which management row from the card would I follow?
- Did the card help, confuse, or add no value?

Mala computes the historical outcome fields from cached bars.

Do not manually infer target/stop/PnL from the chart unless the replay tool is
missing data. Manual chart review is for trader judgment, not bookkeeping.

## Setup

Set the run directory once per replay session:

```bash
RUN_DIR=research/results/playbooks/mean_reversion_at_extremes/20260515T_clean_rth_iwm_qqq_surface64
```

For future playbooks, replace `RUN_DIR` with that playbook's current clean run.

## Step 1: Pick A Real Timestamp

Start from your chart, not from Mala output.

Good replay timestamps are moments where you can honestly say:

- "I would have considered long/short here."
- "This resembles a playbook I actually trade."
- "I can decide take/pass without knowing what Mala says first."

Avoid cherry-picking only obvious winners or only obvious losers.

Record the intended playbook, symbol, direction, and timestamp in your own
words before running the query.

## Step 2: Run The Consultation Query

Template:

```bash
python -m src.research.playbook_surface_query \
  --run-dir "$RUN_DIR" \
  --symbol <SYMBOL> \
  --direction <long|short> \
  --timestamp "<YYYY-MM-DD HH:MM America/Chicago>"
```

Example:

```bash
python -m src.research.playbook_surface_query \
  --run-dir "$RUN_DIR" \
  --symbol IWM \
  --direction short \
  --timestamp "2026-05-11 09:40 America/Chicago"
```

The command prints paths like:

```text
QUERY_REVIEW=.../surface_queries/<query_id>/QUERY_REVIEW.md
QUERY_JSON=.../surface_queries/<query_id>/query_result.json
VERDICT=...
```

The `<query_id>` is the folder name under `surface_queries/`.

## Step 3: Generate The Policy Card

Template:

```bash
python -m src.research.playbook_policy_card \
  --query-json "$RUN_DIR/surface_queries/<query_id>/query_result.json" \
  --update-log
```

Read `POLICY_CARD.md`.

The policy card is a deterministic compression of the cohort. It is not an
agent and it is not an order ticket. It should make the decision easier to
review, not make the decision for you.

Read it as:

- `READ`: cohort read, confidence, and sample size.
- `STATE`: percentile rank versus prior same-symbol, same-bias, same-entry-window
  history for VWAP stretch, prior-close ATR stretch, and velocity.
- `ANALOG`: whether the nearest historical cohort is tight, workable, loose, or
  thin based on similarity quality.
- `POLICY`: deterministic take/pass/wait/out-of-scope rule.
- `EXIT`: the management row the policy would prefill if it says take.
- `STOP`: the paired stop reference for that management row.
- `WATCH`: horizon warning, especially whether edge decays after the scalp window.

## Step 4: Decide Take Or Pass

If you would pass, you do not need to choose a management row.

If you would take, choose one `exit_family` from the card or management menu.
Examples:

- `scalp_0.15pct`
- `scalp_0.25pct`
- `scalp_0.35pct`
- `retrace_to_vwap_25pct`
- `retrace_to_vwap_50pct`
- `vwap_return`

Only choose a row you would actually be willing to follow. If the card says
`take` but you would not trust any exit row, close the replay as pass or write
that clearly in `operator-note`.

## Step 5: Replay-Close The Row

For a historical take:

```bash
python -m src.research.playbook_consultation_log replay-close \
  --run-dir "$RUN_DIR" \
  --query-id <query_id> \
  --taken Y \
  --selected-exit <exit_family> \
  --historical \
  --operator-note "<short trader note>"
```

Example:

```bash
python -m src.research.playbook_consultation_log replay-close \
  --run-dir "$RUN_DIR" \
  --query-id iwm_short_20260511T104000_ET_state_management \
  --taken Y \
  --selected-exit scalp_0.25pct \
  --historical \
  --operator-note "Would take; card confirmed fast scalp only."
```

For a historical pass:

```bash
python -m src.research.playbook_consultation_log replay-close \
  --run-dir "$RUN_DIR" \
  --query-id <query_id> \
  --taken N \
  --historical \
  --operator-note "<short trader note>"
```

Example:

```bash
python -m src.research.playbook_consultation_log replay-close \
  --run-dir "$RUN_DIR" \
  --query-id iwm_short_20260511T104000_ET_state_management \
  --taken N \
  --historical \
  --operator-note "Would pass; cohort mixed and not worth options scalp."
```

`replay-close` fills:

- `actual_exit_reason`
- `actual_pnl_r`
- `actual_time_to_exit`
- `actual_exit_ts_et`

If target and symmetric adverse stop are both touched in the same minute bar,
Mala treats the row conservatively as not survived.

For `taken N`, it records `actual_exit_reason=no_trade`.

## Step 6: Check Open Rows

```bash
python -m src.research.playbook_consultation_log list \
  --run-dir "$RUN_DIR" \
  --open-only
```

The replay batch is complete when there are no open rows for the timestamps you
intended to review.

Check batch progress:

```bash
python -m src.research.playbook_consultation_log status \
  --run-dir "$RUN_DIR"
```

The `NEXT_ACTION` field is the consultation lane's current handoff: start a
chart-first query, close open rows, add more rows, or review the closed batch
before any promotion.

## What To Write In Operator Notes

Keep notes short and judgment-focused:

- `Would take; matches my chart read.`
- `Would pass; cohort too mixed.`
- `Would take but only as fast scalp, not hold.`
- `Card helped timing but exit row too conservative.`
- `Card disagreed with me; I still would take.`
- `Not a real playbook moment in hindsight.`

The note should answer: did the consultation help the trader's decision?

## Replay Batch Target

For a serious review batch, complete 8-12 rows:

- Include both IWM and QQQ if both were real candidates.
- Include both take and pass decisions.
- Include timestamps where the card agreed and disagreed with you.
- Do not require the policy card to say `take`; disagreement is useful data.

After 8-12 closed rows, review `consultation_log.csv` for:

- Did the card reduce bad trades?
- Did it improve management choice?
- Did it miss trades you still think were valid?
- Were the selected exits realistic for your options vehicle?
- Are the policy thresholds too strict, too loose, or useful as-is?

## Common Mistakes

- Picking timestamps from Mala's candidates instead of your chart.
- Treating `POLICY: take` as authorization.
- Manually filling historical actuals from visual chart inspection.
- Choosing a management row you would not actually follow.
- Ignoring useful `pass` rows; pass decisions are part of the evidence.
- Mixing live/manual close with historical replay close.

## Live Vs Historical Close

Use `replay-close` for historical review because Mala can compute actuals from
cached bars.

Use `close` for live/manual logging where the actual outcome came from your
real execution or broker record.
