# Mala 2.2 Consultation Journal

The consultation journal is the feedback loop for every operator-facing
playbook, not a feature of one reversion study.

For the trader-facing replay workflow, use
`docs/PLAYBOOK_REPLAY_CONSULTATION_SOP.md`.

The surface answers, "what did similar states do?" The journal answers the more
important follow-up: "when the trader asked this question and either acted or
passed, did the consultation improve the decision?"

## Contract

Every trader-facing playbook query should append one row to:

```text
<run_dir>/consultation_log.csv
```

The row records:

- query identity: `query_id`, timestamp, playbook, symbol, direction
- desk state: `desk_read`, confidence, cohort size
- trader choice: `selected_exit`, `reported_survived_pct`, `taken`
- outcome: `actual_exit_reason`, `actual_pnl_r`, `actual_time_to_exit`,
  `actual_exit_ts_et`
- review links: `QUERY_REVIEW.md` and `query_result.json`

The query path must not auto-pick the exit. The desk reports the menu; the
trader records what was actually used. That keeps the journal from becoming an
autopilot disguised as evidence.

## Close Loop CLI

Create the deterministic policy card from a query:

```bash
python -m src.research.playbook_policy_card \
  --query-json data/results/playbooks/mean_reversion_at_extremes/CURRENT_RUN/surface_queries/iwm_short_20260421T104000_ET_state_management/query_result.json \
  --update-log
```

The policy card is the machine-compression layer. It can say `take`, `pass`,
`wait`, or `out_of_scope` from explicit thresholds and it can prefill
`selected_exit` / `reported_survived_pct` in the journal. It does not mark the
trade as taken and it does not add external context.

Those thresholds are loaded from a versioned playbook policy YAML, not hidden
inside the Python implementation. The default first-slice policy lives at:

```text
research/playbooks/operator_policies/mean_reversion_intraday_operator_v1.yaml
```

Every state-management `query_result.json` and `policy_card.json` embeds the
policy id, version, source path, and config used for the read. Use
`--operator-policy-config <path>` only when deliberately replaying or comparing
a different policy.

External-context agents may later attach caveats such as macro events, news, or
analog-regime anomalies. They should add warnings beside the deterministic card,
not silently rewrite the policy.

List rows that still need outcome capture:

```bash
python -m src.research.playbook_consultation_log list \
  --run-dir data/results/playbooks/mean_reversion_at_extremes/CURRENT_RUN \
  --open-only
```

For historical replay, close the row with machine-computed actuals. The trader
only supplies the judgment (`taken`) and, if taken, the selected management row:

```bash
python -m src.research.playbook_consultation_log replay-close \
  --run-dir data/results/playbooks/mean_reversion_at_extremes/CURRENT_RUN \
  --query-id iwm_short_20260421T104000_ET_state_management \
  --taken Y \
  --selected-exit scalp_0.25pct \
  --historical \
  --operator-note "Would take; matches my chart read."
```

For a historical pass:

```bash
python -m src.research.playbook_consultation_log replay-close \
  --run-dir data/results/playbooks/mean_reversion_at_extremes/CURRENT_RUN \
  --query-id iwm_short_20260421T104000_ET_state_management \
  --taken N \
  --historical \
  --operator-note "Would pass; cohort too mixed."
```

`replay-close` fills `actual_exit_reason`, `actual_pnl_r`,
`actual_time_to_exit`, and `actual_exit_ts_et` from cached historical bars. It
uses the selected exit exactly as defined in the query's management menu.

For live/manual logging, close or update one row directly:

```bash
python -m src.research.playbook_consultation_log close \
  --run-dir data/results/playbooks/mean_reversion_at_extremes/CURRENT_RUN \
  --query-id iwm_short_20260421T104000_ET_state_management \
  --selected-exit scalp_0.25pct \
  --reported-survived-pct 46.8% \
  --taken Y \
  --actual-exit-reason target \
  --actual-pnl-r 0.5 \
  --actual-time-to-exit 8 \
  --operator-note "Followed the scalp row; clean enough."
```

For repeated test replays, query artifacts can be regenerated without appending
to the journal:

```bash
python -m src.research.playbook_surface_query \
  --run-dir data/results/playbooks/mean_reversion_at_extremes/CURRENT_RUN \
  --symbol IWM \
  --direction short \
  --timestamp "2026-04-21 09:40 America/Chicago" \
  --no-log
```

## Gate

The next evidence gate is not another backtest tweak. It is 8-12 filled journal
rows from real or serious replay consultations where `taken`, selected
management, and outcome fields are populated.

Only then can Mala answer whether the consultation loop improved discretionary
entry selection, management, or pass decisions.
