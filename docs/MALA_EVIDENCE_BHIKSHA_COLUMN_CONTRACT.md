# Mala Evidence to Bhiksha Column Contract

`Mala_Evidence_v1` is the row-level research passport. Bhiksha may consume it
to compile an authorized `active_strategy` row, but `active_strategy` remains
the operator authorization surface.

Playbook packet evidence is intentionally separate. Use
`MALA_PLAYBOOK_EVIDENCE_V2.md` for the proposed `Mala_Playbook_Evidence_v2`
contract that carries playbook packet discovery, P1-P7 gate status, Bhiksha
shadow feedback, and promotion verdicts.

Default ownership:

- Mala Evidence owns exact-row research facts, strategy params, thesis exits,
  M6 option translation, M7 provider translation, and activation verdicts.
- Operator Defaults owns generic execution guardrails such as delta, minimum
  open interest, spread, option stop, profit target, target handoff behavior,
  stop-restore behavior, and default trade premium.
- active_strategy owns only authorization plus explicit capital or emergency
  operator exceptions.

`active_strategy.execution_overrides` is deprecated for normal rows. If present,
Bhiksha still applies it as an explicit last-mile override, so it must be used
only for named exceptions.

| Column | Bhiksha stance |
| --- | --- |
| `mala_handoff_version` | Consume. Identifies compact Mala Evidence contract and triggers fail-closed exit checks. |
| `catalog_key` | Consume. Primary join key from active_strategy `strategy_id`. |
| `hypothesis_id` | Metadata. Stored for provenance. |
| `symbol` | Consume. Validates compiled strategy symbol. |
| `direction` | Consume. Added to strategy params when absent. |
| `strategy_key` | Consume. Validates runtime strategy family. |
| `strategy_name` | Metadata and capability derivation fallback. |
| `strategy_variant` | Consume. Runtime capability check. |
| `strategy_params_json` | Consume. Builds exact strategy params. |
| `bhiksha_capability_status` | Metadata. Bhiksha recomputes support from runtime code. |
| `bhiksha_capability_reason` | Metadata. Stored for provenance. |
| `bhiksha_ready` | Consume as runtime readiness only. It must not encode weak Mala tier/watch-only state. |
| `provider_validation_status` | Metadata. Legacy M6/M7 provider status. |
| `provider_feature_risk` | Metadata. Legacy provider feature risk. |
| `provider_signal_overlap` | Metadata. Legacy provider overlap. |
| `provider_validation_report` | Metadata. Link/path to provider artifact. |
| `signal_window_et` | Consume. Provides strategy-specific execution start when active_strategy does not override it. |
| `signal_window_derivation` | Metadata. Stored for provenance. |
| `recommendation_tier` | Metadata/gate context. Activation fields decide executable status. |
| `recommendation_tier_reason` | Metadata. Stored for review. |
| `recommendation_checks_json` | Metadata. Stored for review. |
| `expectancy` | Metadata. Stored for review, not direct execution gate. |
| `confidence` | Metadata. Stored for review. |
| `signal_count` | Metadata. Stored for review. |
| `execution_robustness` | Metadata. Stored for review. |
| `m5_execution_profile` | Metadata. Stored for review. |
| `m5_stress_profile` | Metadata. Stored for review. |
| `thesis_exit_tested` | Consume. Required for Mala handoff rows that use Bhiksha capability contract. |
| `thesis_exit_policy` | Consume. Builds thesis exit policy. |
| `thesis_exit_params_json` | Consume. Builds thesis exit params. |
| `option_trade_ready` | Consume. `FALSE` suppresses active compile. |
| `option_adjusted_expectancy_pct` | Metadata. Stored for review; activation fields decide gating. |
| `option_exit_quality` | Metadata. Stored for review. |
| `recommended_dte_min` | Consume. Preferred DTE minimum when vehicle mapping does not explicitly override. |
| `recommended_dte_max` | Consume. Preferred DTE maximum when vehicle mapping does not explicitly override. |
| `theta_penalty_pct` | Metadata. Stored for review. |
| `expectancy_pct` | Metadata. Stored for review. |
| `avg_win_pct` | Metadata. Stored for review. |
| `avg_loss_pct_abs` | Metadata. Stored for review. |
| `pnl_pct_per_minute` | Metadata. Stored for review. |
| `pnl_pct_per_bar` | Metadata. Stored for review. |
| `median_minutes_held` | Metadata. Stored for review. |
| `avg_minutes_held` | Metadata. Stored for review. |
| `target_hit_rate` | Metadata. Stored for review. |
| `stop_loss_rate` | Metadata. Stored for review. |
| `target_hit_within_15_minutes` | Metadata. Stored for review. |
| `target_hit_within_30_minutes` | Metadata. Stored for review. |
| `stop_loss_within_15_minutes` | Metadata. Stored for review. |
| `thesis_exit_metrics_json` | Consume. Exit contract proof and DTE fallback source. |
| `exit_reliability` | Metadata. Stored for review. |
| `exit_trade_count` | Metadata. Stored for review. |
| `run_dir` | Metadata. Used to derive validation date if needed. |
| `warnings` | Metadata. Stored for review. |
| `bhiksha_runtime_supported` | Consume. Runtime support assertion when present. |
| `bhiksha_runtime_reason` | Metadata. Stored for review. |
| `mala_evidence_ready` | Consume. `FALSE` suppresses active compile. |
| `mala_evidence_blocking_checks` | Consume as suppression reason. |
| `activation_candidate` | Consume. `FALSE` suppresses active compile. |
| `activation_blocking_checks` | Consume as suppression reason. |
| `m7_status` | Consume. `block` suppresses active compile. |
| `m7_feature_risk` | Metadata. Stored for review. |
| `m7_signal_overlap` | Metadata. Stored for review. |
| `triage_verdict` | Consume. `KILL` suppresses active compile. |
| `triage_verdict_reason` | Consume as suppression reason and metadata. |
| `triage_blocking_checks` | Consume as suppression reason and metadata. |
| `triage_advisory_notes` | Metadata. Stored for review. |
| `triage_artifact` | Metadata. Stored for review. |

Gate separation:

- `bhiksha_ready` means the row is technically loadable by Bhiksha for its
  strategy variant, params, and thesis-exit policy.
- `mala_evidence_ready` means Mala evidence quality is above the shadow floor.
- `activation_candidate` means runtime support, Mala evidence, option exit
  tradeability, and M7 provider translation are all clean enough for
  `active_strategy` authorization.
- `active_strategy` remains the only operator authorization surface.

Current compiler precedence for strategy rows:

1. Load exact strategy/exits from `Mala_Evidence_v1`.
2. Apply DTE from `playbook_summary_json.vehicle_mapping` if explicitly
   present, otherwise `recommended_dte_min/max`, otherwise
   `thesis_exit_metrics_json`, otherwise Operator Defaults.
3. Apply delta/OI/spread from vehicle mapping if present, otherwise Operator
   Defaults.
4. Apply signal-window start from `signal_window_et` unless vehicle mapping or
   active_strategy already provides an execution window.
5. Apply target approach and pullback-restore defaults from
   `Operator_Defaults_v1` unless row-level catastrophe exit params explicitly
   override them.
6. Apply `active_strategy.execution_overrides` only as a final explicit escape
   hatch.
