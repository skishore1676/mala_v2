# Bhiksha Handoff Contract Report

Generated: 2026-05-01

## Scope

This phase is handoff reliability only. It does not propose profitability changes,
new strategy logic, or new option-premium exit behavior.

## A. Findings

### Current Flow

Mala v2 publishes Strategy_Catalog rows after M5 promotion:

- `hypothesis_agent.py` selects M5 candidates and calls `src.research.catalog.upsert_strategy_catalog`.
- `src/research/catalog.py` writes the fixed 20-column Strategy_Catalog schema.
- Human-visible top-level fields are mostly identity and metrics.
- Important execution details are serialized into `playbook_summary_json`.
- `playbook_summary_json` currently contains `entry_params`, `vehicle_mapping`, `mc_metrics`, optional `thesis_exit_params`, `catastrophe_exit_params`, `exit_candidate_policies`, and `exit_controls`.

Mala's current execution mapping source:

- `src/research/stages/execution.py::option_mapping_for` emits prose-ish option mapping fields.
- For `single_option`, it emits `entry_window_et="09:45-14:30"`, `profit_take="50-90% premium"`, and `risk_rule="hard stop at -35% premium"`.
- These are not derived from strategy-specific entry parameters.

Bhiksha reads and compiles the handoff:

- `src/bhiksha/tools/sync_active_plan.py` and `src/bhiksha/tools/bionic_session.py` call `compile_active_plan_from_google_sheets`.
- `src/bhiksha/active_plan/compiler.py` reads Strategy_Catalog, active_strategy, and manual_entry Google tabs.
- `sync_google_strategy_catalog` turns eligible Google Strategy_Catalog rows into generated YAML under `config/strategy_catalog/google_promoted`.
- `_google_catalog_entry_payload` compiles `playbook_summary_json` into Bhiksha `strategy`, `execution`, `risk`, and `exit` specs.
- `_compile_strategy_row` merges active_strategy row overrides on top of the generated catalog entry.
- Runtime uses `artifacts/playbook/active_plan.json`.

Runtime interpretation:

- `src/bhiksha/strategy/market_impulse.py` controls signal generation window from `entry_buffer_minutes` and `entry_window_minutes`.
- `src/bhiksha/strategy/opening_drive_classifier.py` controls signal generation window from `entry_start_offset_minutes` and `entry_end_offset_minutes`.
- `src/bhiksha/strategy/jerk_pivot_momentum.py` controls signal generation window from `session_start` and `session_end`.
- `src/bhiksha/execution/planner.py::_entry_window_allows` separately gates trade planning from `execution.entry_window_start_et` and `execution.entry_window_end_et`.
- Those two windows can disagree.

### Concrete Examples

| Row | Backtest/strategy signal window | Catalog human window | Compiled active_plan execution window | Runtime evidence |
|---|---:|---:|---:|---|
| `market-impulse-all-basket-discovery__iwm_long` | `09:35-10:15` from `entry_buffer_minutes=5`, `entry_window_minutes=45` | `09:45-14:30` | `09:30-14:30` | `signal_decision` true at 2026-05-01 09:36 ET; `trade_plan` approved |
| `expand30-w1-b1-p3-market-impulse-smh__smh_short` | `09:35-10:15` from `entry_buffer_minutes=5`, `entry_window_minutes=45` | `09:45-14:30` | `09:30-14:30` | `signal_decision` true at 2026-04-28 09:59, 10:14, 10:15 ET; trade plans emitted |
| `elastic_band_reversion_nvda_long_f3cb252b76da` | no native time filter | missing from human payload | `09:30-open` | `signal_decision` true at 2026-04-30 09:34 and 09:55 ET |
| `expand30-w1-b2-p3-avgo-opening-drive__avgo_long` | `10:00-11:30` from opening-drive offsets | `09:45-14:30` | `09:30-14:30` | active_plan compiles broad execution gate |

### Root Causes

- The human `entry_window_et` in `vehicle_mapping` describes a generic option-execution window, not the strategy signal window.
- Bhiksha has two window concepts: signal-window params inside `strategy.params`, and trade-planner execution window inside `execution`.
- active_strategy row aliases `start` / `end` can override `execution.entry_window_*`, producing values that differ from the generated catalog YAML and Strategy_Catalog blob.
- `playbook_summary_json` is lossless-ish but not reviewable; important fields are hidden inside nested JSON.
- Several values are prose or compact text rather than canonical fields: `profit_take`, `risk_rule`, `delta_plan`.
- `use_algorithmic_exit` is sometimes inferred by Bhiksha instead of explicit in old rows.
- `bhiksha_contract_version` is absent, so stale/legacy payloads cannot be rejected by version.
- Option-premium target prose often says `50-90% premium`, while the compiled active_plan has `use_profit_target=false`. That is not necessarily wrong for this phase, but it is unsafe as a human review surface.
- Current Strategy_Catalog rows can be `lifecycle_status=candidate` and still compile if active_strategy references them and `bhiksha_ready=true`.

### Safe vs Unsafe

Currently safe:

- Bhiksha strategy code is internally consistent for the signals it produces.
- The IWM and SMH trades fired according to compiled Bhiksha strategy params.
- The compiler suppresses unknown active_strategy ids; current sync logs show `expand30-amd-mi-01__amd_short` suppressed instead of silently deployed.
- Strategy and exit policy support checks exist in both repos.

Currently unsafe:

- A human cannot reliably answer “what will Bhiksha actually do?” from the Strategy_Catalog row without reading JSON and understanding compiler/runtime defaults.
- Entry windows are ambiguous and duplicated.
- Active_strategy overrides can silently widen/narrow compiled behavior relative to catalog.
- Some human-readable fields are advisory prose, not machine-used contract fields.
- Some machine-used fields are only present in nested JSON or generated YAML.
- There is no schema version gate or row-level validation report surfaced back into the catalog.

Severity: high for reviewability and operator safety, especially for live rows. This is a contract defect, not a strategy-performance defect.

## B. Proposed Handoff Schema

### Contract Principles

- One canonical Bhiksha-facing payload, versioned.
- Human-readable columns mirror the canonical payload and are validator-checked.
- JSON payload remains lossless, but it is not the only source a human must inspect.
- Strategy signal window and execution permission window are separate, explicitly named fields.
- Native strategy exit and Mala thesis exit are separate, explicitly named fields.
- Option-premium catastrophe controls are separate from underlying thesis exits.
- Bhiksha should reject live rows with critical mismatches; shadow rows may compile with warnings only when explicitly allowed.

### Top-Level Human Columns

Recommended Strategy_Catalog v2 columns:

| Column | Required | Notes |
|---|---:|---|
| `bhiksha_contract_version` | yes | integer, start at `2` |
| `catalog_key` | yes | immutable row identity |
| `strategy_key` | yes | Bhiksha registry key |
| `symbol` | yes | uppercase |
| `direction` | yes | `long` / `short` |
| `lifecycle_status` | yes | catalog state |
| `operator_status_override` | no | `live`, `shadow`, `paused`, blank |
| `entry_model` | yes | `strategy_signal_window_then_execution_gate` |
| `signal_window_start_et` | yes if applicable | what strategy code can signal |
| `signal_window_end_et` | yes if applicable | what strategy code can signal |
| `execution_window_start_et` | yes | what planner can enter |
| `execution_window_end_et` | no | blank means no end gate; should be explicit as `none` in payload |
| `entry_confirmation` | yes | compact strategy-specific condition summary |
| `regime_filter` | no | e.g. `market_impulse:15m` |
| `trigger_condition_summary` | yes | plain English, generated from params |
| `option_structure` | yes | `long_call`, `long_put`, etc. |
| `dte_min` / `dte_max` | yes | integers |
| `delta_min` / `delta_max` | no | numbers or blank if not used |
| `max_spread_pct` | yes | decimal, e.g. `0.20` |
| `max_premium_usd` | yes | active risk budget |
| `thesis_exit_policy` | yes | underlying thesis policy or `none` |
| `thesis_stop_summary` | yes if thesis stop exists | generated text from params |
| `thesis_target_summary` | yes if thesis target exists | generated text from params |
| `option_stop_pct` | yes | catastrophe premium stop |
| `option_profit_target_pct` | no | blank unless actually compiled |
| `hard_flat_time_et` | yes | option/risk hard flat |
| `use_native_strategy_exit` | yes | explicit boolean |
| `validation_status` | yes | `ok`, `warning`, `blocked` |
| `validation_warnings` | no | concise generated warnings |
| `source_run_id` | yes | Mala run/provenance |
| `source_metrics_summary` | yes | compact M5 metrics |

### Machine Payload

Column: `bhiksha_payload_json`

```json
{
  "bhiksha_contract_version": 2,
  "identity": {
    "catalog_key": "market-impulse-all-basket-discovery__iwm_long",
    "symbol": "IWM",
    "direction": "long",
    "strategy_key": "market_impulse"
  },
  "strategy": {
    "key": "market_impulse",
    "version": 1,
    "params": {
      "direction": "long",
      "entry_buffer_minutes": 5,
      "entry_window_minutes": 45,
      "regime_timeframe": "15m",
      "vwma_periods": [5, 13, 21]
    },
    "signal_window": {
      "start_et": "09:35",
      "end_et": "10:15",
      "derived_from": ["market_open", "entry_buffer_minutes", "entry_window_minutes"]
    }
  },
  "execution": {
    "profile": "single_leg_long_premium_v1",
    "entry_window": {"start_et": "09:35", "end_et": "10:15"},
    "option_structure": "long_call",
    "option_mapping": {"long_signal": "CALL", "short_signal": "PUT"},
    "dte_min": 7,
    "dte_max": 21,
    "target_abs_delta_min": 0.35,
    "target_abs_delta_max": 0.55,
    "min_open_interest": 100,
    "max_bid_ask_spread_pct": 0.20
  },
  "risk": {
    "max_trade_premium_usd": 1000.0,
    "option_stop_pct": 0.35,
    "hard_flat_time_et": "15:55"
  },
  "exit": {
    "use_native_strategy_exit": false,
    "thesis_exit_anchor": "underlying",
    "thesis_exit_policy": "fixed_rr_underlying",
    "thesis_exit_params": {
      "stop_loss_underlying_pct": 0.005,
      "take_profit_underlying_r_multiple": 2.0
    },
    "catastrophe_exit_anchor": "option_premium",
    "catastrophe_exit_params": {
      "stop_loss_pct": 0.35,
      "hard_flat_time_et": "15:55"
    }
  },
  "source_backtest_metrics": {
    "expectancy": 0.5731,
    "confidence": 0.551,
    "signal_count": 49,
    "execution_robustness": 0.97825,
    "selected_exit_policy": "fixed_rr_underlying:0.0050x2.00"
  },
  "validation_provenance": {
    "mala_version": "mala_v2",
    "published_at": "2026-05-01",
    "validator_version": 1,
    "status": "ok",
    "warnings": []
  }
}
```

### Strategy Examples

Market Impulse:

- `entry_confirmation`: `impulse_regime_{timeframe} aligned; cross-and-reclaim of vma_10`
- `signal_window_start_et/end_et`: derived from `market_open + entry_buffer_minutes` and `market_open + entry_window_minutes`
- `execution_window_start_et/end_et`: should match signal window unless operator explicitly narrows it
- `use_native_strategy_exit`: false unless explicitly approved

Jerk Pivot:

- `entry_confirmation`: `near VPOC; velocity/accel aligned; jerk crosses; volume gate`
- `signal_window_start_et/end_et`: `session_start` / `session_end` when `use_time_filter=true`
- `execution_window`: should not be narrower or broader without an explicit `execution_window_reason`

Elastic Band:

- `entry_confirmation`: `VPOC stretch z-score; velocity; optional jerk/directional mass`
- `signal_window_start_et/end_et`: `none` unless a time filter is added to strategy params
- `execution_window`: explicit planner gate, e.g. `09:30-none`; human column must show this

Opening Drive:

- `entry_confirmation`: `opening drive classification; continuation/failure mode; kinematic/volume/mass gates`
- `signal_window_start_et/end_et`: derived from `market_open + entry_start_offset_minutes` and `market_open + entry_end_offset_minutes`
- `execution_window`: should match or be an intentional subset

## C. Migration Plan

1. Freeze behavior: do not change active strategy params or execution windows during schema rollout.
2. Add Mala v2 publisher support for v2 columns and `bhiksha_payload_json`, while continuing to populate legacy columns.
3. Add Bhiksha compiler support that prefers v2 payload when `bhiksha_contract_version >= 2`; legacy `playbook_summary_json` remains fallback.
4. Run dry-run audit against current Strategy_Catalog and active_plan every sync.
5. Mark rows as `validation_status=blocked` when human columns and compiled payload disagree.
6. Repair rows in batches:
   - preserve `catalog_key`;
   - write v2 columns next to existing row;
   - preserve M5 metrics, `playbook_id`, first/last validated dates, and exit optimization provenance;
   - do not create duplicate rows for the same `catalog_key`.
7. For live rows, require human approval before flipping the compiler to v2-authoritative behavior.
8. Rollback path: Bhiksha compiler can be configured to use legacy fallback for rows without v2 payload; keep pre-migration Strategy_Catalog CSV exports under `data/results/bhiksha_handoff_audit/`.

## D. Compiler / Validator Changes

Implemented in Mala v2:

- Added `src/research/bhiksha_handoff_audit.py`.
- Added `tests/test_bhiksha_handoff_audit.py`.
- The tool reads active_plan JSON plus generated catalog YAML and writes a dry-run CSV/Markdown report.

Recommended Mala changes:

- `src/research/catalog.py`
  - add v2 headers;
  - build `bhiksha_payload_json`;
  - promote typed values instead of prose from `option_mapping_for`;
  - generate human summary columns from the same payload.
- `src/research/stages/execution.py`
  - replace generic `entry_window_et` prose with structured `dte_min`, `dte_max`, `delta_min`, `delta_max`, `option_stop_pct`, and explicit execution window fields.
- `tests/test_catalog.py`
  - assert v2 payload columns;
  - assert Market Impulse signal window derives to `09:35-10:15` for the IWM example;
  - assert human columns match payload.

Recommended Bhiksha changes:

- `src/bhiksha/active_plan/compiler.py`
  - parse `bhiksha_payload_json` before legacy `playbook_summary_json`;
  - reject stale/unknown `bhiksha_contract_version`;
  - validate top-level human columns against payload;
  - emit row-level compiler warnings into active_plan `suppressed` or `validation` section;
  - block live rows with critical mismatch.
- `src/bhiksha/config/models.py`
  - add explicit contract models for `BhikshaStrategyContractV2`;
  - make `use_native_strategy_exit` and option stop/target fields explicit.
- `tests/test_active_plan_compiler.py`
  - add v2 happy-path compile;
  - add rejection tests for human/payload entry-window disagreement;
  - add rejection tests for missing thesis params;
  - add warning tests for legacy rows.

Compiler behavior:

- `blocked`: live rows with critical mismatch, unknown keys in v2 payload, stale schema, missing required thesis params, human/payload disagreement.
- `warning`: legacy payload, prose-only premium target, explicit operator override that narrows execution window with a reason.
- `ok`: human columns, payload, generated YAML, and active_plan all match.

## E. Dry-Run Audit Output

Audit command:

```bash
./.venv/bin/python -m src.research.bhiksha_handoff_audit \
  --active-plan data/results/bhiksha_handoff_audit/current_inputs/active_plan.json \
  --catalog-dir data/results/bhiksha_handoff_audit/current_inputs/strategy_catalog \
  --out-dir data/results/bhiksha_handoff_audit/2026-05-01-dry-run
```

Output:

- CSV: `data/results/bhiksha_handoff_audit/2026-05-01-dry-run/bhiksha_handoff_audit.csv`
- Report: `data/results/bhiksha_handoff_audit/2026-05-01-dry-run/bhiksha_handoff_audit.md`
- Rows scanned: 13 enabled deployments
- Critical rows: 10
- Warning-only rows: 3
- OK rows: 0

Currently enabled/live rows observed in active_plan:

| Severity | Catalog key | Symbol | Strategy | Issue summary |
|---|---|---|---|---|
| critical | `market-impulse-all-basket-discovery__iwm_long` | IWM | market_impulse | human window `09:45-14:30`, active_plan `09:30-14:30`, strategy `09:35-10:15` |
| critical | `expand30-w1-b1-p3-market-impulse-smh__smh_short` | SMH | market_impulse | human window `09:45-14:30`, active_plan `09:30-14:30`, strategy `09:35-10:15` |
| critical | `market-impulse-all-basket-discovery__amd_short` | AMD | market_impulse | human window `09:45-14:30`, active_plan `09:30-14:30`, strategy `09:35-11:00` |
| critical | `market-impulse-all-basket-discovery__qqq_short` | QQQ | market_impulse | human window `09:45-14:30`, active_plan `09:30-14:30`, strategy `09:33-10:30` |
| critical | `market_impulse_spy_short_19383a3c9faf` | SPY | market_impulse | missing human window, active_plan `09:30-open`, strategy `09:33-10:15` |
| critical | `market_impulse_tsla_short_c3faeeac93aa` | TSLA | market_impulse | missing human window, active_plan `09:30-open`, strategy `09:33-10:15` |
| critical | `jerk-pivot-current-basket-discovery__amd_short` | AMD | jerk_pivot_momentum | human window `09:45-14:30`, active_plan `09:30-14:30`, strategy `09:35-15:30` |
| critical | `expand30-w1-b2-p2-amzn-opening-drive__amzn_short` | AMZN | opening_drive_classifier | human window `09:45-14:30`, active_plan `09:30-14:30`, strategy `09:55-11:30` |
| critical | `expand30-w1-b2-p3-avgo-opening-drive__avgo_long` | AVGO | opening_drive_classifier | human window `09:45-14:30`, active_plan `09:30-14:30`, strategy `10:00-11:30` |
| critical | `expand30-w1-b1-p2-mu-market-impulse__mu_long` | MU | market_impulse | human window `09:45-14:30`, active_plan `09:30-14:30`, strategy `09:33-10:30` |
| warning | `elastic_band_reversion_nvda_long_f3cb252b76da` | NVDA | elastic_band_reversion | missing human window, no contract version |
| warning | `elastic_band_reversion_tsla_short_74e3f56b682a` | TSLA | elastic_band_reversion | missing human window, no contract version |
| warning | manual breakout row | TSLA | manual_breakout | no playbook summary; algorithmic exit not explicit in catalog context |

Recommended row-level repair:

- Do not silently change live behavior.
- For each live row, add explicit v2 human columns that reflect current compiled behavior first.
- Then decide separately whether execution windows should be narrowed to strategy signal windows.
- Remove or rename legacy `entry_window_et` so it cannot be mistaken for the strategy signal window.
- Replace `profit_take` prose with either `option_profit_target_pct` or blank plus `option_profit_target_enabled=false`.
- Add `bhiksha_contract_version=2` only after validator status is `ok`.
