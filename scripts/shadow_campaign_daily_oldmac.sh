#!/usr/bin/env bash
set -euo pipefail

MALA_ROOT="${MALA_ROOT:-$HOME/Documents/mala_v2}"
BHIKSHA_ROOT="${BHIKSHA_ROOT:-$HOME/Documents/bhiksha}"
MALA_PYTHON="${MALA_PYTHON:-$MALA_ROOT/.venv/bin/python}"
BHIKSHA_PYTHON="${BHIKSHA_PYTHON:-$BHIKSHA_ROOT/.venv/bin/python}"
ACTIVE_PLAN_PATH="${ACTIVE_PLAN_PATH:-$BHIKSHA_ROOT/artifacts/playbook/active_plan.json}"
BHIKSHA_DB_PATH="${BHIKSHA_DB_PATH:-$BHIKSHA_ROOT/bhiksha.db}"
ACTIVE_PLAN_ID="${ACTIVE_PLAN_ID:-active_plan_$(date +%F)}"
EXPECTED_STRATEGY_DEPLOYMENTS="${EXPECTED_STRATEGY_DEPLOYMENTS:-5}"
EXPECTED_MANUAL_DEPLOYMENTS="${EXPECTED_MANUAL_DEPLOYMENTS:-0}"
TRADING_DAYS="${TRADING_DAYS:-3}"
SHADOW_REPLAY_PROVIDER="${SHADOW_REPLAY_PROVIDER:-schwab}"
SIGNAL_EV_LOOKBACK_DAYS="${SIGNAL_EV_LOOKBACK_DAYS:-21}"
POLYGON_CACHE_BACKFILL_DAYS="${POLYGON_CACHE_BACKFILL_DAYS:-$SIGNAL_EV_LOOKBACK_DAYS}"
SHADOW_SKIP_POLYGON_BACKFILL="${SHADOW_SKIP_POLYGON_BACKFILL:-0}"
OBSIDIAN_VAULT_ROOT="${OBSIDIAN_VAULT_ROOT:-$HOME/Documents/northstar}"
OBSIDIAN_SHADOW_DIR="${OBSIDIAN_SHADOW_DIR:-03 Agent Org/research_lab/Mala/Shadow}"

if [[ ! -x "$MALA_PYTHON" ]]; then
  echo "Missing Mala python: $MALA_PYTHON" >&2
  exit 2
fi
if [[ ! -x "$BHIKSHA_PYTHON" ]]; then
  echo "Missing Bhiksha python: $BHIKSHA_PYTHON" >&2
  exit 2
fi

cd "$BHIKSHA_ROOT"
"$BHIKSHA_PYTHON" -m bhiksha.tools.sync_active_plan

if [[ "${SHADOW_SKIP_ACTIVE_PLAN_PREFLIGHT:-0}" != "1" ]]; then
  "$BHIKSHA_PYTHON" - "$ACTIVE_PLAN_PATH" "$EXPECTED_STRATEGY_DEPLOYMENTS" "$EXPECTED_MANUAL_DEPLOYMENTS" <<'PY'
import json
import sys
from pathlib import Path

active_plan_path = Path(sys.argv[1]).expanduser()
expected_strategy = int(sys.argv[2])
expected_manual = int(sys.argv[3])
plan = json.loads(active_plan_path.read_text(encoding="utf-8"))
deployments = plan.get("deployments", [])
summary = plan.get("summary", {})
suppressed = int(summary.get("suppressed_count") or 0)
strategy_count = sum(
    1
    for deployment in deployments
    if ((deployment.get("source") or {}).get("metadata") or {}).get("row_type") == "strategy"
)
manual_count = sum(
    1
    for deployment in deployments
    if ((deployment.get("source") or {}).get("metadata") or {}).get("row_type") == "manual"
)
not_shadow = [
    deployment.get("deployment_id", "<unknown>")
    for deployment in deployments
    if not ((deployment.get("execution") or {}).get("shadow_only") is True)
]
disabled = [
    deployment.get("deployment_id", "<unknown>")
    for deployment in deployments
    if deployment.get("enabled") is not True
]
errors = []
if suppressed:
    errors.append(f"suppressed_count={suppressed}")
if strategy_count != expected_strategy:
    errors.append(f"strategy_count={strategy_count} expected={expected_strategy}")
if manual_count != expected_manual:
    errors.append(f"manual_count={manual_count} expected={expected_manual}")
if not_shadow:
    errors.append(f"non_shadow_deployments={not_shadow}")
if disabled:
    errors.append(f"disabled_deployments={disabled}")

print(
    "ACTIVE_PLAN_PREFLIGHT "
    f"deployments={len(deployments)} strategy={strategy_count} manual={manual_count} "
    f"suppressed={suppressed} shadow_only_ok={not not_shadow} enabled_ok={not disabled}"
)
if errors:
    raise SystemExit("ACTIVE_PLAN_PREFLIGHT_FAIL " + "; ".join(errors))
PY
fi

if [[ "$SHADOW_SKIP_POLYGON_BACKFILL" != "1" ]]; then
  cd "$MALA_ROOT"
  "$MALA_PYTHON" "$MALA_ROOT/scripts/polygon_cache_topup.py" \
    --data-dir "$MALA_ROOT/data" \
    --active-plan "$ACTIVE_PLAN_PATH" \
    --lookback-days "$POLYGON_CACHE_BACKFILL_DAYS"
else
  echo "POLYGON_BACKFILL skipped=1 reason=scheduled_checkin_uses_bhiksha_feedback"
fi

review_args=(
  -m bhiksha.tools.bionic_session
  review
  --active-plan "$ACTIVE_PLAN_PATH"
  --mala-root "$MALA_ROOT"
  --trading-days "$TRADING_DAYS"
  --provider "$SHADOW_REPLAY_PROVIDER"
)
if [[ "${SHADOW_SKIP_REPLAY:-0}" == "1" ]]; then
  review_args+=(--skip-replay)
fi
cd "$BHIKSHA_ROOT"
"$BHIKSHA_PYTHON" "${review_args[@]}"

cd "$MALA_ROOT"
shadow_daily_args=(
  -m src.research.research_ops
  shadow-daily-report
  --with-evidence
  --active-plan-id "$ACTIVE_PLAN_ID"
  --feedback-root "$MALA_ROOT/data/live_feedback"
)
"$MALA_PYTHON" "${shadow_daily_args[@]}"

signal_ev_args=(
  -m src.research.research_ops
  bhiksha-signal-ev
  --db-path "$BHIKSHA_DB_PATH"
  --lookback-days "$SIGNAL_EV_LOOKBACK_DAYS"
  --same-bar-replay
)
if [[ "${SIGNAL_EV_COUNTERFACTUAL:-1}" == "1" ]]; then
  signal_ev_args+=(--counterfactual-replay)
fi
"$MALA_PYTHON" "${signal_ev_args[@]}"

"$MALA_PYTHON" "$MALA_ROOT/scripts/publish_shadow_decision_brief.py" \
  --mala-root "$MALA_ROOT" \
  --vault-root "$OBSIDIAN_VAULT_ROOT" \
  --output-dir "$OBSIDIAN_SHADOW_DIR" \
  --trading-date "$(date +%F)"
