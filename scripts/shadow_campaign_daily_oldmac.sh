#!/usr/bin/env bash
set -euo pipefail

MALA_ROOT="${MALA_ROOT:-$HOME/Documents/mala_v2}"
BHIKSHA_ROOT="${BHIKSHA_ROOT:-$HOME/Documents/bhiksha}"
MALA_PYTHON="${MALA_PYTHON:-$MALA_ROOT/.venv/bin/python}"
BHIKSHA_PYTHON="${BHIKSHA_PYTHON:-$BHIKSHA_ROOT/.venv/bin/python}"
ACTIVE_PLAN_PATH="${ACTIVE_PLAN_PATH:-$BHIKSHA_ROOT/artifacts/playbook/active_plan.json}"
TRADING_DAYS="${TRADING_DAYS:-3}"

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

review_args=(
  -m bhiksha.tools.bionic_session
  review
  --active-plan "$ACTIVE_PLAN_PATH"
  --mala-root "$MALA_ROOT"
  --trading-days "$TRADING_DAYS"
)
if [[ "${SHADOW_SKIP_REPLAY:-0}" == "1" ]]; then
  review_args+=(--skip-replay)
fi
"$BHIKSHA_PYTHON" "${review_args[@]}"

cd "$MALA_ROOT"
"$MALA_PYTHON" -m src.research.research_ops shadow-daily-report --with-evidence
