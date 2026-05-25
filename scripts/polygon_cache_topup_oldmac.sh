#!/usr/bin/env bash
set -euo pipefail

MALA_ROOT="${MALA_ROOT:-$HOME/Documents/mala_v2}"
BHIKSHA_ROOT="${BHIKSHA_ROOT:-$HOME/Documents/bhiksha}"
MALA_PYTHON="${MALA_PYTHON:-$MALA_ROOT/.venv/bin/python}"
LOOKBACK_DAYS="${POLYGON_CACHE_TOPUP_LOOKBACK_DAYS:-7}"
MODE="${POLYGON_CACHE_TOPUP_MODE:-all_cache}"
LOG_DIR="$MALA_ROOT/research/results/cache_topup"
LOCK_DIR="$LOG_DIR/polygon_cache_topup.lock"

mkdir -p "$LOG_DIR"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "POLYGON_TOPUP_SKIP reason=lock_exists lock=$LOCK_DIR"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

cd "$MALA_ROOT"

args=(
  "$MALA_ROOT/scripts/polygon_cache_topup.py"
  --data-dir "$MALA_ROOT/data"
  --active-plan "$BHIKSHA_ROOT/artifacts/playbook/active_plan.json"
  --lookback-days "$LOOKBACK_DAYS"
)

if [[ "$MODE" == "all_cache" ]]; then
  args+=(--include-cache-symbols)
fi

"$MALA_PYTHON" "${args[@]}"
