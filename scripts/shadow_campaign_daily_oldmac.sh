#!/usr/bin/env bash
set -euo pipefail

MALA_ROOT="${MALA_ROOT:-$HOME/Documents/mala_v2}"
BHIKSHA_ROOT="${BHIKSHA_ROOT:-$HOME/Documents/bhiksha}"
MALA_PYTHON="${MALA_PYTHON:-$MALA_ROOT/.venv/bin/python}"
BHIKSHA_PYTHON="${BHIKSHA_PYTHON:-$BHIKSHA_ROOT/.venv/bin/python}"
ACTIVE_PLAN_PATH="${ACTIVE_PLAN_PATH:-$BHIKSHA_ROOT/artifacts/playbook/active_plan.json}"
BHIKSHA_DB_PATH="${BHIKSHA_DB_PATH:-$BHIKSHA_ROOT/bhiksha.db}"
ACTIVE_PLAN_ID="${ACTIVE_PLAN_ID:-active_plan_$(date +%F)}"
TRADING_DAYS="${TRADING_DAYS:-3}"
SIGNAL_EV_LOOKBACK_DAYS="${SIGNAL_EV_LOOKBACK_DAYS:-21}"
POLYGON_CACHE_BACKFILL_DAYS="${POLYGON_CACHE_BACKFILL_DAYS:-$SIGNAL_EV_LOOKBACK_DAYS}"
OBSIDIAN_VAULT_ROOT="${OBSIDIAN_VAULT_ROOT:-$HOME/Library/Mobile Documents/iCloud~md~obsidian/Documents/northstar}"
OBSIDIAN_SHADOW_DIR="${OBSIDIAN_SHADOW_DIR:-areas/trading/mala-shadow}"

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

if [[ "${SHADOW_SKIP_POLYGON_BACKFILL:-0}" != "1" ]]; then
  cd "$MALA_ROOT"
  "$MALA_PYTHON" - "$ACTIVE_PLAN_PATH" "$POLYGON_CACHE_BACKFILL_DAYS" <<'PY'
import json
import sys
from datetime import date, timedelta
from pathlib import Path

from src.chronos.client import PolygonClient
from src.chronos.storage import LocalStorage

active_plan_path = Path(sys.argv[1]).expanduser()
lookback_days = max(int(sys.argv[2]), 0)
today = date.today()
start = today - timedelta(days=lookback_days)

payload = json.loads(active_plan_path.read_text())
symbols = sorted(
    {
        str(deployment.get("symbol", "")).upper()
        for deployment in payload.get("deployments", [])
        if deployment.get("enabled", True) and deployment.get("symbol")
    }
)
if not symbols:
    print("POLYGON_BACKFILL symbols=0")
    raise SystemExit(0)

storage = LocalStorage()
client = PolygonClient()
for symbol in symbols:
    missing = storage.missing_dates(symbol, start, today)
    written = 0
    for day in missing:
        bars = client.fetch_aggs(symbol, day, day)
        written += storage.save_bars(symbol, bars)
    print(
        f"POLYGON_BACKFILL symbol={symbol} "
        f"missing_days={len(missing)} files_written={written}"
    )
PY
fi

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
cd "$BHIKSHA_ROOT"
"$BHIKSHA_PYTHON" "${review_args[@]}"

cd "$MALA_ROOT"
shadow_daily_args=(
  -m src.research.research_ops
  shadow-daily-report
  --with-evidence
  --active-plan-id "$ACTIVE_PLAN_ID"
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
