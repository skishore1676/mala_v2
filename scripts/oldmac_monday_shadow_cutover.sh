#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/oldmac_monday_shadow_cutover.sh --apply --trading-date YYYY-MM-DD

Creates a clean oldmac Monday shadow runtime surface without deleting the old
state. The script snapshots current oldmac Mala/Bhiksha state, rsyncs the local
curated source surface, rotates Bhiksha's runtime DB/log window, regenerates the
active plan, validates the 11 strategy + 1 manual shadow book, and runs a
zero-bar Bhiksha startup check.

Options:
  --apply                 Required to mutate oldmac.
  --host HOST             SSH host. Default: oldmac.
  --trading-date DATE     Trading date used for active_plan_id.
  --skip-rsync            Snapshot/validate only; do not deploy local source.
  --skip-uv-sync          Do not run uv sync on oldmac after deploy.
  --skip-db-rotate        Keep the existing Bhiksha DB/runtime log window.
  --skip-dry-start        Do not run the zero-bar Bhiksha startup check.
  --help                  Show this help.

Environment:
  BHIKSHA_LOCAL_ROOT      Local Bhiksha repo. Default: ../bhiksha.
  EXPECTED_STRATEGY_DEPLOYMENTS  Default: 11.
  EXPECTED_MANUAL_DEPLOYMENTS    Default: 1.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

apply=0
host="${OLDMAC_HOST:-oldmac}"
trading_date="${TRADING_DATE:-}"
skip_rsync=0
skip_uv_sync=0
skip_db_rotate=0
skip_dry_start=0
expected_strategy="${EXPECTED_STRATEGY_DEPLOYMENTS:-11}"
expected_manual="${EXPECTED_MANUAL_DEPLOYMENTS:-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      apply=1
      shift
      ;;
    --host)
      host="${2:?--host requires a value}"
      shift 2
      ;;
    --trading-date)
      trading_date="${2:?--trading-date requires a value}"
      shift 2
      ;;
    --skip-rsync)
      skip_rsync=1
      shift
      ;;
    --skip-uv-sync)
      skip_uv_sync=1
      shift
      ;;
    --skip-db-rotate)
      skip_db_rotate=1
      shift
      ;;
    --skip-dry-start)
      skip_dry_start=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

[[ "$apply" == "1" ]] || die "Refusing to mutate oldmac without --apply"
[[ "$trading_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "--trading-date YYYY-MM-DD is required"

require_cmd git
require_cmd rsync
require_cmd ssh

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mala_local_root="$(cd "$script_dir/.." && pwd)"
bhiksha_local_root="${BHIKSHA_LOCAL_ROOT:-$(cd "$mala_local_root/../bhiksha" && pwd)}"
[[ -d "$bhiksha_local_root/src/bhiksha" ]] || die "Bhiksha local repo not found: $bhiksha_local_root"

cutover_ts="$(date -u +%Y%m%dT%H%M%SZ)"
active_plan_id="active_plan_${trading_date}"

mala_branch="$(git -C "$mala_local_root" rev-parse --abbrev-ref HEAD)"
mala_sha="$(git -C "$mala_local_root" rev-parse --short HEAD)"
mala_dirty="$(git -C "$mala_local_root" status --short | wc -l | tr -d ' ')"
bhiksha_branch="$(git -C "$bhiksha_local_root" rev-parse --abbrev-ref HEAD)"
bhiksha_sha="$(git -C "$bhiksha_local_root" rev-parse --short HEAD)"
bhiksha_dirty="$(git -C "$bhiksha_local_root" status --short | wc -l | tr -d ' ')"

echo "CUTOVER_TS=$cutover_ts"
echo "HOST=$host"
echo "TRADING_DATE=$trading_date"
echo "ACTIVE_PLAN_ID=$active_plan_id"
echo "LOCAL_MALA=$mala_branch@$mala_sha dirty=$mala_dirty"
echo "LOCAL_BHIKSHA=$bhiksha_branch@$bhiksha_sha dirty=$bhiksha_dirty"

ssh "$host" \
  "CUTOVER_TS='$cutover_ts' TRADING_DATE='$trading_date' ACTIVE_PLAN_ID='$active_plan_id' EXPECTED_STRATEGY='$expected_strategy' EXPECTED_MANUAL='$expected_manual' bash -s" <<'REMOTE'
set -euo pipefail

MALA_ROOT="$HOME/Documents/mala_v2"
BHIKSHA_ROOT="$HOME/Documents/bhiksha"
SNAP_ROOT="$HOME/openclaw-backups/monday-shadow-cutover/$CUTOVER_TS"

mkdir -p "$SNAP_ROOT"/{mala,bhiksha,openclaw}

{
  echo "cutover_ts=$CUTOVER_TS"
  echo "trading_date=$TRADING_DATE"
  echo "active_plan_id=$ACTIVE_PLAN_ID"
  echo "host=$(hostname)"
  date -u +"created_at_utc=%Y-%m-%dT%H:%M:%SZ"
} > "$SNAP_ROOT/CUTOVER_CONTEXT.txt"

if [[ -d "$MALA_ROOT/.git" ]]; then
  (
    cd "$MALA_ROOT"
    git rev-parse --abbrev-ref HEAD
    git rev-parse --short HEAD
    git status --short
  ) > "$SNAP_ROOT/mala/git_status_before.txt" || true
fi

if [[ -d "$BHIKSHA_ROOT/.git" ]]; then
  (
    cd "$BHIKSHA_ROOT"
    git rev-parse --abbrev-ref HEAD
    git rev-parse --short HEAD
    git status --short
  ) > "$SNAP_ROOT/bhiksha/git_status_before.txt" || true
fi

crontab -l > "$SNAP_ROOT/openclaw/crontab_before.txt" 2>/dev/null || true
launchctl list | egrep "bhiksha|mala|openclaw|kamandal" > "$SNAP_ROOT/openclaw/launchctl_filtered_before.txt" 2>/dev/null || true
ps aux | egrep "bhiksha|mala|shadow_campaign|server_session" | egrep -v egrep > "$SNAP_ROOT/openclaw/processes_before.txt" 2>/dev/null || true

cp "$MALA_ROOT/.env" "$SNAP_ROOT/mala/env.before" 2>/dev/null || true
cp "$BHIKSHA_ROOT/.env" "$SNAP_ROOT/bhiksha/env.before" 2>/dev/null || true
cp "$BHIKSHA_ROOT/artifacts/playbook/active_plan.json" "$SNAP_ROOT/bhiksha/active_plan.before.json" 2>/dev/null || true
cp "$BHIKSHA_ROOT/bhiksha.db" "$SNAP_ROOT/bhiksha/bhiksha.before.db" 2>/dev/null || true

if [[ -x "$BHIKSHA_ROOT/.venv/bin/python" ]]; then
  (
    cd "$BHIKSHA_ROOT"
    ./.venv/bin/python -m bhiksha.tools.server_session stop --timeout-seconds 10
  ) > "$SNAP_ROOT/bhiksha/server_stop_before.txt" 2>&1 || true
fi

echo "SNAPSHOT_ROOT=$SNAP_ROOT"
REMOTE

if [[ "$skip_rsync" != "1" ]]; then
  echo "Deploying local Mala source to $host..."
  rsync -az --delete \
    --exclude '.git/' \
    --exclude '.venv/' \
    --exclude '.env' \
    --exclude '.pytest_cache/' \
    --exclude '__pycache__/' \
    --exclude '*.pyc' \
    --exclude 'data/' \
    "$mala_local_root/" "$host:~/Documents/mala_v2/"

  echo "Deploying local Bhiksha source to $host..."
  rsync -az --delete \
    --exclude '.git/' \
    --exclude '.venv/' \
    --exclude '.env' \
    --exclude '.pytest_cache/' \
    --exclude '__pycache__/' \
    --exclude '*.pyc' \
    --exclude 'artifacts/' \
    --exclude 'bhiksha.db' \
    "$bhiksha_local_root/" "$host:~/Documents/bhiksha/"
else
  echo "Skipping rsync by request."
fi

ssh "$host" \
  "CUTOVER_TS='$cutover_ts' TRADING_DATE='$trading_date' ACTIVE_PLAN_ID='$active_plan_id' EXPECTED_STRATEGY='$expected_strategy' EXPECTED_MANUAL='$expected_manual' SKIP_UV_SYNC='$skip_uv_sync' SKIP_DB_ROTATE='$skip_db_rotate' SKIP_DRY_START='$skip_dry_start' bash -s" <<'REMOTE'
set -euo pipefail

MALA_ROOT="$HOME/Documents/mala_v2"
BHIKSHA_ROOT="$HOME/Documents/bhiksha"
SNAP_ROOT="$HOME/openclaw-backups/monday-shadow-cutover/$CUTOVER_TS"
BHIKSHA_ARCHIVE="$BHIKSHA_ROOT/artifacts/archive/monday_shadow_cutover_$CUTOVER_TS"

if [[ "$SKIP_UV_SYNC" != "1" ]]; then
  if command -v uv >/dev/null 2>&1; then
    (cd "$MALA_ROOT" && uv sync) > "$SNAP_ROOT/mala/uv_sync_after.log" 2>&1
    (cd "$BHIKSHA_ROOT" && uv sync) > "$SNAP_ROOT/bhiksha/uv_sync_after.log" 2>&1
  else
    echo "uv not found; skipped dependency sync" | tee "$SNAP_ROOT/uv_missing.txt"
  fi
fi

mkdir -p "$BHIKSHA_ARCHIVE"
if [[ "$SKIP_DB_ROTATE" != "1" ]]; then
  if [[ -f "$BHIKSHA_ROOT/bhiksha.db" ]]; then
    mv "$BHIKSHA_ROOT/bhiksha.db" "$BHIKSHA_ARCHIVE/bhiksha.pre_cutover.db"
  fi
  if [[ -d "$BHIKSHA_ROOT/artifacts/playbook/runtime" ]]; then
    mv "$BHIKSHA_ROOT/artifacts/playbook/runtime" "$BHIKSHA_ARCHIVE/runtime.pre_cutover"
  fi
  mkdir -p "$BHIKSHA_ROOT/artifacts/playbook/runtime"
fi
mkdir -p "$BHIKSHA_ROOT/artifacts/playbook/logs"

(
  cd "$BHIKSHA_ROOT"
  ./.venv/bin/python -m bhiksha.tools.sync_active_plan \
    --trading-date "$TRADING_DATE" \
    --active-plan-id "$ACTIVE_PLAN_ID"
) | tee "$SNAP_ROOT/bhiksha/sync_active_plan_after.txt"

"$BHIKSHA_ROOT/.venv/bin/python" - "$BHIKSHA_ROOT/artifacts/playbook/active_plan.json" "$EXPECTED_STRATEGY" "$EXPECTED_MANUAL" <<'PY'
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
non_shadow = [
    deployment.get("deployment_id", "<unknown>")
    for deployment in deployments
    if (deployment.get("execution") or {}).get("shadow_only") is not True
]
disabled = [
    deployment.get("deployment_id", "<unknown>")
    for deployment in deployments
    if deployment.get("enabled") is not True
]
errors = []
if len(deployments) != expected_strategy + expected_manual:
    errors.append(f"deployment_count={len(deployments)} expected={expected_strategy + expected_manual}")
if strategy_count != expected_strategy:
    errors.append(f"strategy_count={strategy_count} expected={expected_strategy}")
if manual_count != expected_manual:
    errors.append(f"manual_count={manual_count} expected={expected_manual}")
if suppressed:
    errors.append(f"suppressed_count={suppressed}")
if non_shadow:
    errors.append(f"non_shadow={non_shadow}")
if disabled:
    errors.append(f"disabled={disabled}")

print(
    "ACTIVE_PLAN_PREFLIGHT "
    f"deployments={len(deployments)} strategy={strategy_count} manual={manual_count} "
    f"suppressed={suppressed} shadow_only_ok={not non_shadow} enabled_ok={not disabled}"
)
for deployment in deployments:
    source_metadata = ((deployment.get("source") or {}).get("metadata") or {})
    strategy = deployment.get("strategy") or {}
    print(
        "ACTIVE_ROW "
        f"id={deployment.get('deployment_id')} "
        f"row_type={source_metadata.get('row_type')} "
        f"symbol={deployment.get('symbol')} "
        f"strategy={strategy.get('key')} "
        f"shadow_only={(deployment.get('execution') or {}).get('shadow_only')}"
    )
if errors:
    raise SystemExit("ACTIVE_PLAN_PREFLIGHT_FAIL " + "; ".join(errors))
PY

if [[ "$SKIP_DRY_START" != "1" ]]; then
  (
    cd "$BHIKSHA_ROOT"
    ./.venv/bin/python -m bhiksha.tools.trade_session \
      --active-plan artifacts/playbook/active_plan.json \
      --live \
      --max-bars 0
  ) | tee "$SNAP_ROOT/bhiksha/dry_start_after.txt"
fi

{
  echo "cutover_ts=$CUTOVER_TS"
  echo "trading_date=$TRADING_DATE"
  echo "active_plan_id=$ACTIVE_PLAN_ID"
  echo "snapshot_root=$SNAP_ROOT"
  echo "bhiksha_archive=$BHIKSHA_ARCHIVE"
  echo "status=ready"
} | tee "$BHIKSHA_ROOT/artifacts/playbook/runtime/monday_shadow_cutover_$CUTOVER_TS.env"

echo "CUTOVER_READY snapshot_root=$SNAP_ROOT"
REMOTE
