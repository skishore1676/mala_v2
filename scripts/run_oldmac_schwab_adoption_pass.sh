#!/usr/bin/env bash
set -euo pipefail

STAMP="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_DIR="/Users/sunny/Documents/mala_v2/data/results/research_ops/schwab_adoption/${STAMP}"

if [[ "$(id -un)" == "sunny" && -d "/Users/sunny/Documents/mala_v2" ]]; then
  run_target() {
    bash -lc "$1"
  }
else
  run_target() {
    ssh oldmac "$1"
  }
fi

run_target "cd /Users/sunny/Documents/mala_v2 && mkdir -p '$ARTIFACT_DIR' && ./.venv/bin/python - <<'PY'
import json
from pathlib import Path
from src.config import settings
from src.research.shadow_campaign import read_sheet_rows

out = Path('$ARTIFACT_DIR')
rows, active, defaults = read_sheet_rows(
    spreadsheet_id=settings.strategy_catalog_sheet_id,
    credentials_path=settings.google_api_credentials_path,
)
(out / 'evidence_rows.json').write_text(json.dumps(rows, indent=2), encoding='utf-8')
(out / 'operator_defaults_rows.json').write_text(json.dumps(defaults, indent=2), encoding='utf-8')
print(f'EVIDENCE_ROWS={len(rows)}')
print(f'ARTIFACT_DIR={out}')
PY
cd /Users/sunny/Documents/bhiksha && ./.venv/bin/python /Users/sunny/Documents/mala_v2/scripts/schwab_adoption_pass.py --artifact-dir '$ARTIFACT_DIR'"
