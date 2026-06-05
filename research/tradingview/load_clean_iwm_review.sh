#!/usr/bin/env bash
set -euo pipefail

# Loads the clean Mala IWM review overlay:
# - Market Pulse VMA on
# - entry/exit labels on
# - VPOC, VWMA stack, risk boxes, and risk lines off by default

PINE_FILE="${1:-/Users/suman/code/mala_v2/research/results/playbooks/mean_reversion_at_extremes/current/tradingview_visual_review/iwm_2026-05-06_2026-05-12/mala_iwm_playbook_review.pine}"
TV_SYMBOL="${TV_SYMBOL:-NYSEARCA:IWM}"
TV_TIMEFRAME="${TV_TIMEFRAME:-1}"
TV_SCROLL_TO="${TV_SCROLL_TO:-2026-05-06T10:11:00-04:00}"

: "${TRADINGVIEW_MCP_ROOT:=/Users/suman/code/openclaw-core/workspace-main/external/tradingview-mcp}"
: "${TRADINGVIEW_CDP_HOST:=127.0.0.1}"
: "${TRADINGVIEW_CDP_PORT:=9223}"

cd "$TRADINGVIEW_MCP_ROOT"
export TRADINGVIEW_CDP_HOST TRADINGVIEW_CDP_PORT

npm run -s tv -- status
npm run -s tv -- symbol "$TV_SYMBOL"
npm run -s tv -- timeframe "$TV_TIMEFRAME"
npm run -s tv -- type Candles
npm run -s tv -- scroll "$TV_SCROLL_TO"
npm run -s tv -- pine check --file "$PINE_FILE"

for attempt in 1 2 3; do
  npm run -s tv -- ui panel pine-editor open || true
  sleep "$attempt"
  if npm run -s tv -- pine set --file "$PINE_FILE"; then
    break
  fi
  if [[ "$attempt" == "3" ]]; then
    echo "Could not inject Pine source through the MCP bridge." >&2
    echo "Open the Pine Editor and paste this file manually:" >&2
    echo "$PINE_FILE" >&2
    exit 1
  fi
done

npm run -s tv -- pine compile
