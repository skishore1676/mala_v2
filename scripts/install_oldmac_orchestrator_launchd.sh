#!/usr/bin/env bash
set -euo pipefail

LABEL="${LABEL:-com.mala.research-orchestrator}"
REPO_DIR="${REPO_DIR:-/Users/sunny/Documents/mala_v2}"
SHEET_ID="${SHEET_ID:-1qzXNn8ezagqeDR9EI9hoUTzhANKARk4jG4pdy8-32T0}"
GOOGLE_CREDENTIALS="${GOOGLE_CREDENTIALS:-/Users/sunny/.openclaw/credentials/sheets_sa.json}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-900}"
PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="${REPO_DIR}/data/results/research_ops/orchestrator"

mkdir -p "${HOME}/Library/LaunchAgents" "${LOG_DIR}"

cat > "${PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>WorkingDirectory</key>
  <string>${REPO_DIR}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${REPO_DIR}/.venv/bin/python</string>
    <string>-m</string>
    <string>src.research.local_orchestrator</string>
    <string>daemon</string>
    <string>--mode</string>
    <string>apply-safe</string>
    <string>--with-control-sheet</string>
    <string>--with-intake-sheet</string>
    <string>--interval-seconds</string>
    <string>${INTERVAL_SECONDS}</string>
    <string>--control-sheet-id</string>
    <string>${SHEET_ID}</string>
    <string>--control-sheet-name</string>
    <string>Research_Control</string>
    <string>--control-google-credentials</string>
    <string>${GOOGLE_CREDENTIALS}</string>
    <string>--intake-sheet-id</string>
    <string>${SHEET_ID}</string>
    <string>--intake-sheet-name</string>
    <string>Research_Intake</string>
    <string>--intake-google-credentials</string>
    <string>${GOOGLE_CREDENTIALS}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/daemon.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/daemon.err.log</string>
</dict>
</plist>
PLIST

launchctl unload "${PLIST}" >/dev/null 2>&1 || true
launchctl load "${PLIST}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true

echo "LAUNCHD_LABEL=${LABEL}"
echo "LAUNCHD_PLIST=${PLIST}"
echo "ORCHESTRATOR_LOG=${LOG_DIR}/daemon.log"
echo "ORCHESTRATOR_ERR_LOG=${LOG_DIR}/daemon.err.log"
