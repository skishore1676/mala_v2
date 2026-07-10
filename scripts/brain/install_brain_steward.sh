#!/bin/bash
# Install (or reinstall) the nightly brain-steward launchd job on the DEV Mac.
# Usage: bash scripts/brain/install_brain_steward.sh [install|uninstall|status]
set -euo pipefail

LABEL="com.mala.brain-steward"
SRC="$(cd "$(dirname "$0")" && pwd)/launchd/${LABEL}.plist"
DEST="$HOME/Library/LaunchAgents/${LABEL}.plist"
DOMAIN="gui/$(id -u)"

case "${1:-install}" in
  install)
    mkdir -p "$HOME/Library/Logs/mala-brain-steward"
    launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
    cp "$SRC" "$DEST"
    launchctl bootstrap "$DOMAIN" "$DEST"
    launchctl list | grep "$LABEL" && echo "installed: nightly 21:45 local"
    ;;
  uninstall)
    launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
    rm -f "$DEST"
    echo "uninstalled"
    ;;
  status)
    launchctl list | grep "$LABEL" || echo "not loaded"
    tail -n 20 "$HOME/Library/Logs/mala-brain-steward/steward.log" 2>/dev/null || true
    ;;
  *)
    echo "usage: $0 [install|uninstall|status]" >&2; exit 2;;
esac
