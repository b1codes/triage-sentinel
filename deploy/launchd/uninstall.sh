#!/usr/bin/env bash
# Remove the triage-sentinel LaunchAgent. Leaves var/ and configuration alone.
set -euo pipefail

LABEL="com.b1codes.sentinel"
DOMAIN="gui/$(id -u)"
PLIST_DEST="$HOME/Library/LaunchAgents/$LABEL.plist"

launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
rm -f "$PLIST_DEST"

printf 'removed %s\n' "$PLIST_DEST"
