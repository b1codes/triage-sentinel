#!/usr/bin/env bash
# Install triage-sentinel as a user LaunchAgent.
set -euo pipefail

LABEL="com.b1codes.sentinel"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DOMAIN="gui/$(id -u)"
PLIST_SRC="$REPO_DIR/deploy/launchd/$LABEL.plist"
PLIST_DEST="$HOME/Library/LaunchAgents/$LABEL.plist"

fail() { printf 'install: %s\n' "$1" >&2; exit 1; }

# Preflight. Installing a service that cannot start produces a crash loop that
# is harder to diagnose than an upfront refusal.
[ -x "$REPO_DIR/bin/sentinel" ] || fail "bin/sentinel not found — run: make build"
[ -f "$REPO_DIR/.env" ]         || fail ".env not found — copy .env.example and fill it in"
[ -f "$REPO_DIR/projects.yaml" ] || fail "projects.yaml not found — copy projects.example.yaml"

perms="$(stat -f '%Lp' "$REPO_DIR/.env")"
[ "$perms" = "600" ] || fail ".env has mode $perms — run: chmod 600 .env"

"$REPO_DIR/bin/sentinel" validate --env-file "$REPO_DIR/.env" --config "$REPO_DIR/projects.yaml" \
  || fail "configuration is invalid; fix it before installing the service"

mkdir -p "$REPO_DIR/var/log" "$HOME/Library/LaunchAgents"

sed "s|__INSTALL_DIR__|$REPO_DIR|g" "$PLIST_SRC" > "$PLIST_DEST"
chmod 644 "$PLIST_DEST"

# bootout first so re-running this script upgrades a live service cleanly.
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
launchctl bootstrap "$DOMAIN" "$PLIST_DEST"
launchctl enable "$DOMAIN/$LABEL"

printf 'installed %s\n' "$PLIST_DEST"

# Reboot-survival advisory. FileVault, not the LaunchAgent/LaunchDaemon choice,
# decides whether the host comes back unattended.
if /usr/bin/fdesetup status 2>/dev/null | grep -q 'FileVault is On'; then
  cat >&2 <<'WARN'

note: FileVault is on, so automatic login is unavailable and the disk must be
unlocked at the console after any cold boot.

  Planned restarts:  sudo fdesetup authrestart   (returns unattended)
  Power loss/panic:  requires someone at the console

See deploy/launchd/README.md.
WARN
elif ! /usr/bin/defaults read /Library/Preferences/com.apple.loginwindow autoLoginUser >/dev/null 2>&1; then
  cat >&2 <<'WARN'

warning: FileVault is off but automatic login is also disabled.

A LaunchAgent starts only after a user logs in, so the sentinel will NOT return
after an unattended reboot. Enable automatic login in
System Settings > Users & Groups.
WARN
fi

# A sleeping host is a stopped daemon (SPEC §1.3).
sleep_setting="$(pmset -g 2>/dev/null | awk '$1=="sleep"{print $2; exit}')"
if [ -n "$sleep_setting" ] && [ "$sleep_setting" != "0" ]; then
  printf '\nwarning: system sleep is %s minutes — the sentinel stops while asleep.\n' "$sleep_setting" >&2
  printf '  fix: sudo pmset -a sleep 0 disksleep 0 autorestart 1\n\n' >&2
fi

sleep 2
launchctl print "$DOMAIN/$LABEL" | sed -n '1,12p'
