# Running triage-sentinel as a service

The sentinel runs as a **user LaunchAgent** labelled `com.b1codes.sentinel`.

## Install

    make build
    cp .env.example .env && chmod 600 .env
    ./bin/sentinel hash-password        # paste the hash into .env
    cp projects.example.yaml projects.yaml
    make install-service

`install.sh` refuses to install unless the binary exists, `.env` is mode 600,
`projects.yaml` exists, and `sentinel validate` passes — a service that cannot
start produces a crash loop that is harder to diagnose than an upfront refusal.

## Operate

| Task | Command |
|---|---|
| Status | `make service-status` |
| Logs | `make logs` |
| Restart | `launchctl kickstart -k gui/$(id -u)/com.b1codes.sentinel` |
| Stop | `launchctl bootout gui/$(id -u)/com.b1codes.sentinel` |
| Reload `projects.yaml` without restarting | `kill -HUP $(pgrep -f 'sentinel serve')` |
| Upgrade after a code change | `make install-service` (boots out and re-bootstraps) |
| Remove | `make uninstall-service` |

## Restart behaviour

`KeepAlive.SuccessfulExit=false` restarts the service only on a non-zero exit.
A clean `SIGTERM` shutdown exits 0 and is treated as intentional, so
`launchctl bootout` actually stops the service instead of triggering an
immediate relaunch. `ThrottleInterval=10` prevents a crash loop from spinning.

## Restarting the host

**FileVault decides whether the host returns unattended — not the
LaunchAgent/LaunchDaemon choice.** With FileVault on, nothing on the disk runs
until the volume is unlocked at the console, and macOS will not enable automatic
login at all. A LaunchDaemon does not change this: it starts without *login*, but
still only after the disk is *unlocked*.

| Restart cause | Behaviour | What to do |
|---|---|---|
| Planned restart or OS update | Halts at the unlock prompt | `sudo fdesetup authrestart` — caches the unlock key in memory for one restart, so it returns unattended |
| Power loss or kernel panic | Halts at the unlock prompt | Someone must unlock at the console. `pmset autorestart 1` powers the Mini back on, but cannot unlock it |

Always use `sudo fdesetup authrestart` in place of `sudo reboot` on this host.
Verify it worked with `curl -s http://127.0.0.1:8787/api/health` — no manual step
should have been needed.

**Deliberate trade-off:** FileVault stays on because this host holds the
Anthropic API key, a GitHub token with write access, GCP credentials, and local
clones of 25+ private repositories. Disabling it would make the sentinel fully
self-recovering from power loss at the cost of handing all of that to anyone with
physical access. The accepted consequence is that an unexpected power event
requires a manual unlock.

## Sleep

A sleeping host is a stopped daemon. Required on the deployment host:

    sudo pmset -a sleep 0 disksleep 0 autorestart 1

`install.sh` warns if system sleep is non-zero. `displaysleep` is safe to leave
enabled — the display sleeping does not suspend the process.

## LaunchAgent vs LaunchDaemon

The LaunchAgent runs in your GUI session: no root, and it inherits your keychain
and `PATH`, which M4 needs for git credentials.

To convert to a LaunchDaemon: move the plist to `/Library/LaunchDaemons`,
`chown root:wheel`, add a `UserName` key, and bootstrap into `system` instead of
`gui/$(id -u)`. It then starts without a login session — but as shown above this
does **not** improve FileVault reboot behaviour, and git and GCP credentials must
be provisioned separately for that user. Not recommended for this deployment.

## Secrets

No secret is ever written to the plist — a plist is world-readable. Credentials
live only in `.env` (mode 600), passed via `--env-file` (SPEC §14).
