// Command sentinel is the triage-sentinel control plane.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	// Embed the IANA timezone database into the binary so time.LoadLocation
	// (used to resolve SENTINEL_TIMEZONE) does not depend on the host having
	// /usr/share/zoneinfo, even though the binary is built with CGO_ENABLED=0.
	_ "time/tzdata"
)

func main() {
	// SIGHUP is handled inside serve for configuration reload, so it is
	// deliberately not part of this cancellation set.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "sentinel: %v\n", err)
		os.Exit(1)
	}
}
