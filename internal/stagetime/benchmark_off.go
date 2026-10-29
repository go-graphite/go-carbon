//go:build !phasecpubench

package stagetime

import "time"

// The empty call is inlined away in ordinary builds.
func observeBenchmarkUpdate(time.Duration) {}
