package v2daemon

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// These fixtures send only a few KiB per producer. Keep the full producer
	// and owner geometry without allocating production-sized dispatch rings.
	// Set this before tests start so parallel tests never mutate the environment.
	if os.Getenv("CORE_STREAM_BYTES") == "" {
		if err := os.Setenv("CORE_STREAM_BYTES", "65536"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}
