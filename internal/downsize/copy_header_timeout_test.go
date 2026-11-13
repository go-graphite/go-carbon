package downsize

import (
	"testing"
	"time"
)

// A pull or verify sends no bytes until the target finished the batch, so
// the idle bound (which splits a slow batch) must fire before the response
// header deadline (a transport error that retries the batch at full size).
func TestCopyHeaderTimeoutStaysBehindIdleBound(t *testing.T) {
	for _, idle := range []time.Duration{time.Second, 80 * time.Second, defaultTransferIdle, 5 * time.Minute} {
		if got := copyHeaderTimeout(idle); got <= idle || got < copyHeaderTimeoutMin {
			t.Fatalf("idle %s: header timeout %s fires first or below the %s floor", idle, got, copyHeaderTimeoutMin)
		}
	}
	if got := copyHeaderTimeout(0); got != copyHeaderTimeoutMin {
		t.Fatalf("idle bound disabled: header timeout %s, want %s", got, copyHeaderTimeoutMin)
	}
	if copyDefaultBatchBytes != 16<<20 {
		t.Fatalf("default batch bytes %d", copyDefaultBatchBytes)
	}
}
