package contentiontest

import (
	"sync"
	"testing"
	"time"
)

func TestWaitBudgetDetectsGlobalLock(t *testing.T) {
	var mu sync.Mutex
	wait, wall := Measure(256, 1, func(int, int) {
		mu.Lock()
		time.Sleep(50 * time.Microsecond)
		mu.Unlock()
	})
	rate := wait / wall
	t.Logf("injected global lock: %.1f wait seconds/wall second", rate)
	if rate <= 48 {
		t.Fatalf("wait budget failed to detect deliberate global serialization: %.1f s/s", rate)
	}
}
