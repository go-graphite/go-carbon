package carbon

import (
	"errors"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
)

func TestCaptureAdmissionUsesBytesAndWakesOnStop(t *testing.T) {
	s := &startupInput{stop: make(chan struct{})}
	j := &startupJournal{owner: s, budget: captureByteBudget{limit: 200000, changed: make(chan struct{})}}
	release, err := j.acquire(100, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// A larger batch waits although only one caller currently holds admission.
	done := make(chan error, 1)
	go func() {
		r, err := j.acquire(30000, 1)
		if err == nil {
			r()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatal("byte budget did not block", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(s.stop)
	if err := <-done; !errors.Is(err, inputlog.ErrClosed) {
		t.Fatal(err)
	}
	if j.budget.peak.Load() > j.budget.limit {
		t.Fatal("budget exceeded")
	}
}
