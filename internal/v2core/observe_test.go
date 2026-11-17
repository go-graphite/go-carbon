package v2core

import (
	"testing"
	"time"
)

func TestObserveBufferedAndDurableSeal(t *testing.T) {
	e, err := Open(t.TempDir(), coreTestConfig(), Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.Append(0, []Row{{ID: 1, Time: 1, Seq: 1}}, 1, false); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		s := e.Observe()
		if s.BufferedPoints == 1 && s.SealLagSeconds > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("buffered head not visible: %+v", s)
		}
		time.Sleep(time.Millisecond)
	}
	if err := e.Append(0, []Row{{ID: 1, Time: 2, Seq: 2}}, 2, true); err != nil {
		t.Fatal(err)
	}
	s := e.Observe()
	if s.CommittedPoints != 2 || s.BufferedPoints != 0 || s.SealLagSeconds != 0 || s.Seals == 0 || s.SealNanoseconds == 0 {
		t.Fatalf("durable seal observation: %+v", s)
	}
}
