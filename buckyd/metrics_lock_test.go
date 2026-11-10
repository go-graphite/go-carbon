package buckyd

import (
	"testing"
	"time"
)

func TestReadinessDoesNotWaitForSecretIO(t *testing.T) {
	s := &Service{}
	s.authPending.Store(true)
	s.mu.Lock() // the secret-delivery retry reads its file under mu
	done := make(chan bool, 1)
	go func() { done <- s.AuthSecretReady() }()
	select {
	case ready := <-done:
		s.mu.Unlock()
		if ready {
			t.Fatal("missing secret reported ready")
		}
	case <-time.After(time.Second):
		s.mu.Unlock()
		<-done
		t.Fatal("readiness metric waited for secret filesystem I/O")
	}
	s.authPending.Store(false)
	if !s.AuthSecretReady() {
		t.Fatal("published secret not reported ready")
	}
}
