package persister

import (
	"fmt"
	"testing"
	"time"
)

func TestStoreReservationsSeparateCollidingNames(t *testing.T) {
	p := &Whisper{}
	name := "held"
	collision := ""
	for i := 0; ; i++ {
		collision = fmt.Sprintf("collision.%d", i)
		if storeMutexIndex(collision) == storeMutexIndex(name) {
			break
		}
	}
	release := p.lockStoreMetrics([]string{name})
	independent := make(chan struct{})
	go func() { unlock := p.lockStoreMetrics([]string{collision}); unlock(); close(independent) }()
	select {
	case <-independent:
	case <-time.After(time.Second):
		release()
		t.Fatal("unrelated name waited on storage work")
	}
	waiting := make(chan struct{})
	go func() { unlock := p.lockStoreMetrics([]string{collision, name}); unlock(); close(waiting) }()
	select {
	case <-waiting:
		release()
		t.Fatal("same-name write overtook owner")
	case <-time.After(10 * time.Millisecond):
	}
	release()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("reservation not released")
	}
}
