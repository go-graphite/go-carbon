package carbon

import (
	"errors"
	"fmt"
	"testing"
)

// Keep the old three-channel select as the cancellation oracle. All cases
// return the same error, so choosing among closed channels cannot affect it.
func referenceCaptureCancellation(s *startupInput, cancel <-chan struct{}) error {
	select {
	case <-cancel:
		return errStartupReplayStopped
	case <-s.stop:
		return errStartupReplayStopped
	case <-s.replayStop:
		return errStartupReplayStopped
	default:
		return nil
	}
}

func TestCaptureCancellationDifferential(t *testing.T) {
	channel := func(state int) chan struct{} {
		if state == 0 {
			return nil
		}
		ch := make(chan struct{})
		if state == 2 {
			close(ch)
		}
		return ch
	}
	for a := range 3 {
		for b := range 3 {
			for c := range 3 {
				s := &startupInput{stop: channel(a), replayStop: channel(b)}
				cancel := channel(c)
				if got, want := s.waitCaptureUntil(cancel), referenceCaptureCancellation(s, cancel); !errors.Is(got, want) {
					t.Fatalf("states %d/%d/%d: got %v want %v", a, b, c, got, want)
				}
			}
		}
	}
}

func BenchmarkCaptureCancellation(b *testing.B) {
	s := &startupInput{stop: make(chan struct{}), replayStop: make(chan struct{})}
	for _, parallelism := range []int{1, 4} {
		b.Run(fmt.Sprint(parallelism), func(b *testing.B) {
			b.SetParallelism(parallelism)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := s.waitCapture(); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
