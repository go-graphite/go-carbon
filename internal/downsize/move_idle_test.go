package downsize

import (
	"context"
	"errors"
	"io"
	"net/http/httptrace"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// The watchdog's deadline is virtual: scheduler and disk latency cannot decide
// whether a healthy transfer is idle. Integration tests cover split/resume.
func TestIdleWatchProgressAndExpiry(t *testing.T) {
	t.Parallel()
	for _, progress := range []string{"read", "request", "response"} {
		t.Run(progress, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, watch, stop := watchIdle(t.Context(), time.Second)
				defer stop()
				for range 3 {
					time.Sleep(750 * time.Millisecond)
					switch progress {
					case "read":
						_, err := io.ReadAll(idleReader{Reader: strings.NewReader("payload"), watch: watch})
						if err != nil {
							t.Fatal(err)
						}
					case "request":
						httptrace.ContextClientTrace(ctx).WroteRequest(httptrace.WroteRequestInfo{})
					case "response":
						httptrace.ContextClientTrace(ctx).GotFirstResponseByte()
					}
					if ctx.Err() != nil {
						t.Fatal("progress did not renew deadline", context.Cause(ctx))
					}
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if !errors.Is(context.Cause(ctx), errTransferIdleCause) {
					t.Fatal("idle deadline did not expire", context.Cause(ctx))
				}
			})
		})
	}
}

func TestIdleWatchStopPreservesParentCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		ctx, _, stop := watchIdle(parent, time.Second)
		cancel()
		stop()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if !errors.Is(context.Cause(ctx), context.Canceled) {
			t.Fatal(context.Cause(ctx))
		}
	})
}
