package downsize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type resumeParallelBackend struct {
	*fakeSync
	budget  int64
	entered atomic.Int64
	gate    chan struct{}
	live    *syncLiveBackend
	healthy atomic.Int64
}

func (b *resumeParallelBackend) Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	if store == syncT {
		n := b.entered.Add(1)
		if n == b.budget {
			close(b.gate)
		}
		if n <= b.budget {
			select {
			case <-b.gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return b.fakeSync.Info(ctx, store, names)
}

func (b *resumeParallelBackend) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	if store == syncT && names[0] == "metric.00" {
		return b.live.Digest(ctx, "http://stuck:4242", names, from, until)
	}
	if store == syncT {
		b.healthy.Add(int64(len(names)))
	}
	return b.fakeSync.Digest(ctx, store, names, from, until)
}

func TestSyncLegacyResumeParallelAndStuckRead(t *testing.T) {
	for _, round := range []int{2, 4} {
		for _, verifyStreams := range []int{0, 2} {
			t.Run(fmt.Sprintf("round%d/verify%d", round, verifyStreams), func(t *testing.T) {
				s, j := phaseResumeFixture(t)
				s.o.streams, s.o.verifyStreams, s.o.batch = 3, verifyStreams, 1
				s.o.rescanAll, s.o.maxRounds = false, 0
				b := &resumeParallelBackend{fakeSync: newFakeSync(), budget: int64(s.o.verifyConcurrency()), gate: make(chan struct{})}
				b.live = newSyncLiveBackend(syncLiveOptions{})
				b.live.readTimeout = 100 * time.Millisecond
				var healthyBeforeTimeout atomic.Bool
				b.live.m.client.Transport = syncInfoRoundTrip(func(r *http.Request) (*http.Response, error) {
					<-r.Context().Done()
					healthyBeforeTimeout.Store(b.healthy.Load() > 0)
					return nil, r.Context().Err()
				})
				var names []string
				for i := 0; i < 12; i++ {
					n := fmt.Sprintf("metric.%02d", i)
					names = append(names, n)
					for _, host := range []string{syncT, syncD1} {
						b.put(host, n, []syncArchive{{60, 10000}})
					}
				}
				var union strings.Builder
				for _, n := range names {
					fmt.Fprintf(&union, "%s\t1\t1\n", n)
				}
				if err := os.WriteFile(s.unionPath(), []byte(union.String()), 0600); err != nil {
					t.Fatal(err)
				}
				s.sum.Discovered = len(names)
				s.b = b
				// Entered legacy union round beyond the new default limit. Earlier
				// completed receipts have bounds which no longer prove exact membership.
				if err := j.append(syncRow{Kind: "verified", Round: 1, First: names[0], Last: names[len(names)-1], Names: 1}); err != nil {
					t.Fatal(err)
				}
				if err := j.append(syncRow{Kind: "verified", Round: round, First: names[0], Last: names[6], Names: 7}); err != nil {
					t.Fatal(err)
				}
				if err := s.savePhase("verify", round, 7, len(names)); err != nil {
					t.Fatal(err)
				}
				s = restartPhaseFixture(t, s)
				s.b = b
				if err := s.restorePhase(); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				set, err := s.setSource(names)
				if err != nil {
					t.Fatal(err)
				}
				holes, unknown, skips, err := s.verifyRound(ctx, 1, set)
				if err != nil || len(holes)+len(unknown) != 0 || len(skips) != 1 || skips[0].Name != names[0] || !strings.Contains(skips[0].Reason, "deadline") {
					t.Fatalf("holes=%v unknown=%v skips=%v error=%v", holes, unknown, skips, err)
				}
				if !healthyBeforeTimeout.Load() || b.entered.Load() < 12 {
					t.Fatal("stuck read blocked healthy siblings")
				}
				if s.resume.Round != round || s.prog.durableFloor != 7 || s.prog.done.Load() != 0 {
					t.Fatal("saved round progress changed", s.resume)
				}
				before := b.entered.Load()
				set, err = s.setSource(names)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, _, err = s.verifyRound(ctx, 1, set); err != nil {
					t.Fatal(err)
				}
				if b.entered.Load() != before {
					t.Fatal("replayed durable recovery issued new reads")
				}
			})
		}
	}
}

func TestSyncReadOperationBoundsRestartRetries(t *testing.T) {
	for _, mode := range []string{"stuck_headers", "stuck_body", "restart"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				calls.Add(1)
				switch mode {
				case "restart":
					http.Error(w, "buckyd is stopping", 503)
				case "stuck_body":
					w.WriteHeader(200)
					_, _ = w.Write([]byte("["))
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				default:
					<-r.Context().Done()
				}
			}))
			defer server.Close()
			b := newSyncLiveBackend(syncLiveOptions{})
			defer b.m.client.CloseIdleConnections()
			b.readTimeout = 80 * time.Millisecond
			b.retryTick = time.Millisecond
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if _, err := b.Digest(ctx, server.URL, []string{"a"}, 60, 180); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if ctx.Err() != nil || b.waiting.Load() != 0 {
				t.Fatal("operation exhausted its parent or leaked wait")
			}
			if mode == "restart" && calls.Load() < 2 {
				t.Fatal("transient failure never retried")
			}
		})
	}
}

type deadlineMetadataBackend struct{ *proofFixture }

func (b *deadlineMetadataBackend) Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	if store == syncT && names[0] == "metric.00" {
		return nil, context.DeadlineExceeded
	}
	return b.fakeSync.Info(ctx, store, names)
}

func TestSyncFinishFirstMetadataTimeoutResidual(t *testing.T) {
	s, _ := phaseResumeFixture(t)
	s.o.streams, s.o.batch = 3, 1
	s.o.rescanAll = false
	f := &proofFixture{fakeSync: newFakeSync(), epoch: "one", unknown: map[string]bool{}, seen: map[string]bool{}}
	var union strings.Builder
	for i := 0; i < 8; i++ {
		n := fmt.Sprintf("metric.%02d", i)
		fmt.Fprintf(&union, "%s\t1\t1\n", n)
		for _, h := range []string{syncT, syncD1} {
			f.put(h, n, []syncArchive{{60, 10000}})
		}
	}
	if err := os.WriteFile(s.unionPath(), []byte(union.String()), 0600); err != nil {
		t.Fatal(err)
	}
	s.sum.Discovered = 8
	s.b = f
	if err := s.openFrontier(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer s.frontier.file.Close()
	s.b = &deadlineMetadataBackend{f}
	holes, unknown, skips, err := s.verifyRound(t.Context(), 1, s.unionSource())
	if err != nil || len(holes)+len(unknown) != 0 || len(skips) != 1 || skips[0].Name != "metric.00" {
		t.Fatalf("holes=%v unknown=%v skips=%v err=%v", holes, unknown, skips, err)
	}
	if s.prog.done.Load() != 8 {
		t.Fatal("metadata timeout stranded round", s.prog.done.Load())
	}
	rows := s.j.find(func(r syncRow) bool { return r.Kind == "verified" && r.First == "metric.00" })
	if len(rows) != 1 || rows[0].ObservationProof != "" || len(rows[0].HoleProofs) != 0 {
		t.Fatal("timeout sealed as clean", rows)
	}
}
