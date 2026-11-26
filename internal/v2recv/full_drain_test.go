package v2recv_test

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

type fullDrainOwner struct {
	rows    int
	seals   atomic.Int32
	drained chan int
	err     error
}

func (o *fullDrainOwner) Append(_ []string, _ []uint64, rows []points.Point, _ []uint64, _ int64) error {
	o.rows += len(rows)
	return nil
}
func (o *fullDrainOwner) Seal() error { o.seals.Add(1); return nil }
func (o *fullDrainOwner) Drain() error {
	select {
	case o.drained <- o.rows:
	default:
	}
	return o.err
}

// Full receiver batches must reach core without waiting for the idle drain
// clock. They must not request a seal, and a failed drain must fail intake.
func TestFullInboxDrainsWithoutTicker(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			o := &fullDrainOwner{drained: make(chan int, 1)}
			if fail {
				o.err = errors.New("drain failed")
			}
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			s, err := v2recv.Start(l, []v2recv.Owner{o}, v2recv.Config{BatchSize: 2, SyncInterval: time.Hour, Logger: quiet()})
			if err != nil {
				l.Close()
				t.Fatal(err)
			}
			defer s.Wait(t.Context())
			c, err := net.Dial("tcp", l.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = fmt.Fprint(c, "drain.first 1 123\ndrain.second 2 124\n"); err != nil {
				c.Close()
				t.Fatal(err)
			}
			c.Close()
			select {
			case n := <-o.drained:
				if n != 2 {
					t.Fatalf("drained %d points, want 2", n)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("full inbox waited for producer drain tick")
			}
			if n := o.seals.Load(); n != 0 {
				t.Fatalf("full inbox requested %d seals", n)
			}
			err = s.Wait(t.Context())
			if !errors.Is(err, o.err) {
				t.Fatalf("Wait: %v, want %v", err, o.err)
			}
			if fail && s.Stats().SinkErrors != 1 {
				t.Fatalf("lost drain failure: %+v", s.Stats())
			}
		})
	}
}
