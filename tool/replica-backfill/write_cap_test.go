package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

type capConnection struct {
	calls int
	fail  bool
}

func (c *capConnection) Write(p []byte) (int, error) {
	c.calls++
	if c.fail {
		return len(p) / 2, errors.New("ambiguous partial write")
	}
	return len(p), nil
}
func (*capConnection) Read([]byte) (int, error)         { return 0, errors.New("unused") }
func (*capConnection) Close() error                     { return nil }
func (*capConnection) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*capConnection) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*capConnection) SetDeadline(time.Time) error      { return nil }
func (*capConnection) SetReadDeadline(time.Time) error  { return nil }
func (*capConnection) SetWriteDeadline(time.Time) error { return nil }

func TestWriterPointCapIncludesAmbiguousRetries(t *testing.T) {
	c := &capConnection{fail: true}
	w := newWriter("127.0.0.1:0", 0, 2, nil, 5, 0, 0)
	w.conn = c
	w.maxPoints = 3
	w.buf = []byte("a 1 1\nb 2 2\n")
	w.bufPts = 2
	err := w.flush(context.Background())
	if err == nil || !strings.Contains(err.Error(), "max-write-points") {
		t.Fatalf("want cap failure after ambiguous send, got %v", err)
	}
	if c.calls != 1 || w.stats.attempted.Load() != 2 || w.stats.points.Load() != 0 {
		t.Fatalf("calls %d attempted %d acknowledged %d", c.calls, w.stats.attempted.Load(), w.stats.points.Load())
	}
}

func TestWriterPointCapIsCumulativeAndFailsBeforeSend(t *testing.T) {
	c := &capConnection{}
	w := newWriter("unused", 0, 2, nil, 0, 0, 0)
	w.conn = c
	w.maxPoints = 3
	for _, n := range []int{2, 1} {
		w.buf = []byte("fixture\n")
		w.bufPts = n
		if err := w.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	w.buf = []byte("overflow\n")
	w.bufPts = 1
	if err := w.flush(context.Background()); err == nil || !strings.Contains(err.Error(), "max-write-points") {
		t.Fatal("cap not enforced", err)
	}
	if c.calls != 2 || w.stats.attempted.Load() != 3 || w.stats.points.Load() != 3 {
		t.Fatal("writes exceeded cap")
	}
}

func TestWriterPointCapRejectsOversizedFirstFlush(t *testing.T) {
	c := &capConnection{}
	w := newWriter("unused", 0, 2, nil, 0, 0, 0)
	w.conn = c
	w.maxPoints = 1
	w.buf = []byte("fixture\n")
	w.bufPts = 2
	if err := w.flush(context.Background()); err == nil {
		t.Fatal("oversized first flush accepted")
	}
	if c.calls != 0 || w.stats.attempted.Load() != 0 {
		t.Fatal("oversized flush touched socket")
	}
}

func TestWriteCapFlagAndRun(t *testing.T) {
	for _, n := range []string{"0", "1", "1999"} {
		o, err := parseFlags([]string{"-replica", "http://example.invalid", "-since", "1h", "-max-write-points", n})
		if err != nil || o.maxWritePoints < 0 {
			t.Fatal(n, err)
		}
	}
	if _, err := parseFlags([]string{"-replica", "http://example.invalid", "-since", "1h", "-max-write-points", "-1"}); err == nil {
		t.Fatal("negative cap accepted")
	}
	rep, o := writerFailureOptions(t)
	in := newIngest(t, nil)
	o.ingest = in.ln.Addr().String()
	o.maxWritePoints = 1
	report, err := run(context.Background(), o, time.Now())
	if err == nil || !strings.Contains(err.Error(), "max-write-points") {
		t.Fatal("run did not fail on cap", err)
	}
	if report.Write["points_sent"] != 1 || report.Write["points_attempted"] != 1 {
		t.Fatalf("write report %v", report.Write)
	}
	rep.mu.Lock()
	n := len(rep.requests)
	rep.mu.Unlock()
	if n > 4 {
		t.Fatalf("continued fetching after cap: %d batches", n)
	}
}
