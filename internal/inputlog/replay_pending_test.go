package inputlog

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestReplayPendingBytes(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	l := openStarted(t, dir, Options{SegmentSize: 1 << 20}, src)
	if got := l.ReplayPendingBytes(); got != 0 {
		t.Fatal("fresh journal backlog", got)
	}
	src.add("pending.metric", 1, 1700000000)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	l.Kill()
	l, err := Open(dir, withTestDefaults(Options{SegmentSize: 1 << 20}))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var want int64
	for _, f := range l.pending {
		want += int64(frameHeaderSize) + int64(f.h.length)
	}
	if want == 0 || l.ReplayPendingBytes() != want {
		t.Fatal("recovered journal backlog", l.ReplayPendingBytes(), want)
	}
	seen := false
	if _, err := l.Replay(func(*points.Points) {
		seen = true
		if got := l.ReplayPendingBytes(); got == 0 {
			t.Error("in-flight frame excluded from backlog")
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !seen || l.ReplayPendingBytes() != 0 {
		t.Fatal("completed journal backlog", seen, l.ReplayPendingBytes())
	}
}
