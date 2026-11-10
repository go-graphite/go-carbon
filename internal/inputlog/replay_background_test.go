package inputlog

import (
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestStartWithReplayPinsOriginalDuringLiveCommits(t *testing.T) {
	dir := t.TempDir()
	source := newFakeSource()
	l, _, _ := reopen(t, dir, Options{}, source)
	source.add("same", 1, 100)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	l.Kill()
	l, err := Open(dir, withTestDefaults(Options{}))
	if err != nil {
		t.Fatal(err)
	}
	source = newFakeSource()
	source.setLow(1000, false) // even an otherwise empty/persisted cache cannot retire the reader
	read := l.StartWithReplay(source, testShards)
	source.add("same", 2, 100) // live correction must follow original on the next crash
	for range 3 {
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	var old []points.Point
	stats, err := read(func(p *points.Points) { old = append(old, p.Data...) }, nil)
	if err != nil || stats.Points != 1 || !slices.Equal(old, []points.Point{{Value: 1, Timestamp: 100}}) {
		t.Fatalf("snapshot read: %v %v %v", old, stats, err)
	}
	l.Kill() // crash while history retention is still owned
	l, got, _ := reopen(t, dir, Options{}, newFakeSource())
	defer l.Close()
	if want := []points.Point{{Value: 1, Timestamp: 100}, {Value: 2, Timestamp: 100}}; !slices.Equal(got["same"], want) {
		t.Fatalf("second replay %v, want %v", got, want)
	}
}
