package inputlog

import (
	"fmt"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestReplayCursorResumesInsideFrame(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	src.refs = make(map[string]*StreamRef)
	log := openStarted(t, dir, Options{SegmentSize: 1 << 20}, src)
	for cycle := 0; cycle < 3; cycle++ {
		for i := 0; i < 40; i++ {
			src.add(fmt.Sprintf("cursor.metric.%03d", i), float64(cycle*40+i), int64(cycle*40+i))
		}
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	log.Kill()
	log, err := Open(dir, withTestDefaults(Options{SegmentSize: 1 << 20}))
	if err != nil {
		t.Fatal(err)
	}
	cursors := make(map[uint16]ReplayCursor)
	var mu sync.Mutex
	var saved []points.Point
	_, err = log.ReplayStoppedFrom(func(p *points.Points) bool {
		mu.Lock()
		defer mu.Unlock()
		if len(saved) >= 37 {
			return false
		}
		saved = append(saved, p.Data...)
		return true
	}, nil, nil, func(part uint16, cursor ReplayCursor) { mu.Lock(); cursors[part] = cursor; mu.Unlock() })
	if err != nil {
		t.Fatal(err)
	}
	if len(saved) == 0 || len(saved) >= 120 {
		t.Fatal("fixture did not stop within journal", len(saved))
	}
	log.Kill()
	log, err = Open(dir, withTestDefaults(Options{SegmentSize: 1 << 20}))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	var resumed []points.Point
	stats, err := log.ReplayStoppedFrom(func(p *points.Points) bool { resumed = append(resumed, p.Data...); return true }, nil, cursors, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Corrupt != 0 || stats.Points != int64(len(resumed)) || len(saved)+len(resumed) != 120 {
		t.Fatal("journal cursor replay counts", len(saved), len(resumed), stats)
	}
	for i, p := range append(saved, resumed...) {
		if p.Value != float64(i) || p.Timestamp != int64(i) {
			t.Fatal(fmt.Sprintf("record %d: %#v", i, p))
		}
	}
}

func TestReplayCursorDoesNotAliasRetiredJournal(t *testing.T) {
	dir := t.TempDir()
	opts := withTestDefaults(Options{SegmentSize: 1 << 20})
	src := newFakeSource()
	log := openStarted(t, dir, opts, src)
	src.add("journal.old", 1, 100)
	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}
	log.Kill()
	log, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	cursors := make(map[uint16]ReplayCursor)
	if _, err = log.ReplayStoppedFrom(func(*points.Points) bool { return true }, nil, nil, func(part uint16, c ReplayCursor) { cursors[part] = c }); err != nil {
		t.Fatal(err)
	}
	if err = log.Retire(); err != nil {
		t.Fatal(err)
	}
	src = newFakeSource()
	log = openStarted(t, dir, opts, src)
	src.add("journal.new", 2, 200)
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	log.Kill()
	log, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	var got []points.Point
	if _, err = log.ReplayStoppedFrom(func(p *points.Points) bool { got = append(got, p.Data...); return true }, nil, cursors, nil); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Value != 2 || got[0].Timestamp != 200 {
		t.Fatal("retired cursor skipped new journal", got)
	}
}
