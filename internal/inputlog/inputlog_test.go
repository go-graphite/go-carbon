package inputlog

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

// fakeSource stands in for the cache. With follow, every recorded point counts
// as persisted as soon as the log takes it.
type fakeSource struct {
	mu     sync.Mutex
	shards [][]Entry
	gens   []uint64
	low    uint64
	follow bool
	// refs, if set, keeps one StreamRef per metric, as the cache keeps one
	// per item, so entries carry stream ids.
	refs map[string]*StreamRef
}

const testShards = 16

func newFakeSource() *fakeSource {
	return &fakeSource{shards: make([][]Entry, testShards), gens: make([]uint64, testShards)}
}

func (f *fakeSource) TakeLogEntries(shard int, gen uint64, spare []Entry) []Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	taken := f.shards[shard]
	f.shards[shard], f.gens[shard] = spare[:0], gen
	return taken
}

func (f *fakeSource) LogLowWater() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.follow {
		return slices.Min(f.gens)
	}
	return f.low
}

func (f *fakeSource) RelogBefore(uint64, int) int { return 0 }
func (f *fakeSource) LogDropped() uint64          { return 0 }

// add records a point in the shard its metric belongs to, so one metric's
// points stay in one ordered stream like in the cache.
func (f *fakeSource) add(metric string, value float64, timestamp int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	shard := len(metric) % len(f.shards)
	ref := f.refs[metric]
	if ref == nil {
		ref = NewStreamRef(metric)
		if f.refs != nil {
			f.refs[metric] = ref
		}
	}
	f.shards[shard] = append(f.shards[shard], ref.Entry(value, timestamp))
}

func (f *fakeSource) setLow(low uint64, follow bool) {
	f.mu.Lock()
	f.low, f.follow = low, follow
	f.mu.Unlock()
}

type recorded map[string][]points.Point

func (r recorded) add(metric string, value float64, timestamp int64) {
	r[metric] = append(r[metric], points.Point{Value: value, Timestamp: timestamp})
}

// reopen simulates a restart: open, replay, start.
func reopen(t *testing.T, dir string, opts Options, src Source) (*Log, recorded, ReplayStats) {
	t.Helper()
	l, err := Open(dir, withTestDefaults(opts))
	if err != nil {
		t.Fatal(err)
	}
	got, stats := replayAll(t, l)
	l.Start(src, testShards)
	return l, got, stats
}

func replayAll(t *testing.T, l *Log) (recorded, ReplayStats) {
	t.Helper()
	var mu sync.Mutex
	got := recorded{}
	stats, err := l.Replay(func(p *points.Points) {
		mu.Lock()
		got[p.Metric] = append(got[p.Metric], p.Data...)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	return got, stats
}

func equalRecorded(t *testing.T, got, want recorded) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("replayed %d metrics, want %d", len(got), len(want))
	}
	for metric, w := range want {
		if !slices.Equal(got[metric], w) {
			t.Fatalf("metric %s replayed %v, want %v", metric, got[metric], w)
		}
	}
}

func files(t *testing.T, dir, suffix string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), suffix) {
			n++
		}
	}
	return n
}

func withTestDefaults(opts Options) Options {
	if opts.SyncInterval == 0 {
		opts.SyncInterval = time.Hour // cycles run on Sync only
	}
	return opts
}

func openStarted(t *testing.T, dir string, opts Options, src Source) *Log {
	t.Helper()
	l, got, _ := reopen(t, dir, opts, src)
	if len(got) != 0 {
		t.Fatalf("fresh log replayed %d metrics", len(got))
	}
	return l
}

func TestReplayAfterKill(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	l := openStarted(t, dir, Options{SegmentSize: 1 << 20}, src)
	want := recorded{}
	for cycle := range 5 {
		for i := range 500 {
			metric := fmt.Sprintf("metric.%d.%s", i%41, strings.Repeat("x", i%7))
			value, ts := float64(cycle*1000+i), int64(1700000000+cycle)
			src.add(metric, value, ts)
			want.add(metric, value, ts)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	src.add("lost.with.the.crash", 1, 1) // never synced
	l.Kill()

	l, got, stats := reopen(t, dir, Options{SegmentSize: 1 << 20}, newFakeSource())
	defer l.Close()
	equalRecorded(t, got, want)
	if stats.Points != 2500 || stats.Corrupt != 0 {
		t.Fatalf("stats %+v", stats)
	}
}

func TestReplaySkipsTornFrames(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	l := openStarted(t, dir, Options{SegmentSize: 1 << 20}, src)
	want := recorded{}
	for cycle := range 3 {
		metric := fmt.Sprintf("metric.%d", cycle)
		src.add(metric, float64(cycle), int64(cycle))
		if cycle < 2 {
			want.add(metric, float64(cycle), int64(cycle))
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	l.Kill()

	// The last cycle's frame reached the disk only partly: its payload no
	// longer verifies. A torn header after it ends the segment.
	l, err := Open(dir, withTestDefaults(Options{}))
	if err != nil {
		t.Fatal(err)
	}
	last := l.pending[len(l.pending)-1]
	path, off, end := last.seg.path, last.off, last.seg.end
	l.Kill()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte{0xff}, off+frameHeaderSize); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(frameHeader{codec: codecStream, salt: last.h.salt, length: 1 << 30}.append(nil)[:30], end); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	l, got, stats := reopen(t, dir, Options{}, newFakeSource())
	defer l.Close()
	equalRecorded(t, got, want)
	if stats.Corrupt != 1 {
		t.Fatalf("corrupt frames %d, want 1", stats.Corrupt)
	}
}

// Once points are persisted, their segments are recycled, and a recycled
// segment's stale frames are never replayed.
func TestTruncationRecyclesSegments(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	src.setLow(0, true)
	opts := Options{SegmentSize: 16 << 10}
	l := openStarted(t, dir, opts, src)
	value := 0.0
	for range 40 {
		for i := range 2000 {
			value++
			src.add(fmt.Sprintf("big.metric.%d", i), value, int64(value))
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if live := files(t, dir, logSuffix); live > 2 {
		t.Fatalf("%d live segments after every point was persisted", live)
	}
	if spares := files(t, dir, spareSuffix); spares == 0 || spares > maxSpares {
		t.Fatalf("%d spare segments", spares)
	}
	if l.stat.recycled.Load() == 0 {
		t.Fatal("no segment was recycled")
	}

	// Pin everything from now on, and write small frames into recycled
	// segments that still hold large stale ones.
	src.setLow(l.gen, false)
	want := recorded{}
	for cycle := range 3 {
		for i := range 5 {
			metric := fmt.Sprintf("small.%d", i)
			src.add(metric, float64(cycle), int64(cycle))
			want.add(metric, float64(cycle), int64(cycle))
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	l.Kill()

	l, got, _ := reopen(t, dir, opts, newFakeSource())
	defer l.Close()
	equalRecorded(t, got, want)
}

func TestRetireRemovesSegments(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	l := openStarted(t, dir, Options{SegmentSize: 16 << 10}, src)
	for i := range 3000 {
		src.add(fmt.Sprintf("metric.%d", i), 1, 1)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Retire(); err != nil {
		t.Fatal(err)
	}
	if n := files(t, dir, logSuffix) + files(t, dir, spareSuffix); n != 0 {
		t.Fatalf("%d segment files after retire", n)
	}
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.NeedsReplay() {
		t.Fatal("retired log needs replay")
	}
}

// Replayed points stay protected by their old segments until they are
// persisted again, including across a second crash.
func TestReplayedSegmentsPinnedUntilPersisted(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	opts := Options{SegmentSize: 16 << 10}
	l := openStarted(t, dir, opts, src)
	want := recorded{}
	for i := range 3000 {
		metric := fmt.Sprintf("metric.%d", i)
		src.add(metric, float64(i), 1)
		want.add(metric, float64(i), 1)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	l.Kill()

	// Restart: the replayed items are unpersisted at generation zero while
	// new input arrives and cycles run.
	src = newFakeSource()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = l.Replay(func(*points.Points) {}); err != nil {
		t.Fatal(err)
	}
	l.Start(src, testShards)
	for i := range 5 {
		src.add("new.metric", float64(i), int64(i))
		want.add("new.metric", float64(i), int64(i))
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	l.Kill()

	l, got, _ := reopen(t, dir, opts, newFakeSource())
	equalRecorded(t, got, want)
	l.Kill()

	// Once generation zero is persisted, the old segments go.
	src = newFakeSource()
	src.setLow(0, true)
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = l.Replay(func(*points.Points) {}); err != nil {
		t.Fatal(err)
	}
	l.Start(src, testShards)
	defer l.Close()
	for range 3 {
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if live := files(t, dir, logSuffix); live > 1 {
		t.Fatalf("%d live segments after the replayed points were persisted", live)
	}
}

func TestCloseKeepsSegmentsAndUnlocks(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	l := openStarted(t, dir, Options{}, src)
	src.add("metric", 1, 2)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, got, _ := reopen(t, dir, Options{}, newFakeSource())
	defer l.Close()
	equalRecorded(t, got, recorded{"metric": {{Value: 1, Timestamp: 2}}})
	if _, err := os.Stat(filepath.Join(dir, "LOCK")); err != nil {
		t.Fatal(err)
	}
}

// A replay that starts in the middle of a segment first reads the frames
// before it there: a name defined by an earlier, persisted cycle still
// resolves, and a name first seen in a later cycle is defined there.
func TestReplayStartsMidSegment(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	l := openStarted(t, dir, Options{SegmentSize: 1 << 20}, src)
	// Equal lengths: one shard, so one partition stream.
	src.add("metric.a", 1, 100)
	src.add("other.metric.x", 1, 100)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	src.setLow(1, false) // the first cycle is persisted
	want := recorded{}
	for _, e := range []Entry{NewStreamRef("metric.a").Entry(2, 160), NewStreamRef("metric.b").Entry(3, 160), NewStreamRef("other.metric.y").Entry(4, 160)} {
		src.add(e.Metric(), e.Value, e.Timestamp)
		want.add(e.Metric(), e.Value, e.Timestamp)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	src.add("metric.c", 5, 220)
	want.add("metric.c", 5, 220)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	l.Kill()

	l, got, stats := reopen(t, dir, Options{}, newFakeSource())
	defer l.Close()
	equalRecorded(t, got, want)
	if stats.DictionaryFrames != 2 || stats.Corrupt != 0 {
		t.Fatalf("stats %+v, want the first cycle's two frames read for names", stats)
	}
}

// A torn frame ends its own stream for the rest of the segment, also when
// it precedes the replay start; other partitions' streams are unaffected.
func TestReplayStopsDamagedStreamOnly(t *testing.T) {
	for _, damaged := range []int{0, 1} {
		t.Run(fmt.Sprintf("cycle %d", damaged), func(t *testing.T) {
			dir := t.TempDir()
			src := newFakeSource()
			l := openStarted(t, dir, Options{SegmentSize: 1 << 20}, src)
			want := recorded{}
			for cycle := range 3 {
				if cycle == 1 {
					src.setLow(1, false) // replay starts at the second cycle
				}
				for _, metric := range []string{"damaged.stream", "intact.stream.metric"} {
					src.add(metric, float64(cycle), int64(cycle))
					if cycle > 0 && metric != "damaged.stream" {
						want.add(metric, float64(cycle), int64(cycle))
					}
				}
				if err := l.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			l.Kill()

			l, err := Open(dir, withTestDefaults(Options{}))
			if err != nil {
				t.Fatal(err)
			}
			frames := slices.Concat(l.prefix, l.pending)
			partition := uint16(len("damaged.stream") % testShards)
			var target frameRef
			for i, n := 0, 0; i < len(frames); i++ {
				if frames[i].h.partition == partition {
					if n == damaged {
						target = frames[i]
					}
					n++
				}
			}
			l.Kill()
			f, err := os.OpenFile(target.seg.path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.WriteAt([]byte{0xff, 0xff}, target.off+frameHeaderSize+2); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()

			l, got, stats := reopen(t, dir, Options{}, newFakeSource())
			defer l.Close()
			equalRecorded(t, got, want)
			if stats.Corrupt != int64(3-damaged) {
				t.Fatalf("corrupt frames %d, want the damaged one and the rest of its stream", stats.Corrupt)
			}
		})
	}
}

// writeTestSegment writes a segment file of the given frames, encoding each
// payload for its codec.
func writeTestSegment(t *testing.T, path string, seq, salt uint64, frames ...frameHeader) {
	t.Helper()
	buf := segmentHeader{seq: seq, salt: salt}.append(nil)
	for _, h := range frames {
		buf = h.append(buf)
		buf = append(buf, testPayloads[h.crc]...)
	}
	if err := os.WriteFile(path, append(buf, make([]byte, 4096)...), 0644); err != nil {
		t.Fatal(err)
	}
}

var testPayloads = map[uint32][]byte{}

// testFrame compresses raw as a frame payload of salt and codec.
func testFrame(t *testing.T, codec uint8, salt uint64, raw []byte, rows int) frameHeader {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	payload := enc.EncodeAll(raw, nil)
	crc := crc32.Checksum(payload, castagnoli)
	testPayloads[crc] = payload
	return frameHeader{codec: codec, salt: salt, length: uint32(len(payload)), rawLength: uint32(len(raw)), points: uint32(rows), crc: crc,
		replayFrom: position{1, segmentHeaderSize}}
}

// A recycled segment keeps frames of its earlier use after the new ones.
// Such a stale frame can continue the new stream's frame index and name
// count, yet refer to the earlier use's dictionary; its salt ends the scan
// before it.
func TestReplayIgnoresStaleDictionary(t *testing.T) {
	staleSalt, salt := uint64(1111), uint64(2222)
	names := func(prefix string) []Entry {
		var batch []Entry
		for i := range 5 {
			batch = append(batch, NewStreamRef(fmt.Sprintf("%s.%d", prefix, i)).Entry(float64(i), 100))
		}
		return batch
	}
	// The earlier use defined five names, then sent them again in frame 1.
	var stale streamEncoder
	staleFrames := encodeStream(&stale, 1, names("earlier.use.metric"), names("earlier.use.metric"))
	// The new use defines five other names in frame 0, then crashed.
	var current streamEncoder
	frames := encodeStream(&current, 1, names("new"))

	for _, sameSalt := range []bool{false, true} {
		dir := t.TempDir()
		staleHeader := testFrame(t, codecStream, staleSalt, staleFrames[1], 5)
		if sameSalt {
			staleHeader.salt = salt // what the salt protects against
		}
		writeTestSegment(t, filepath.Join(dir, fmt.Sprintf("%016x%s", 1, logSuffix)), 1, salt,
			testFrame(t, codecStream, salt, frames[0], 5), staleHeader)
		l, got, _ := reopen(t, dir, Options{}, newFakeSource())
		l.Kill()
		want := recorded{}
		for _, e := range names("new") {
			want.add(e.Metric(), e.Value, e.Timestamp)
			if sameSalt {
				want.add(e.Metric(), e.Value, e.Timestamp)
			}
		}
		equalRecorded(t, got, want)
	}
}

// Frames of legacy binary records, as the first version wrote, replay too.
func TestReplayLegacyRecordFrames(t *testing.T) {
	dir := t.TempDir()
	entries := []Entry{NewStreamRef("legacy.a").Entry(1, 2), NewStreamRef("legacy.a").Entry(3, 4), NewStreamRef("legacy.b").Entry(5, 6)}
	raw, _, _ := appendRecords(nil, entries, nil, maxFrameRaw)
	writeTestSegment(t, filepath.Join(dir, fmt.Sprintf("%016x%s", 1, logSuffix)), 1, 7, testFrame(t, codecRecords, 7, raw, 3))
	l, got, stats := reopen(t, dir, Options{}, newFakeSource())
	defer l.Close()
	equalRecorded(t, got, recorded{"legacy.a": {{Value: 1, Timestamp: 2}, {Value: 3, Timestamp: 4}}, "legacy.b": {{Value: 5, Timestamp: 6}}})
	if stats.Points != 3 {
		t.Fatalf("stats %+v", stats)
	}
}
