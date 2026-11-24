package v2index

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"syscall"
	"testing"
)

// Last publication wins across records, including a removal followed by
// recreation, and a record that makes no membership change. Reopening must
// preserve both find results and the revision used by the next publication.
func TestNameLogRecordOrder(t *testing.T) {
	dir := t.TempDir()
	var c Catalog
	if err := c.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(1, []string{"app.a", "app.b", "app.c"}); err != nil {
		t.Fatal(err)
	}
	c.appendLog([]string{"app.d", "app.e"}, []string{"app.a", "app.b"})
	c.appendLog([]string{"app.a"}, []string{"app.d"})
	c.appendLog([]string{"app.b", "app.d"}, []string{"app.c", "app.e"})
	c.appendLog([]string{"app.a", "app.b"}, []string{"app.c"}) // no-op
	c.appendLog([]string{"app.x"}, []string{"app.x"})          // removal wins within one record
	c.Close()
	var d Catalog
	if err := d.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	want := []string{"app.a", "app.b", "app.d"}
	if d.Snapshot().Revision() != 1 || !d.Snapshot().EqualNames(want) {
		t.Fatalf("replayed %v at revision %d", slices.Collect(d.Snapshot().Names()), d.Snapshot().Revision())
	}
	var found []string
	if err := d.Snapshot().MatchPaths(context.Background(), "app/*.wsp", func(s string) error { found = append(found, s); return nil }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(found, []string{"/app/a.wsp", "/app/b.wsp", "/app/d.wsp"}) {
		t.Fatalf("find got %v, want %v", found, want)
	}
	if err := d.Apply(2, []string{"app.c"}, []string{"app.d"}); err != nil {
		t.Fatal(err)
	}
	if !d.Snapshot().EqualNames([]string{"app.a", "app.b", "app.c"}) {
		t.Fatal("publication after replay differs")
	}
}

// Even a CRC-valid record must keep the writer's sorted-run invariant.
// Recovery discards a noncanonical projection instead of merging it incorrectly.
func TestNameLogUnsortedRebuilds(t *testing.T) {
	dir := t.TempDir()
	var c Catalog
	if err := c.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(1, []string{"app.a"}); err != nil {
		t.Fatal(err)
	}
	c.appendLog([]string{"app.c", "app.b"}, nil)
	c.Close()
	var d Catalog
	if err := d.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Snapshot().Revision() != 0 {
		t.Fatal("unsorted projection served")
	}
}

// BenchmarkNameLogReplay recreates a 10M-name file base and a ~53MB log.
// IDX_REPLAY_FIXTURE optionally retains the fixture and names for daemon tests.
// Fixture generation and output verification are outside the measured interval.
func BenchmarkNameLogReplay(b *testing.B) {
	dir := os.Getenv("IDX_REPLAY_FIXTURE")
	if dir == "" {
		dir = b.TempDir()
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		b.Fatal(err)
	}
	const baseN, addedN = 10_000_000, 2_000_000
	name := func(i int) string {
		x := uint64(i+1) * 0x9e3779b97f4a7c15
		if i < baseN {
			return fmt.Sprintf("idx.name.%010d.%016x%02x", i, x, x&255)
		}
		return fmt.Sprintf("idx.name.%010d.%016x%06x", i, x, x&0xffffff)
	}
	if _, err := os.Stat(filepath.Join(dir, "base-000001.fc")); os.IsNotExist(err) {
		var c Catalog
		if err := c.SetDir(dir); err != nil {
			b.Fatal(err)
		}
		names := make([]string, baseN)
		for i := range names {
			names[i] = name(i)
		}
		if err := c.Publish(1, names); err != nil {
			b.Fatal(err)
		}
		names = make([]string, addedN)
		for i := range names {
			names[i] = name(baseN + i)
		}
		// Large sorted records occur when a base merge carries unmerged levels.
		batches := 1
		if v := os.Getenv("IDX_REPLAY_BATCHES"); v != "" {
			batches, _ = strconv.Atoi(v)
		}
		if batches < 1 || batches > addedN {
			b.Fatal("invalid batch count")
		}
		for i := 0; i < addedN; {
			end := min(addedN, i+(addedN+batches-1)/batches)
			c.appendLog(names[i:end], nil)
			i = end
		}
		c.Close()
		if os.Getenv("IDX_REPLAY_FIXTURE") != "" {
			f, err := os.Create(filepath.Join(dir, "names.txt"))
			if err != nil {
				b.Fatal(err)
			}
			w := bufio.NewWriterSize(f, 1<<20)
			for i := 0; i < baseN+addedN; i++ {
				fmt.Fprintln(w, name(i))
			}
			if err := w.Flush(); err != nil {
				b.Fatal(err)
			}
			if err := f.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
	for _, file := range []string{"base-000001.fc", "log-000001"} {
		st, err := os.Stat(filepath.Join(dir, file))
		if err != nil {
			b.Fatal(err)
		}
		b.Logf("%s: %d bytes", file, st.Size())
	}
	b.ReportAllocs()
	runtime.GC()
	var cpu float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var before, after syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &before)
		var c Catalog
		if err := c.SetDir(dir); err != nil {
			b.Fatal(err)
		}
		syscall.Getrusage(syscall.RUSAGE_SELF, &after)
		b.StopTimer()
		cpu += float64(after.Utime.Sec-before.Utime.Sec+after.Stime.Sec-before.Stime.Sec) + float64(after.Utime.Usec-before.Utime.Usec+after.Stime.Usec-before.Stime.Usec)/1e6
		if c.Snapshot().Len() != baseN+addedN {
			b.Fatalf("replayed %d names", c.Snapshot().Len())
		}
		c.Close()
		b.StartTimer()
	}
	b.ReportMetric(cpu/float64(b.N), "cpu-s/op")
}
