package v2read

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestL0RestartRendersMatch kills a store between seals by copying its files
// while it runs: only L0 run files hold the published head. The copy must
// render exactly what the running store rendered, before and after its first
// seal, and that seal must unlink the loaded files.
func TestL0RestartRendersMatch(t *testing.T) {
	cfg := testConfig()
	cfg.Shards, cfg.Owners = 2, 2
	cfg.L0 = true
	cfg.PublishInterval = 5 * time.Millisecond
	cfg.SealInterval = time.Hour
	const now = 1000
	open := func(dir string) (*Reader, *v2core.Engine) {
		t.Helper()
		reader, err := New(cfg.Owners, 1<<28, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		reader.Now = func() time.Time { return time.Unix(now, 0) }
		engine, err := v2core.Open(dir, cfg, v2core.Hooks{PublishPending: reader.PublishPending, Now: func() time.Time { return time.Unix(now, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		return reader, engine
	}
	var names []string
	policies := map[uint64][]byte{}
	for id := uint64(1); id <= 12; id++ {
		name := fmt.Sprintf("m.%02d", id)
		names = append(names, name)
		method := []sealrollup.Method{sealrollup.Last, sealrollup.Sum, sealrollup.Average}[id%3]
		policies[id], _ = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: 0, Archives: []sealrollup.Archive{{Step: 1, Count: 60}, {Step: 10, Count: 60}}}})
	}
	render := func(reader *Reader) string {
		t.Helper()
		out := ""
		err := reader.FetchMany(context.Background(), names, now-500, now, now, func(name string, s *Series) error {
			out += fmt.Sprintf("%s %d %d %d %v %v\n", name, s.From, s.Until, s.Step, s.Values, s.Occupied)
			return nil
		})
		if err != nil {
			return err.Error()
		}
		return out
	}
	l0Files := func(dir string) int {
		files, _ := filepath.Glob(filepath.Join(dir, "*", "l0", "l0-*"))
		return len(files)
	}
	dir := t.TempDir()
	reader, engine := open(dir)
	// Several publications, multi-point applications, repeated slots with
	// later orders and clocks, and times across a coarse bucket edge.
	order, cursor := uint64(0), uint64(0)
	for round := range 6 {
		var batches []v2core.Batch
		for id := uint64(1); id <= 12; id++ {
			order++
			n := 1 + int(id+uint64(round))%3
			var points []v2core.Row
			for j := range n {
				ts := int64(now - 120 + round*7 + j*3 + int(id%4))
				points = append(points, v2core.Row{ID: id, Time: ts, Bits: math.Float64bits(float64(round*100+j) + float64(id)/8), Seq: order})
			}
			batches = append(batches, v2core.Batch{ID: id, Order: order, Clock: now - 10 + int64(round), Policy: policies[id], Points: points})
			cursor += uint64(n)
		}
		if err := engine.AppendBatches(0, batches, cursor, false); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return l0Files(dir) >= 1 })
		time.Sleep(20 * time.Millisecond)
	}
	await(t, func() bool { return !strings.Contains(render(reader), "not found") })
	time.Sleep(50 * time.Millisecond)
	want := render(reader)
	files := l0Files(dir)
	if files < 2 {
		t.Fatalf("L0 files %d, want one per owner publication", files)
	}
	// Kill -9 between seals: the copy holds core files and L0 files only.
	killed := t.TempDir()
	if err := os.CopyFS(killed, os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
	reader.Close()
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if n := l0Files(dir); n != 0 {
		t.Fatalf("seal left %d L0 files", n)
	}
	reader, engine = open(dir)
	if got := render(reader); got != want {
		t.Fatalf("sealed store differs from published head:\n%s\nwant\n%s", got, want)
	}
	reader.Close()
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// A failed lazy catalog open must preserve this owner's durable head for
	// the next restart, even if the failed engine then closes cleanly.
	t.Run("catalog error preserves L0", func(t *testing.T) {
		failed := t.TempDir()
		if err := os.CopyFS(failed, os.DirFS(killed)); err != nil {
			t.Fatal(err)
		}
		failedReader, failedEngine := open(failed)
		wantErr := errors.New("catalog unavailable")
		if err := failedEngine.LoadL0(func(int, *v2core.Batch) (bool, error) { return false, wantErr }); !errors.Is(err, wantErr) {
			t.Fatalf("replay ignored catalog error: %v", err)
		}
		failedReader.Close()
		if err := failedEngine.Close(); err != nil {
			t.Fatal(err)
		}
		if n := l0Files(failed); n != files {
			t.Fatalf("catalog error consumed L0 files: got %d want %d", n, files)
		}
	})
	reader, engine = open(killed)
	// The daemon's durable catalogs register every handle before LoadL0.
	for id, policy := range policies {
		if _, err := engine.RegisterApplicationPolicy(id, names[id-1], policy); err != nil {
			t.Fatal(err)
		}
	}
	var loaded []uint64
	if err := engine.LoadL0(func(_ int, b *v2core.Batch) (bool, error) { loaded = append(loaded, b.Order); return true, nil }); err != nil {
		t.Fatal(err)
	}
	slices.Sort(loaded)
	if len(loaded) != int(order) || loaded[0] != 1 || loaded[len(loaded)-1] != order {
		t.Fatalf("loaded %d applications, orders %v..; want 1..%d", len(loaded), loaded[:1], order)
	}
	for deadline := time.Now().Add(3 * time.Second); render(reader) != want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("loaded store differs:\n%s\nwant\n%s", render(reader), want)
		}
	}
	reader.Close()
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if n := l0Files(killed); n != 0 {
		t.Fatalf("seal after load left %d L0 files", n)
	}
	reader, engine = open(killed)
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	if got := render(reader); got != want {
		t.Fatalf("restarted store differs:\n%s\nwant\n%s", got, want)
	}
}
