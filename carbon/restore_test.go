package carbon

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/helper/qa"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func TestRestore(t *testing.T) {
	qa.Root(t, func(root string) {
		w := func(fn, body string) {
			err := os.WriteFile(path.Join(root, fn), []byte(body), 0600)
			if err != nil {
				t.Fatal(err)
			}
		}

		w("input.42.1470686967790091088", "m1 1 1470687039\n")
		w("cache.42.1470686967790091088", "m2 2 1470687039\n")

		w("input.15.1470687188677488571", "bad_message\nm3 3 1470687217\n")
		w("cache.15.1470687188677488571", "m4 4 1470687217\n")

		w("input.33.1470687188677488570", "m5 5 1470687217\n")
		w("cache.33.1470687188677488570", "")

		expected := []*points.Points{
			{
				Metric: "m2",
				Data: []points.Point{
					{
						Value:     2.000000,
						Timestamp: 1470687039,
					},
				},
			},
			{
				Metric: "m1",
				Data: []points.Point{
					{
						Value:     1.000000,
						Timestamp: 1470687039,
					},
				},
			},
			{
				Metric: "m5",
				Data: []points.Point{
					{
						Value:     5.000000,
						Timestamp: 1470687217,
					},
				},
			},
			{
				Metric: "m4",
				Data: []points.Point{
					{
						Value:     4.000000,
						Timestamp: 1470687217,
					},
				},
			},
			{
				Metric: "m3",
				Data: []points.Point{
					{
						Value:     3.000000,
						Timestamp: 1470687217,
					},
				},
			},
		}

		ch := make(chan *points.Points, 1024)

		app := &App{}
		app.RestoreFromDir(root, func(p *points.Points) {
			ch <- p
		})

		close(ch)

		if len(ch) != len(expected) {
			t.FailNow()
		}

		index := 0
		for p := range ch {
			if !expected[index].Eq(p) {
				t.FailNow()
			}
			index++
		}
	})
}

// Ordered restore reads legacy, compact and blocks generations, oldest
// first, each dump before its input file, and removes them. It needs no
// checkpoint index: this is the restore after a crash.
func TestRestoreCompactAndLegacyGenerations(t *testing.T) {
	root := t.TempDir()
	w := func(fn string, body []byte) {
		if err := os.WriteFile(path.Join(root, fn), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	compact := func(p *points.Points) []byte {
		return recovery.AppendCompactRecord(nil, p.Metric, recovery.AppendCompactPoints(nil, p.Data), len(p.Data))
	}
	w("cache.10.1000.bin", points.OnePoint("m", 1, 1).AppendBinary(nil))
	w("input.10.1000.bin", points.OnePoint("m", 2, 2).AppendBinary(nil))
	w("cache-compact.11.2000.bin", append(compact(points.OnePoint("m", 3, 3).Add(4, 4)), compact(points.OnePoint("n", 5, 5))...))
	w("input.11.2000.bin", points.OnePoint("m", 6, 6).AppendBinary(nil))
	// A compact checkpoint's input file, then its handoff tail.
	input, err := recovery.NewWriterFormat(path.Join(root, recovery.InputName(recovery.FormatCompact, "12.3000.bin")), 1, 4096, recovery.NewBuilder(nil), recovery.FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	if err = input.WritePoints(points.OnePoint("m", 7, 7).Add(8, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err = input.Close(); err != nil {
		t.Fatal(err)
	}
	w(recovery.TailName(recovery.InputName(recovery.FormatCompact, "12.3000.bin")), points.OnePoint("m", 9, 9).AppendBinary(nil))
	// A blocks dump, as a stop writes it now, then its input file.
	dump, err := recovery.NewWriterFormat(path.Join(root, recovery.CacheName(recovery.FormatBlocks, "13.4000.bin")), 0, 1<<20, nil, recovery.FormatBlocks)
	if err != nil {
		t.Fatal(err)
	}
	err = dump.WriteSegments(3, 2, func(seg int, out *recovery.Segment) error {
		switch seg {
		case 0:
			return out.WritePoints(points.OnePoint("n", 1, 1))
		case 2:
			if err := out.WritePoints(points.OnePoint("m", 10, 10)); err != nil { // in flight
				return err
			}
			return out.WritePoints(points.OnePoint("m", 11, 11))
		}
		return nil
	})
	if err == nil {
		_, err = dump.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	input, err = recovery.NewWriterFormat(path.Join(root, recovery.InputName(recovery.FormatCompact, "13.4000.bin")), 1, 4096, recovery.NewBuilder(nil), recovery.FormatCompact)
	if err == nil {
		err = input.WritePoints(points.OnePoint("m", 12, 12))
	}
	if err == nil {
		_, err = input.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	var got []points.Point
	app := &App{}
	app.RestoreFromDir(root, func(p *points.Points) {
		if p.Metric == "m" {
			got = append(got, p.Data...)
		}
	})
	want := []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}, {Value: 6, Timestamp: 6},
		{Value: 7, Timestamp: 7}, {Value: 8, Timestamp: 8}, {Value: 9, Timestamp: 9}, {Value: 10, Timestamp: 10}, {Value: 11, Timestamp: 11},
		{Value: 12, Timestamp: 12}}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("restore order", got)
	}
	if left, _ := os.ReadDir(root); len(left) != 0 {
		t.Fatal("restored files kept", left)
	}
}

func TestDumpFormatConfig(t *testing.T) {
	for format, want := range map[string]recovery.Format{
		"": recovery.FormatLegacy, "legacy": recovery.FormatLegacy,
		"compact": recovery.FormatCompact, "blocks": recovery.FormatBlocks,
	} {
		if got, err := (dumpConfig{Format: format}).cacheFormat(); err != nil || got != want {
			t.Fatal(format, got, want, err)
		}
	}
	if _, err := (dumpConfig{Format: "stream"}).cacheFormat(); err == nil {
		t.Fatal("accepted invalid dump format")
	}
}
