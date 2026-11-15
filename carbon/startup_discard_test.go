package carbon

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func TestStartupJournalLegacyMovedWithWindow(t *testing.T) {
	root := t.TempDir()
	cfg := NewConfig()
	cfg.Dump.Path = root
	name := "input.old.tail.bin.capture-epoch-01700000000000000000.000000.000000000000.tail.bin"
	path := filepath.Join(root, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := inputlog.NewSegmentWriter(f, 1)
	if err != nil {
		t.Fatal(err)
	}
	p := points.OnePoint("legacy", 1, 1699999990)
	p.Add(2, 1700000020)
	if _, err = writer.Append([]*points.Points{p}, nil); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	f.Close()
	app := New("")
	app.Config = cfg
	moved, err := app.discardLegacyStartup()
	if err != nil || !moved {
		t.Fatal(moved, err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("source not moved")
	}
	dirs, _ := filepath.Glob(filepath.Join(root, "startup-discard.*"))
	if len(dirs) != 1 {
		t.Fatal(dirs)
	}
	if _, err = os.Stat(filepath.Join(dirs[0], name)); err != nil {
		t.Fatal("spool not retained", err)
	}
	raw, err := os.ReadFile(filepath.Join(dirs[0], "window.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record discardedStartup
	if err = json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	if record.MinTimestamp != 1699999990 || record.MaxTimestamp != 1700000020 || record.Points != 2 || !record.CaptureStart.Equal(time.Unix(1700000000, 0)) {
		t.Fatalf("incorrect window %+v", record)
	}
	moved, err = app.discardLegacyStartup()
	if err != nil || moved {
		t.Fatal("move not idempotent", moved, err)
	}
}
func TestStartupJournalCompletesInterruptedMove(t *testing.T) {
	root := t.TempDir()
	aside := filepath.Join(root, "startup-discard.123")
	if err := os.Mkdir(aside, 0700); err != nil {
		t.Fatal(err)
	}
	record := discardedStartup{Version: 1, Files: []string{"input.a.tail.bin.capture-1.tail.bin", "input.a.tail.bin.capture-2.tail.bin"}}
	raw, _ := json.Marshal(record)
	if err := os.WriteFile(filepath.Join(aside, "window.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(aside, record.Files[0]), []byte("first"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, record.Files[1]), []byte("second"), 0600); err != nil {
		t.Fatal(err)
	}
	app := New("")
	app.Config = NewConfig()
	app.Config.Dump.Path = root
	moved, err := app.discardLegacyStartup()
	if err != nil || !moved {
		t.Fatal(moved, err)
	}
	for _, name := range record.Files {
		if _, err = os.Stat(filepath.Join(aside, name)); err != nil {
			t.Fatal(err)
		}
	}
}
