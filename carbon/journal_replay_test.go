//go:build unix

package carbon

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// The ordinary cache insert is the oracle for fused journal application. Check
// the receipt's bits/order and close/reopen the two stores before comparing.
func TestJournalReplayStoredBitDifferential(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "0000000000000001.log"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := inputlog.NewSegmentWriter(file, 1)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := cache.New()
	ordinary.SetMaxSize(0)
	ordinary.SetTagsEnabled(true)
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001, 0x3ff0000000000000}
	var want []points.Point
	for epoch := range 3 {
		var records []*points.Points
		for i, value := range bits {
			name := "journal.alias;b=2;a=1"
			if i%2 == 1 {
				name = "journal.alias;a=1;b=2"
			}
			// Adjacent pairs are same-timestamp corrections. The following
			// epoch corrects all of those timestamps again.
			p := points.OnePoint(name, math.Float64frombits(value), 1700000000+int64(i/2))
			if epoch%2 == 1 {
				p.Data[0].Value = math.Float64frombits(bits[len(bits)-1-i])
			}
			want = append(want, p.Data...)
			records = append(records, p)
			copy := *p
			ordinary.AddRecovered(&copy)
		}
		if _, err := writer.Append(records, nil); err != nil {
			t.Fatal(err)
		}
	}
	writer.Close()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	log, err := inputlog.Open(dir, inputlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	got := cache.New()
	got.SetMaxSize(0)
	got.SetTagsEnabled(true)
	got.EnableInputLog()
	app := New("")
	app.Config, app.Cache, app.startupInput = NewConfig(), got, &startupInput{}
	app.Config.InputLog.Path = dir
	app.inputLog.Store(log)
	if err := app.replayInputLogStopped(got, nil); err != nil {
		t.Fatal(err)
	}
	assertDirectStoreDifferential(t, got, ordinary, "journal.alias;a=1;b=2", want)
}
