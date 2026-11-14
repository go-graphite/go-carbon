package carbon

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func TestJournalOnlyCaptureBeforeStorage(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "journal", true: "legacy_dump"}[legacy], func(t *testing.T) {
			app := New("")
			cfg := NewConfig()
			app.Config = cfg
			cfg.Dump.Enabled = true
			cfg.Dump.StoreInputHandoff = true
			cfg.Dump.Path = t.TempDir()
			cfg.InputLog.Enabled = true
			cfg.InputLog.Path = t.TempDir()
			if err := os.WriteFile(filepath.Join(cfg.InputLog.Path, "0000000000000001.log"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			cfg.Whisper.StorageBackend = "pebble-chunk"
			cfg.Tcp.Enabled = true
			cfg.Tcp.Listen = "127.0.0.1:0"
			cfg.Udp.Enabled = false
			cfg.Pickle.Enabled = false
			if legacy {
				if err := os.WriteFile(filepath.Join(cfg.Dump.Path, "cache.1.2.bin"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := app.startStartupInput(); err != nil {
				t.Fatal(err)
			}
			if legacy {
				if app.startupInput != nil {
					t.Fatal("unvalidated legacy generation captured")
				}
				return
			}
			s := app.startupInput
			if s == nil || !s.journalOnly || app.Cache != nil || app.MetricStore != nil {
				t.Fatal("journal capture waited for storage")
			}
			defer s.close()
			defer s.receiver.Stop()
			p := points.OnePoint("journal.capture", 1, 123)
			if err := s.Write(p.AppendBinary(nil), 1); err != nil {
				t.Fatal(err)
			}
			if s.points.Load() != 1 {
				t.Fatal("capture did not durably append")
			}
			if _, err := os.Stat(filepath.Join(cfg.Dump.Path, recovery.ManifestName)); !os.IsNotExist(err) {
				t.Fatal("manufactured a recovery manifest")
			}
		})
	}
}
