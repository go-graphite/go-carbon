package carbon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

func incrementalOracleLive(t *testing.T, app *App, stamp int64) {
	t.Helper()
	conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(conn, "saved.metric00063 999.125 %d\nlive.marker -0.125 %d\n", stamp+1, stamp+2)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	waitExpiration(t, func() bool {
		return app.Cache.Has("live.marker") || func() bool {
			_, err := app.MetricStore.Metadata(context.Background(), "live.marker")
			return err == nil
		}()
	})
}

// Exit without teardown while saved history is still queued. The log must
// preserve both that history and the newer correction after dump retirement.
func TestIncrementalIntakeCrashChild(t *testing.T) {
	path := os.Getenv("INTAKE_CRASH_CONFIG")
	if path == "" {
		return
	}
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	if app.Cache.RestoreOutstanding() == 0 {
		t.Fatal("crash must interrupt restore writeout")
	}
	stamp, err := strconv.ParseInt(os.Getenv("RESTORE_REPRO_STAMP"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	incrementalOracleLive(t, app, stamp)
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	os.Exit(99)
}

// Compare every archive timestamp and float bit with the ordinary drain-first
// path, and again after a process crash during early intake. All paths use the
// same dump/input ordering, real TCP correction and original-file conversion.
func TestIncrementalIntakeDifferentialAndCrash(t *testing.T) {
	if isolatedTestProcessSerial(t) {
		return
	}
	defer zapwriter.Test()()
	t.Setenv("RESTORE_REPRO_STAMP", strconv.FormatInt(time.Now().Unix()-1500, 10))
	var reference map[string]store.Snapshot
	for _, mode := range []string{"drain-first", "early", "crash"} {
		t.Run(mode, func(t *testing.T) {
			path, cfg, stamp := legacyIntakeFixture(t, 64, 10)
			cfg.Whisper.StoreIncrementalMigration = true
			cfg.Whisper.StoreDir = t.TempDir()
			cfg.Whisper.FLock = true
			cfg.Whisper.OutOfOrder = true
			if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:1h,5s:6h,60s:1d\n"), 0600); err != nil {
				t.Fatal(err)
			}
			schemas, err := persister.ReadWhisperSchemas(cfg.Whisper.SchemasFilename)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Whisper.Schemas = schemas
			cfg.Whisper.StoreMigrationMinFreeBytes = 0
			cfg.Whisper.StoreMigrationControlFile = filepath.Join(t.TempDir(), "control.json")
			cfg.Carbonserver.FileListCache = filepath.Join(t.TempDir(), "files.flc")
			cfg.InputLog.Enabled, cfg.InputLog.Path = true, t.TempDir()
			cfg.InputLog.SegmentSize = 16 << 20
			incrementalLease(t, cfg, 1, 0)
			original := filepath.Join(cfg.Whisper.DataDir, "saved", "metric00063.wsp")
			if err := makeIntakeOriginal(original, stamp); err != nil {
				t.Fatal(err)
			}
			if mode == "drain-first" {
				cfg.Carbonserver.ConcurrentIndex, cfg.Carbonserver.RealtimeIndex = false, 0
			} else {
				cfg.Whisper.MaxUpdatesPerSecond = 20
			}
			writeSharedConfig(t, path, cfg)
			if mode == "crash" {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIncrementalIntakeCrashChild$")
				cmd.Env = append(os.Environ(), "INTAKE_CRASH_CONFIG="+path)
				out, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 99 {
					t.Fatalf("crash child: %v\n%s", err, out)
				}
			}
			app := New(path)
			app.Config = cfg
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			defer app.Stop()
			if mode == "early" && app.Cache.RestoreOutstanding() == 0 {
				t.Fatal("early path waited for drain")
			}
			if mode != "crash" {
				incrementalOracleLive(t, app, stamp)
			}
			waitExpiration(t, func() bool { return app.Cache.IsEmpty() && app.Cache.RestoreOutstanding() == 0 })
			incrementalLease(t, cfg, 2, 1)
			waitExpiration(t, func() bool {
				_, err := app.MetricStore.Metadata(context.Background(), "saved.metric00063")
				return err == nil
			})
			snapshots := make(map[string]store.Snapshot)
			for i := 0; i <= 64; i++ {
				name := fmt.Sprintf("saved.metric%05d", i)
				if i == 64 {
					name = "live.marker"
				}
				snap, err := app.MetricStore.Snapshot(context.Background(), name)
				if err != nil {
					t.Fatal(name, err)
				}
				snapshots[name] = snap
				if reference != nil {
					if err := store.EqualSnapshots(reference[name], snap); err != nil {
						t.Fatalf("%s %s differs: %v", mode, name, err)
					}
				}
			}
			if reference == nil {
				reference = snapshots
			}
		})
	}
}

func TestIncrementalIntakeRejectsBrokenDump(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg, stamp := legacyIntakeFixture(t, 4, 10)
	cfg.Whisper.StoreIncrementalMigration = true
	cfg.Whisper.StoreDir = t.TempDir()
	cfg.Whisper.FLock = true
	cfg.Carbonserver.FileListCache = filepath.Join(t.TempDir(), "files.flc")
	// A compact source truncated after a complete record must be retained and prevent
	// live listeners from opening, even after earlier generations were loaded.
	broken := filepath.Join(cfg.Dump.Path, "cache-compact.99.9999999999999999999")
	data := []points.Point{{Timestamp: stamp, Value: 123.125}}
	valid := recovery.AppendCompactRecord(nil, "partial.loaded", recovery.AppendCompactPoints(nil, data), len(data))
	truncated := recovery.AppendCompactRecord(nil, "partial.missing", recovery.AppendCompactPoints(nil, data), len(data))
	raw := append(valid, truncated[:len(truncated)-1]...)
	if err := os.WriteFile(broken, raw, 0600); err != nil {
		t.Fatal(err)
	}
	app := New(path)
	app.Config = cfg
	defer app.Stop()
	if err := app.Start(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("expected partial dump failure before live input", err)
	}
	if _, err := os.Stat(broken); err != nil {
		t.Fatal("failed source was removed", err)
	}
	if len(app.Receivers) != 0 {
		t.Fatal("live intake opened after incomplete load")
	}
}

func TestLoadCompleteIntakeRequiresJournal(t *testing.T) {
	app := &App{Config: NewConfig(), MetricStore: &store.Store{}}
	if app.loadCompleteIntakeCompatible() {
		t.Fatal("unconfirmed dump points need a durable journal")
	}
	app.Config.InputLog.Enabled = true
	if !app.loadCompleteIntakeCompatible() {
		t.Fatal("journal-backed intake unexpectedly disabled")
	}
}

func TestIncrementalIntakeKeepsNamesConfirmedDuringLoad(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg, stamp := legacyIntakeFixture(t, 2, 2)
	dumpPath := cfg.Dump.Path
	cfg.Dump.Path = t.TempDir()
	cfg.Whisper.StoreIncrementalMigration = true
	cfg.Whisper.StoreDir = t.TempDir()
	cfg.Whisper.FLock = true
	cfg.Whisper.StoreMigrationControlFile = filepath.Join(t.TempDir(), "control.json")
	incrementalLease(t, cfg, 1, 0)
	cfg.Carbonserver.FileListCache = filepath.Join(t.TempDir(), "files.flc")
	if err := makeIntakeOriginal(filepath.Join(cfg.Whisper.DataDir, "original", "only.wsp"), stamp); err != nil {
		t.Fatal(err)
	}
	first := New(path)
	first.Config = cfg
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	waitExpiration(t, first.Carbonserver.HasMappedIndex)
	first.Stop()
	// This original is absent from the saved index and never enters the shared
	// catalog. Its restore finishes before the final input dump's delayed row.
	if err := makeIntakeOriginal(filepath.Join(cfg.Whisper.DataDir, "saved", "metric00001.wsp"), stamp); err != nil {
		t.Fatal(err)
	}
	cfg.Dump.Path, cfg.Dump.RestorePerSecond = dumpPath, 1
	app := New(path)
	app.Config = cfg
	defer app.Stop()
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	if app.Cache.RestorePending("saved.metric00001") {
		t.Fatal("fixture did not confirm the original during load")
	}
	for _, name := range []string{"original.only", "saved.metric00000", "saved.metric00001"} {
		if !app.Carbonserver.MetricExists(name) {
			t.Fatal("restored name missing at intake", name)
		}
	}
}
