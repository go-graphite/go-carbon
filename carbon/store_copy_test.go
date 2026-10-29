package carbon

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/storecopy"
	"github.com/go-graphite/go-carbon/points"
)

func TestStoreCopyLiveIngestionReplayAndParity(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "converted-v7"}[incremental], func(t *testing.T) {
			path, cfg := sharedAppConfig(t)
			if incremental {
				path, cfg = incrementalAppFixture(t)
				cfg.Buckyd.Enabled = true
				cfg.Whisper.StoreTimeBuckets = true
				incrementalLease(t, cfg, 2, 4)
			}
			cfg.Whisper.StoreEncoding = "exact"
			cfg.Buckyd.CopyMinFreeBytes = 1
			cfg.Common.MetricInterval = &Duration{time.Hour}
			cfg.InputLog.Enabled = true
			cfg.InputLog.Path = filepath.Join(filepath.Dir(path), "input-log")
			cfg.InputLog.SyncInterval = Duration{10 * time.Millisecond}
			cfg.InputLog.SegmentSize = 16 << 20
			cfg.Tcp.Enabled = true
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			cfg.Tcp.Listen = listener.Addr().String()
			listener.Close()
			if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
				t.Fatal(err)
			}
			writeSharedConfig(t, path, cfg)
			source := New(path)
			if err := source.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			if err := source.Start(); err != nil {
				t.Fatal(err)
			}
			defer source.Stop()
			if incremental {
				waitMigration(t, func() bool { return source.incrementalMigration.Load().receipt()["phase"] == "shared" })
			}
			now := time.Now().Unix()
			const metrics, rounds = 64, 20
			for i := range metrics {
				if incremental {
					source.Cache.Add(points.OnePoint(fmt.Sprintf("copy.%d", i), float64(i), now-180))
					continue
				}
				ret := store.Retention{Step: 1, Count: 3600}
				_, err := source.MetricStore.CreateFromSnapshot(context.Background(), store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: fmt.Sprintf("copy.%d", i), AggregationMethod: store.Average, Retentions: []store.Retention{ret}}}, Archives: []store.Archive{{Retention: ret, Points: []store.Point{{Timestamp: now - 180, Value: float64(i)}}}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if incremental {
				waitCopy(t, func() bool {
					for i := range metrics {
						snap, err := source.MetricStore.Snapshot(context.Background(), fmt.Sprintf("copy.%d", i))
						if err != nil || len(snap.Archives[0].Points) != 1 {
							return false
						}
					}
					return true
				})
			}
			if err := source.MetricStore.Flush(); err != nil {
				t.Fatal(err)
			}
			ranges := make(chan struct{})
			continueCopy := make(chan struct{})
			var once sync.Once
			handler := source.Buckyd.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == storecopy.Endpoint+"/file" {
					once.Do(func() { close(ranges); <-continueCopy })
				}
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			client := &storecopy.Client{URL: server.URL}
			manifest, err := client.Manifest(context.Background(), "begin", "")
			if err != nil {
				t.Fatal(err)
			}
			if err := source.ReloadConfig(); err == nil {
				t.Fatal("reload allowed during copy")
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(root, "clone")
			options := storecopy.PullOptions{Workers: 4, Chunk: 1 << 20, Apply: true}
			copied := make(chan error, 1)
			go func() { copied <- client.Pull(context.Background(), destination, manifest.ID, options) }()
			<-ranges
			conn, err := net.Dial("tcp", cfg.Tcp.Listen)
			if err != nil {
				t.Fatal(err)
			}
			writer := bufio.NewWriter(conn)
			for round := range rounds {
				for i := range metrics + 1 {
					fmt.Fprintf(writer, "copy.%d %d %d\n", i, round*100+i, now-100+int64(round))
				}
			}
			if err := writer.Flush(); err != nil {
				t.Fatal(err)
			}
			conn.Close()
			baselineLogged := int64(0)
			if incremental {
				baselineLogged = metrics
			}
			waitCopy(t, func() bool {
				return source.inputLog.Load().Metrics().PointsWritten >= (metrics+1)*rounds+baselineLogged
			})
			close(continueCopy)
			if err := <-copied; err != nil {
				t.Fatal(err)
			}
			if err := checkStoreConversion(filepath.Join(destination, "store")); err == nil {
				t.Fatal("partial copy was allowed to serve")
			}
			manifest, err = client.Manifest(context.Background(), "seal", manifest.ID)
			if err != nil || !manifest.Sealed {
				t.Fatal(manifest, err)
			}
			if len(source.Receivers) != 0 {
				t.Fatal("source receivers were not fenced")
			}
			options.ReuseVerified = true
			if err := client.Pull(context.Background(), destination, manifest.ID, options); err != nil {
				t.Fatal(err)
			}
			prepare := exec.Command("python3", "../tool/downsize/copy/control.py", "prepare", "--destination", destination, "--apply")
			if incremental {
				prepare.Args = append(prepare.Args, "--reuse-verified")
			}
			prepare.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
			if output, err := prepare.CombinedOutput(); err != nil {
				t.Fatalf("prepare: %v %s", err, output)
			}
			clonePath, cloneCfg := sharedAppConfig(t)
			cloneCfg.Whisper.StoreEncoding = "exact"
			cloneCfg.Whisper.StoreDir = filepath.Join(destination, "store")
			cloneCfg.InputLog = cfg.InputLog
			cloneCfg.InputLog.Path = filepath.Join(destination, "input-log")
			cloneCfg.Common.MetricInterval = &Duration{time.Hour}
			if incremental {
				cloneCfg.Whisper.StoreIncrementalMigration = true
				cloneCfg.Whisper.StoreTimeBuckets = true
				cloneCfg.Whisper.FLock = true
				cloneCfg.Whisper.Compressed = true
				cloneCfg.Whisper.DataDir = filepath.Join(destination, "empty-originals")
				cloneCfg.Carbonserver.TrieIndex = true
				cloneCfg.Carbonserver.TrigramIndex = false
				cloneCfg.Carbonserver.ConcurrentIndex = true
				cloneCfg.Carbonserver.RealtimeIndex = 100
				cloneCfg.Carbonserver.FileListCache = filepath.Join(destination, "rebuilt.flc")
				cloneCfg.Carbonserver.FileListCacheVersion = 2
			}
			if err := os.WriteFile(cloneCfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
				t.Fatal(err)
			}
			writeSharedConfig(t, clonePath, cloneCfg)
			clone := New(clonePath)
			if err := clone.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			if err := clone.Start(); err != nil {
				t.Fatal(err)
			}
			defer clone.Stop()
			waitCopy(t, func() bool {
				for i := range metrics + 1 {
					snap, err := clone.MetricStore.Snapshot(context.Background(), fmt.Sprintf("copy.%d", i))
					if err != nil {
						return false
					}
					expected := rounds
					if i < metrics {
						expected++
					}
					if len(snap.Archives[0].Points) != expected {
						return false
					}
				}
				return true
			})
			for i := range metrics + 1 {
				snap, err := clone.MetricStore.Snapshot(context.Background(), fmt.Sprintf("copy.%d", i))
				if err != nil {
					t.Fatal(err)
				}
				got := map[int64]float64{}
				for _, point := range snap.Archives[0].Points {
					got[point.Timestamp] = point.Value
				}
				if i < metrics && got[now-180] != float64(i) {
					t.Fatal("baseline missing", i)
				}
				for round := range rounds {
					value, ok := got[now-100+int64(round)]
					if !ok || value != float64(round*100+i) {
						t.Fatal("live point missing", i, round, got)
					}
				}
			}
			stats := clone.inputLog.Load().Metrics()
			if stats.ReplayedPoints < (metrics+1)*rounds {
				data, _ := json.Marshal(stats)
				t.Fatal("wrong replay count", string(data))
			}
			if _, err := client.Manifest(context.Background(), "release", manifest.ID); err != nil {
				t.Fatal(err)
			}
			if paths, err := filepath.Glob(filepath.Join(cfg.Whisper.StoreDir, ".store-copy-*")); err != nil || len(paths) != 0 {
				t.Fatal("source checkpoint was not cleaned", paths, err)
			}
			clone.Stop()
			restarted := New(clonePath)
			if err := restarted.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			if err := restarted.Start(); err != nil {
				t.Fatal(err)
			}
			defer restarted.Stop()
			waitCopy(t, func() bool {
				snap, err := restarted.MetricStore.Snapshot(context.Background(), "copy.64")
				return err == nil && len(snap.Archives[0].Points) == rounds
			})
			t.Logf("copied %d baseline metrics and replayed %d live points with zero missing", metrics, stats.ReplayedPoints)
		})
	}
}

func waitCopy(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !check() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("copy condition timed out")
		}
	}
}
