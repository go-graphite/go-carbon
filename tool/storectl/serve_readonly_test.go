package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/downsize"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/golang-jwt/jwt/v5"
)

// Keep the normal donor/target's writable disk-admission test independent of
// the developer's free space. Actual data and the read-only reader use disk.
type sourceFixtureFS struct{ vfs.FS }

func (sourceFixtureFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{TotalBytes: 1 << 40, AvailBytes: 1 << 39}, nil
}

type sourceFileState struct {
	Size   int64
	Mtime  time.Time
	SHA256 [32]byte
}

func sourceTree(t *testing.T, root string) map[string]sourceFileState {
	t.Helper()
	out := map[string]sourceFileState{}
	err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		st, err := e.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		row := sourceFileState{Size: st.Size(), Mtime: st.ModTime()}
		if !e.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			row.SHA256 = sha256.Sum256(data)
		}
		out[rel] = row
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func sourceUnchanged(t *testing.T, root string, before map[string]sourceFileState) {
	t.Helper()
	after := sourceTree(t, root)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("store tree changed:\nbefore=%v\nafter=%v", before, after)
	}
	t.Logf("read-only proof: %d entries retain size, mtime and SHA-256; no added/deleted paths", len(before))
}
func sourceConfig(t *testing.T, root string, existingSecret ...string) (string, string) {
	t.Helper()
	secret := filepath.Join(t.TempDir(), "jwt")
	if err := os.WriteFile(secret, []byte("source-test-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if len(existingSecret) != 0 {
		secret = existingSecret[0]
	}
	cfg := filepath.Join(t.TempDir(), "carbon.conf")
	body := fmt.Sprintf("[whisper]\nstorage-backend = 'pebble-chunk'\nstore-dir = %q\n[buckyd]\nauth-jwt-secret-file = %q\ntmpdir = %q\nmax_transfers = 2\n", root, secret, filepath.Join(root, "must-not-create"))
	if err := os.WriteFile(cfg, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return cfg, secret
}
func sourceJWT(t *testing.T, secret string, namespaces, ops []string) string {
	t.Helper()
	key, err := os.ReadFile(secret)
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, buckyd.ACL{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}, Namespaces: namespaces, Ops: ops}).SignedString(bytes.TrimSpace(key))
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func sourceRequest(t *testing.T, method, url, token, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Buckyd-Authorization", token)
	client := &http.Client{Timeout: 20 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
func sourceOptions(continuation bool) chunkstore.Options {
	opts := chunkstore.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	if continuation {
		opts.Continuation = &whisperio.ContinuationCodec{}
		opts.TimeBuckets = true
	}
	return opts.WithFS(sourceFixtureFS{vfs.Default})
}
func sourceMetric(t *testing.T, db *chunkstore.Store, name string, values []chunkstore.Point, count int) {
	t.Helper()
	if _, err := db.Create(context.Background(), chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 1, Count: count}}, AggregationMethod: chunkstore.Sum, XFilesFactor: .5}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(context.Background(), name, values); err != nil {
		t.Fatal(err)
	}
}
func sourceFixture(t *testing.T, continuation bool) (string, []string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "store")
	db, err := chunkstore.Open(root, sourceOptions(continuation))
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"fixture.a", "fixture.b", "fixture.zero"}
	now := time.Now().Unix() - 3600
	for i, name := range names {
		value := float64(i + 7)
		if i == 2 {
			value = math.Copysign(0, -1)
		}
		sourceMetric(t, db, name, []chunkstore.Point{{Timestamp: now, Value: value}, {Timestamp: now + 1, Value: 12}}, 7*86400)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return root, names
}
func sourceService(t *testing.T, db *chunkstore.Store, secret string) *httptest.Server {
	t.Helper()
	svc, err := buckyd.New(buckyd.Config{JWTSecretFile: secret, TmpDir: t.TempDir(), TransferSources: []string{"127.0.0.1"}, MaxTransfers: 2}, db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(svc.Handler())
	t.Cleanup(func() {
		srv.Close()
		if err := svc.Stop(); err != nil {
			t.Error(err)
		}
	})
	return srv
}
func sourceCarbon(t *testing.T, db *chunkstore.Store, address string) *carbonserver.CarbonserverListener {
	t.Helper()
	listener := carbonserver.NewCarbonserverListener(cache.New().Get)
	listener.SetMetricStore(db)
	listener.SetWhisperData(t.TempDir())
	listener.SetScanFrequency(time.Hour)
	listener.SetMaxGlobs(100)
	listener.SetMaxMetricsGlobbed(100)
	listener.SetMaxMetricsRendered(100)
	listener.SetMaxFetchDataGoroutines(2)
	if err := listener.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if err := listener.Listen(address); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Stop() })
	return listener
}
func sourceCounts(t *testing.T, state string) map[string]int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var row struct {
			Kind                             string
			Names, Copied, Missing, OnTarget int
			Failed, Absent                   []string
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		// Periodic progress checkpoints depend on wall-clock scheduling, not
		// sync results. Compare all logical journal rows and outcome counters.
		if row.Kind == "phase-progress" {
			continue
		}
		out[row.Kind+"/rows"]++
		out[row.Kind+"/names"] += row.Names
		out[row.Kind+"/copied"] += row.Copied
		out[row.Kind+"/missing"] += row.Missing
		out[row.Kind+"/on-target"] += row.OnTarget
		out[row.Kind+"/failed"] += len(row.Failed)
		out[row.Kind+"/absent"] += len(row.Absent)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestServeReadOnlySyncDifferential(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		for _, window := range []bool{false, true} {
			t.Run(fmt.Sprintf("continuation=%v/window=%v", continuation, window), func(t *testing.T) {
				root, names := sourceFixture(t, continuation)
				_, secret := sourceConfig(t, root)
				copied := filepath.Join(t.TempDir(), "copy")
				if err := os.CopyFS(copied, os.DirFS(root)); err != nil {
					t.Fatal(err)
				}
				rocfg, _ := sourceConfig(t, copied, secret)
				before := sourceTree(t, copied)
				db, err := chunkstore.Open(root, sourceOptions(continuation))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				normal := sourceService(t, db, secret)
				normalCarbon := sourceCarbon(t, db, "127.0.0.1:0")
				carbonPort := normalCarbon.HTTPListener().Addr().(*net.TCPAddr).Port
				// Bind target reads to IPv6 so the existing one-port-per-host CLI can
				// address independent live source and target carbonservers on loopback.
				var oracle map[string]chunkstore.Snapshot
				var counts map[string]int
				for _, readonly := range []bool{false, true} {
					target, err := chunkstore.Open(filepath.Join(t.TempDir(), "target"), sourceOptions(continuation))
					if err != nil {
						t.Fatal(err)
					}
					if window {
						original, err := db.Snapshot(context.Background(), names[1])
						if err != nil {
							t.Fatal(err)
						}
						sourceMetric(t, target, names[1], []chunkstore.Point{{Timestamp: original.Archives[0].Points[0].Timestamp, Value: 99}}, 7*86400)
					}
					targetService, err := buckyd.New(buckyd.Config{JWTSecretFile: secret, TmpDir: t.TempDir(), TransferSources: []string{"127.0.0.1"}, MaxTransfers: 2}, target)
					if err != nil {
						t.Fatal(err)
					}
					listener, err := net.Listen("tcp6", "[::1]:0")
					if err != nil {
						t.Fatal(err)
					}
					targetHTTP := httptest.NewUnstartedServer(targetService.Handler())
					targetHTTP.Listener = listener
					targetHTTP.Start()
					targetCarbon := sourceCarbon(t, target, fmt.Sprintf("[::1]:%d", carbonPort))
					origin := normal.URL
					var stop context.CancelFunc
					var done chan error
					if readonly {
						_ = normalCarbon.Stop()
						addrListener, err := net.Listen("tcp", "127.0.0.1:0")
						if err != nil {
							t.Fatal(err)
						}
						address := addrListener.Addr().String()
						addrListener.Close()
						ctx, cancel := context.WithCancel(context.Background())
						stop = cancel
						done = make(chan error, 1)
						go func() { done <- runReadOnlySource(ctx, rocfg, address) }()
						origin = "http://" + address
						waitSource(t, origin, sourceJWT(t, secret, []string{"*"}, []string{"read"}))
						sourceUnchanged(t, copied, before)
					}
					state := filepath.Join(t.TempDir(), "sync")
					namesFile := filepath.Join(t.TempDir(), "names")
					if err := os.WriteFile(namesFile, []byte(strings.Join(names, "\n")+"\n"), 0600); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
					args := []string{"--names", namesFile, "--source", origin, "--to", targetHTTP.URL, "--state", state, "--jwt-secret", secret, "--carbonserver-port", fmt.Sprint(carbonPort), "--streams=1", "--target-health=false", "--settle=0s"}
					if window {
						args = append(args, "--window", fmt.Sprintf("past:%d:%d", time.Now().Unix()-7200, time.Now().Unix()-600), "--no-full-history")
					}
					err = downsize.SyncMain(ctx, args)
					cancel()
					if readonly {
						stop()
						if e := <-done; e != nil {
							t.Error(e)
						}
						sourceUnchanged(t, copied, before)
					}
					if err != nil {
						t.Fatal(err)
					}
					got := map[string]chunkstore.Snapshot{}
					for _, name := range names {
						snapshot, err := target.Snapshot(context.Background(), name)
						if err != nil {
							t.Fatal(err)
						}
						got[name] = snapshot
						want, err := db.Snapshot(context.Background(), name)
						if err != nil {
							t.Fatal(err)
						}
						if window && name == names[1] {
							var found, filled bool
							for _, point := range snapshot.Archives[0].Points {
								if point.Value == 12 {
									filled = true
								}
								if point.Value == 99 {
									found = true
								}
							}
							if !found || !filled {
								t.Fatal("target-wins merge lost an existing value or a source gap point")
							}
						} else if err := chunkstore.EqualSnapshots(want, snapshot); err != nil {
							t.Fatal(name, err)
						}
					}
					currentCounts := sourceCounts(t, state)
					if readonly {
						for _, name := range names {
							if err := chunkstore.EqualSnapshots(oracle[name], got[name]); err != nil {
								t.Fatal(name, err)
							}
						}
						if !reflect.DeepEqual(counts, currentCounts) {
							t.Fatalf("journal counts differ: %v != %v", counts, currentCounts)
						}
						t.Logf("normal/read-only targets and journal counts identical: %v", currentCounts)
					} else {
						oracle, counts = got, currentCounts
					}
					targetHTTP.Close()
					_ = targetService.Stop()
					_ = targetCarbon.Stop()
					if err := target.Close(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
func waitSource(t *testing.T, origin, token string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", origin+"/v1/transfer/status", nil)
		req.Header.Set("X-Buckyd-Authorization", token)
		resp, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("read-only source did not become ready")
}

func TestServeReadOnlyAuthAndRoutes(t *testing.T) {
	root, _ := sourceFixture(t, true)
	config, secret := sourceConfig(t, root)
	before := sourceTree(t, root)
	db, cfg, err := openReadOnlySource(config)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := buckyd.New(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(svc.Handler())
	read := sourceJWT(t, secret, []string{"*"}, []string{"read"})
	// Only a root read grant is sufficient; updates alone do not authorize reads.
	for _, token := range []string{"", "invalid", sourceJWT(t, secret, []string{"fixture.*"}, []string{"read"}), sourceJWT(t, secret, []string{"*"}, []string{"update"})} {
		for _, path := range []string{"/v1/transfer/status", "/v1/transfer/list", "/v1/native/capabilities"} {
			resp := sourceRequest(t, "GET", srv.URL+path, token, "")
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("unauthorized %s: %d", path, resp.StatusCode)
			}
		}
		for _, path := range []string{"/v1/native/source", "/v1/native/export", "/v1/transfer/digest", "/info/", "/render/"} {
			resp := sourceRequest(t, "POST", srv.URL+path, token, "{}")
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("unauthorized %s: %d", path, resp.StatusCode)
			}
		}
	}
	for _, path := range []string{"/v1/native/import", "/v1/native/pull", "/v1/native/abandon", "/v1/transfer/import", "/v1/transfer/pull", "/v1/transfer/sync", "/v1/transfer/delete", "/v1/transfer/stage-delete", "/v1/transfer/stage-copy", "/v1/transfer/retire", "/v1/store-copy", "/v1/maintenance/archive-anchors", "/v1/maintenance/sidecars", "/v1/activity-backfill", "/v1/expiration/evidence", "/new-admin-endpoint"} {
		resp := sourceRequest(t, "POST", srv.URL+path, read, "{}")
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("write route %s: %d", path, resp.StatusCode)
		}
	}
	for _, method := range []string{"POST", "PUT", "DELETE", "PATCH"} {
		resp := sourceRequest(t, method, srv.URL+"/metrics/fixture.a", read, "{}")
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("write metric %s: %d", method, resp.StatusCode)
		}
	}
	// Exact-name point reads omit absent names and out-of-retention ranges.
	now := time.Now().Unix()
	for _, from := range []int64{now - 7200, now - 9*86400, now + 600} {
		request := &protov3.MultiFetchRequest{Metrics: []*protov3.FetchRequest{{Name: "fixture.a", StartTime: from, StopTime: from + 3600}, {Name: "absent", StartTime: from, StopTime: from + 3600}}}
		raw, err := request.MarshalVT()
		if err != nil {
			t.Fatal(err)
		}
		resp := sourceRequest(t, "POST", srv.URL+"/render/?format=carbonapi_v3_pb", read, string(raw))
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("render status %d: %s", resp.StatusCode, data)
		}
		var response protov3.MultiFetchResponse
		if err := response.UnmarshalVT(data); err != nil {
			t.Fatal(err)
		}
		if from == now-7200 {
			if len(response.Metrics) != 1 || response.Metrics[0].Name != "fixture.a" {
				t.Fatal("known metric missing or absent metric included")
			}
		} else if len(response.Metrics) != 0 {
			t.Fatal("out-of-retention read returned metrics")
		}
	}
	// Exercise both disposable-digest cache and proof reads on continuation data.
	for _, mode := range []string{"sum-v1", "proof-v1", "proof-v2"} {
		from, until := int64(0), int64(0)
		if mode == "proof-v2" {
			from = time.Now().Unix() - 7200
			until = from + 3600
		}
		body := fmt.Sprintf(`{"Names":["fixture.a","fixture.zero"],"Mode":%q,"From":%d,"Until":%d}`, mode, from, until)
		resp := sourceRequest(t, "POST", srv.URL+"/v1/transfer/digest", read, body)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("digest %s: %d %s", mode, resp.StatusCode, raw)
		}
	}
	sourceUnchanged(t, root, before)
	srv.Close()
	if err := svc.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	sourceUnchanged(t, root, before)
	// No authentication configuration must fail closed before listening.
	db, _, err = openReadOnlySource(config)
	if err != nil {
		t.Fatal(err)
	}
	if svc, err := buckyd.New(buckyd.Config{}, db); err == nil {
		_ = svc.Stop()
		t.Error("read-only service accepted missing JWT configuration")
	}
	_ = db.Close()
}

func TestServeReadOnlyFailsClosed(t *testing.T) {
	for _, failure := range []string{"missing-store", "missing-lock", "bad-format", "bad-binding", "missing-secret", "active-writer"} {
		t.Run(failure, func(t *testing.T) {
			root, _ := sourceFixture(t, false)
			config, secret := sourceConfig(t, root)
			switch failure {
			case "active-writer":
				writer, err := chunkstore.Open(root, chunkstore.Options{ExactBlocks: true})
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close()
			case "missing-store":
				root = filepath.Join(t.TempDir(), "missing")
				config, _ = sourceConfig(t, root)
			case "missing-lock":
				if err := os.Remove(filepath.Join(root, "LOCK")); err != nil {
					t.Fatal(err)
				}
			case "bad-format":
				if err := os.WriteFile(filepath.Join(root, "CHUNKSTORE"), []byte("go-carbon-chunks-v999\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "bad-binding":
				if err := os.WriteFile(filepath.Join(root, "INCREMENTAL-FILE-MIGRATION"), []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-secret":
				if err := os.Remove(secret); err != nil {
					t.Fatal(err)
				}
			}
			var before map[string]sourceFileState
			if failure != "missing-store" {
				before = sourceTree(t, root)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := runReadOnlySource(ctx, config, "127.0.0.1:0"); err == nil {
				t.Fatal("invalid source started")
			}
			if before != nil {
				sourceUnchanged(t, root, before)
			} else if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatal("missing source directory was created", err)
			}
		})
	}
}

// Run the real command in a child test process so SIGTERM reaches its signal
// handler, including under go test -race. The parent keeps the filesystem proof.
func TestServeReadOnlyProcess(t *testing.T) {
	config := os.Getenv("STORECTL_READONLY_TEST_CONFIG")
	if config == "" {
		return
	}
	if err := serveReadOnly([]string{"--config", config, "--listen", os.Getenv("STORECTL_READONLY_TEST_LISTEN")}); err != nil {
		t.Fatal(err)
	}
}
func TestServeReadOnlySIGTERMMidExport(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	db, err := chunkstore.Open(root, sourceOptions(false))
	if err != nil {
		t.Fatal(err)
	}
	// Incompressible values keep the stream larger than the socket buffers.
	const count = 1500000
	points := make([]chunkstore.Point, count)
	now := time.Now().Unix() - 3600
	value := uint64(1)
	for i := range points {
		value ^= value << 13
		value ^= value >> 7
		value ^= value << 17
		points[i] = chunkstore.Point{Timestamp: now - int64(count) + int64(i), Value: math.Float64frombits((value & ((1 << 52) - 1)) | uint64(1023)<<52)}
	}
	sourceMetric(t, db, "large.export", points, 2*count)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	config, secret := sourceConfig(t, root)
	before := sourceTree(t, root)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestServeReadOnlyProcess$", "-test.v")
	command.Env = append(os.Environ(), "STORECTL_READONLY_TEST_CONFIG="+config, "STORECTL_READONLY_TEST_LISTEN="+address)
	var log bytes.Buffer
	command.Stdout = &log
	command.Stderr = &log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	origin := "http://" + address
	token := sourceJWT(t, secret, []string{"*"}, []string{"read"})
	waitSource(t, origin, token)
	sourceUnchanged(t, root, before)
	pin := sourceRequest(t, "POST", origin+"/v1/native/source", token, "{}")
	var session struct{ ID string }
	if err := json.NewDecoder(pin.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	pin.Body.Close()
	if session.ID == "" {
		t.Fatal("missing source session")
	}
	response := sourceRequest(t, "POST", origin+"/v1/native/export", token, fmt.Sprintf(`{"Session":%q,"Names":["large.export"]}`, session.ID))
	if response.StatusCode != 200 {
		raw, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("export: %d %s", response.StatusCode, raw)
	}
	first := make([]byte, 1024)
	if _, err := io.ReadFull(response.Body, first); err != nil {
		t.Fatal(err)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		waited = true
		if err != nil {
			t.Fatalf("SIGTERM shutdown: %v\n%s", err, log.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("SIGTERM did not stop active export")
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if readErr == nil {
		t.Fatal("expected SIGTERM to interrupt the active export stream")
	}
	t.Logf("SIGTERM interrupted export: %v", readErr)
	sourceUnchanged(t, root, before)
}

func TestServeReadOnlyRetainsMigrationBinding(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	original := t.TempDir()
	options := sourceOptions(true)
	options.IncrementalSource = original
	db, err := chunkstore.Open(root, options)
	if err != nil {
		t.Fatal(err)
	}
	sourceMetric(t, db, "bound.metric", []chunkstore.Point{{Timestamp: time.Now().Unix() - 3600, Value: 42}}, 7*86400)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	config, _ := sourceConfig(t, root)
	before := sourceTree(t, root)
	sourceBefore := sourceTree(t, original)
	db, _, err = openReadOnlySource(config)
	if err != nil {
		t.Fatal(err)
	}
	if db.IncrementalSource() != original {
		t.Fatalf("source binding changed: %s", db.IncrementalSource())
	}
	if _, err := db.Snapshot(context.Background(), "bound.metric"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	sourceUnchanged(t, root, before)
	sourceUnchanged(t, original, sourceBefore)
}
