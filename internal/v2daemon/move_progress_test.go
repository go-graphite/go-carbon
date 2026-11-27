package v2daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func moveProgressFixture(t *testing.T) (*MoveAdmin, *Store, *moveRecord) {
	t.Helper()
	root := t.TempDir()
	store, e := Open(root, Config{Layer: 3, Producers: 1, Shards: 1})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = store.Close() })
	admin := NewMoveAdmin(root, store, func(_ int, run func() error) error { return run() })
	m, e := admin.create(strings.Repeat("a", 64), 100, []MoveFileStatus{{Name: strings.Repeat("b", 64), Size: 9, Checksum: strings.Repeat("b", 64), State: "pending"}})
	if e != nil {
		t.Fatal(e)
	}
	return admin, store, m
}
func TestMoveProgressRejectsRegression(t *testing.T) {
	admin, _, m := moveProgressFixture(t)
	m.Phase = "verified"
	m.Parts[0].State = "verified"
	m.Parts[0].Owners[0] = "verified"
	m.Owners[0].State = "verified"
	m.PartsDone = 1
	m.BytesDone = 9
	m.VerifyCount = 1
	m.BytesTransferred = 9
	if e := admin.save(m); e != nil {
		t.Fatal(e)
	}
	cases := []struct {
		name   string
		change func(*moveRecord)
	}{
		{"phase", func(m *moveRecord) { m.Phase = "received" }},
		{"part", func(m *moveRecord) { m.Parts[0].State = "received" }},
		{"part-owner", func(m *moveRecord) { m.Parts[0].Owners[0] = "received" }},
		{"owner", func(m *moveRecord) { m.Owners[0].State = "pending" }},
		{"parts", func(m *moveRecord) { m.PartsDone = 0 }},
		{"bytes", func(m *moveRecord) { m.BytesDone = 0 }},
		{"transfer", func(m *moveRecord) { m.BytesTransferred = 0 }},
		{"verify", func(m *moveRecord) { m.VerifyCount = 0 }},
	}
	path := filepath.Join(admin.root, m.ID, "record.json")
	before, _ := os.ReadFile(path)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			next, e := admin.load(m.ID)
			if e != nil {
				t.Fatal(e)
			}
			c.change(next)
			if e = admin.save(next); e == nil || !strings.Contains(e.Error(), "regression") {
				t.Fatalf("regression accepted: %v", e)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(after, before) {
				t.Fatal("refused progress changed record")
			}
		})
	}
	if admin.MoveRegressions() != uint64(len(cases)) {
		t.Fatal("missing rejection metric")
	}
}
func TestMoveProgressFailureAndVersion(t *testing.T) {
	admin, store, m := moveProgressFixture(t)
	request := httptest.NewRequest(http.MethodPut, "/admin/v2/move/part?id="+m.ID+"&part="+m.Parts[0].Name, strings.NewReader("bad"))
	response := httptest.NewRecorder()
	admin.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatal(response.Code)
	}
	m, e := admin.load(m.ID)
	if e != nil {
		t.Fatal(e)
	}
	if m.FailureCount != 1 || len(m.Failures) != 1 || m.Owners[0].Reason == "" {
		t.Fatalf("missing failure status: %+v", m)
	}
	b, e := os.ReadFile(filepath.Join(filepath.Dir(admin.root), "restart.json"))
	if e != nil {
		t.Fatal(e)
	}
	var geometry restartGeometry
	if e = json.Unmarshal(b, &geometry); e != nil {
		t.Fatal(e)
	}
	if geometry.Version != restartVersion {
		t.Fatal("old binary would accept move root")
	}
	m.Version = 99
	b, _ = json.Marshal(m)
	if e = writeMoveFile(filepath.Join(admin.root, m.ID), "record.json", b); e != nil {
		t.Fatal(e)
	}
	before, _ := os.ReadFile(filepath.Join(admin.root, m.ID, "record.json"))
	response = httptest.NewRecorder()
	admin.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/admin/v2/move/adopt?id="+m.ID, nil))
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "unsupported") {
		t.Fatal("unknown record accepted", response.Body.String())
	}
	after, _ := os.ReadFile(filepath.Join(admin.root, m.ID, "record.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("unknown version touched")
	}
	_ = store.Close()
	if reopened, e := Open(filepath.Dir(admin.root), Config{Layer: 3, Producers: 1, Shards: 1}); e == nil {
		_ = reopened.Close()
		t.Fatal("daemon opened unknown move format")
	} else if !strings.Contains(e.Error(), "move record version") {
		t.Fatal(e)
	}
}
func TestMoveProgressDoneCleanupOnRestart(t *testing.T) {
	admin, _, m := moveProgressFixture(t)
	m.Done = true
	m.Phase = "done"
	m.PartsDone = 1
	m.BytesDone = 9
	m.Parts = nil
	if e := admin.save(m); e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(admin.root, m.ID)
	if e := os.WriteFile(filepath.Join(dir, "retained.part"), []byte("pin"), 0600); e != nil {
		t.Fatal(e)
	}
	_ = NewMoveAdmin(filepath.Dir(admin.root), admin.store, admin.onOwner)
	entries, e := os.ReadDir(dir)
	if e != nil || len(entries) != 1 || entries[0].Name() != "record.json" {
		t.Fatalf("done cleanup: %v %v", entries, e)
	}
}

func TestMoveProgressImportsLegacyCertificate(t *testing.T) {
	for _, done := range []bool{false, true} {
		t.Run(fmt.Sprint(done), func(t *testing.T) {
			cfg := Config{Layer: 3, Producers: 1, Shards: 1}
			sourceRoot := t.TempDir()
			source, e := Open(sourceRoot, cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer source.Close()
			if e = source.Producers[0].AppendApplications(restartApps(t, 0, 1)); e != nil {
				t.Fatal(e)
			}
			if e = source.Producers[0].Flush(); e != nil {
				t.Fatal(e)
			}
			snapshot := source.Core.DBs()[0].NewSnapshot()
			files, e := (v2core.SealedSnapshotReader{Snapshot: snapshot, Directory: filepath.Join(sourceRoot, "placement-group-00000001", "core", "owner-000")}).ReadPartFiles(t.Context())
			_ = snapshot.Close()
			if e != nil {
				t.Fatal(e)
			}
			var parts []v2core.MovePart
			for _, file := range files {
				if !strings.HasSuffix(file.Path, ".descriptor") {
					continue
				}
				b, e := os.ReadFile(file.Path)
				if e != nil {
					t.Fatal(e)
				}
				d, e := v2part.ParseDescriptor(b)
				if e != nil {
					t.Fatal(e)
				}
				parts = append(parts, v2core.MovePart{Path: strings.TrimSuffix(file.Path, ".descriptor"), Descriptor: d})
			}
			root := t.TempDir()
			target, e := Open(root, cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer target.Close()
			id := strings.Repeat("c", 64)
			dir := filepath.Join(root, "moves", id)
			if e = os.MkdirAll(dir, 0700); e != nil {
				t.Fatal(e)
			}
			prepared, e := v2core.PrepareMove(t.Context(), dir, parts)
			if e != nil {
				t.Fatal(e)
			}
			if e = prepared.Close(); e != nil {
				t.Fatal(e)
			}
			for i := range parts {
				parts[i].Path = filepath.Join(dir, fmt.Sprintf("%x.sst", parts[i].Descriptor.SHA256))
			}
			request := MoveRequest{Parts: parts, Now: 10030}
			b, _ := json.Marshal(request)
			if e = writeMoveFile(dir, "request.json", b); e != nil {
				t.Fatal(e)
			}
			if done {
				if e = writeMoveFile(dir, "done", []byte("done\n")); e != nil {
					t.Fatal(e)
				}
			}
			admin := NewMoveAdmin(root, target, func(_ int, run func() error) error { return run() })
			m, e := admin.load(id)
			if e != nil {
				t.Fatal(e)
			}
			if m.PartsDone != len(parts) || m.VerifyCount != 0 || m.Done != done {
				t.Fatalf("legacy certificate lost: %+v", m)
			}
			response := httptest.NewRecorder()
			admin.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/admin/v2/move/adopt?id="+id, nil))
			if response.Code != 200 {
				t.Fatal(response.Body.String())
			}
			m, e = admin.load(id)
			if e != nil || !m.Done || m.VerifyCount != 0 {
				t.Fatal("legacy certificate reverified", e)
			}
			entries, e := os.ReadDir(dir)
			if e != nil || len(entries) != 1 {
				t.Fatalf("legacy cleanup: %v %v", entries, e)
			}
		})
	}
}

// The begin receipt must bind donor dictionary ranges before any file arrives;
// recreating the admin after interruption must recover that same namespace.
func TestMoveBeginRetainsDictionary(t *testing.T) {
	admin, store, _ := moveProgressFixture(t)
	hash := strings.Repeat("d", 64)
	snapshot := v2core.PartSnapshot{Version: 1, Parts: []v2core.SnapshotPart{{ID: hash, SHA256: hash, Bytes: 42, Dictionary: &v2core.MoveDictionary{Generation: 7, Names: []v2core.MoveDictionaryName{{VShard: v2part.Route("move.metric"), Name: "move.metric", Entry: v2dict.Entry{ID: 17}}}}}}}
	body, err := json.Marshal(MoveCopyRequest{Snapshot: snapshot, Now: 100})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	admin.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/admin/v2/move/begin", bytes.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	var status MoveStatus
	if err = json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	reopened := NewMoveAdmin(filepath.Dir(admin.root), store, func(_ int, run func() error) error { return run() })
	record, err := reopened.load(status.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Snapshot == nil || !reflect.DeepEqual(*record.Snapshot, snapshot) {
		t.Fatal("begin receipt lost donor dictionary")
	}
}
