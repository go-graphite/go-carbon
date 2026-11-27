package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2daemon"
)

func TestMoveProgressCopierProcess(t *testing.T) {
	if os.Getenv("MOVE_PROGRESS_COPY") != "1" {
		return
	}
	if e := copyMove(os.Getenv("MOVE_PROGRESS_TARGET"), os.Getenv("MOVE_PROGRESS_SOURCE"), os.Getenv("MOVE_PROGRESS_ID"), 0, io.Discard); e != nil {
		t.Fatal(e)
	}
}
func progressCall(t *testing.T, base, action string, body any) v2daemon.MoveStatus {
	t.Helper()
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	method := http.MethodPost
	if strings.HasPrefix(action, "status") {
		method = http.MethodGet
	}
	r, e := http.NewRequest(method, base+"/admin/v2/move/"+action, bytes.NewReader(data))
	if e != nil {
		t.Fatal(e)
	}
	response, e := (&http.Client{Timeout: 20 * time.Second}).Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		b, _ := io.ReadAll(response.Body)
		t.Fatalf("%s: %s", action, b)
	}
	var s v2daemon.MoveStatus
	if e = json.NewDecoder(response.Body).Decode(&s); e != nil {
		t.Fatal(e)
	}
	return s
}
func startProgressCopier(t *testing.T, target, source, id string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMoveProgressCopierProcess$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), "MOVE_PROGRESS_COPY=1", "MOVE_PROGRESS_TARGET="+target, "MOVE_PROGRESS_SOURCE="+source, "MOVE_PROGRESS_ID="+id)
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return cmd
}
func waitProgressCut(t *testing.T, root string) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat(filepath.Join(root, "cut-fired")); e == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("move did not reach cut")
}
func TestMoveProgressProcessResume(t *testing.T) {
	donor := startMoveChild(t, t.TempDir(), "", false)
	reference := startMoveChild(t, t.TempDir(), "", false)
	var snapshot moveSnapshot
	moveGet(t, donor.addr.HTTP, "/snapshot", &snapshot)
	// Descriptors first make verification available as each data part lands.
	sort.Slice(snapshot.Manifest.Parts, func(i, j int) bool { return snapshot.Manifest.Parts[i].Bytes < snapshot.Manifest.Parts[j].Bytes })
	var expected map[string]map[string]uint64
	moveGet(t, reference.addr.HTTP, "/read", &expected)
	for _, who := range []string{"copier", "daemon"} {
		for _, boundary := range []string{"mid-file", "move-verified-part", "move-progress-staged", "move-owner-active"} {
			t.Run(who+"/"+boundary, func(t *testing.T) {
				root := t.TempDir()
				if e := os.WriteFile(filepath.Join(root, "target"), []byte("4"), 0600); e != nil {
					t.Fatal(e)
				}
				t.Setenv("MOVE_PROGRESS_CUT", boundary)
				target := startMoveChild(t, root, "", false)
				var mu sync.Mutex
				requests := map[string]int{}
				block := boundary == "mid-file"
				blocked := make(chan struct{}, 1)
				last := snapshot.Manifest.Parts[len(snapshot.Manifest.Parts)-1]
				handler := partSnapshotHandler(snapshot.Directory, snapshot.Manifest, "")
				source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasPrefix(r.URL.Path, "/parts/") {
						mu.Lock()
						requests[strings.TrimPrefix(r.URL.Path, "/parts/")]++
						shouldBlock := block && r.URL.Path == "/parts/"+last.ID
						mu.Unlock()
						if shouldBlock {
							w.Header().Set("Content-Length", fmt.Sprint(last.Bytes))
							f, e := os.Open(filepath.Join(snapshot.Directory, last.ID+".part"))
							if e != nil {
								t.Error(e)
								return
							}
							_, _ = io.CopyN(w, f, max(int64(1), last.Bytes/2))
							_ = f.Close()
							w.(http.Flusher).Flush()
							select {
							case blocked <- struct{}{}:
							default:
							}
							<-r.Context().Done()
							return
						}
					}
					handler.ServeHTTP(w, r)
				}))
				defer source.Close()
				initial := progressCall(t, target.addr.HTTP, "begin", v2daemon.MoveCopyRequest{Snapshot: snapshot.Manifest, Now: time.Now().Unix()})
				copier := startProgressCopier(t, target.addr.HTTP, source.URL, initial.ID)
				if boundary == "mid-file" {
					select {
					case <-blocked:
					case <-time.After(25 * time.Second):
						t.Fatal("copier did not reach mid-file")
					}
				} else {
					waitProgressCut(t, root)
				}
				before := progressCall(t, target.addr.HTTP, "status?id="+initial.ID, nil)
				verified := map[string]int{}
				mu.Lock()
				for _, p := range before.Parts {
					if p.State == "verified" || p.State == "staged" || p.State == "active" {
						verified[p.Name] = requests[p.Name]
					}
				}
				mu.Unlock()
				if len(verified) == 0 {
					t.Fatal("cut has no verified part")
				}
				if who == "daemon" {
					_ = target.cmd.Process.Kill()
					_ = target.cmd.Wait()
					_ = copier.Process.Kill()
					_ = copier.Wait()
					target = startMoveChild(t, root, "", false)
				} else {
					_ = copier.Process.Kill()
					_ = copier.Wait()
					_ = os.WriteFile(filepath.Join(root, "cut-release"), []byte("resume"), 0600)
				}
				mu.Lock()
				block = false
				mu.Unlock()
				var output bytes.Buffer
				if e := copyMove(target.addr.HTTP, source.URL, initial.ID, 0, &output); e != nil {
					t.Fatal(e)
				}
				after := progressCall(t, target.addr.HTTP, "status?id="+initial.ID, nil)
				if !after.Done || after.PartsDone != after.PartsTotal || after.BytesDone != after.BytesTotal || after.VerifyCount != uint64(after.PartsTotal) {
					t.Fatalf("final progress: %+v", after)
				}
				for i, owner := range before.Owners {
					if owner.State == "active" && after.Owners[i].State != "active" {
						t.Fatalf("completed owner %d regressed", i)
					}
				}
				mu.Lock()
				for id, count := range verified {
					if requests[id] != count {
						t.Errorf("verified part %s resent: %d -> %d", id, count, requests[id])
					}
				}
				mu.Unlock()
				if before.PartsDone == before.PartsTotal && after.VerifyCount != before.VerifyCount {
					t.Fatal("verified parts rechecked")
				}
				var actual map[string]map[string]uint64
				moveGet(t, target.addr.HTTP, "/read", &actual)
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("reference mismatch: names got %d want %d", len(actual), len(expected))
				}
				count := after.VerifyCount
				sent := after.BytesTransferred
				if e := copyMove(target.addr.HTTP, "", initial.ID, 0, io.Discard); e != nil {
					t.Fatal(e)
				}
				again := progressCall(t, target.addr.HTTP, "status?id="+initial.ID, nil)
				if again.VerifyCount != count || again.BytesTransferred != sent {
					t.Fatal("finished move repeated work")
				}
				entries, e := os.ReadDir(filepath.Join(root, "moves", initial.ID))
				if e != nil || len(entries) != 1 || entries[0].Name() != "record.json" {
					t.Fatalf("move links not released: %v %v", entries, e)
				}
				t.Logf("verified=%d/%d, verified resend bytes=0; checks=%d, exact reference names=%d", len(verified), after.PartsTotal, after.VerifyCount, len(actual))
			})
		}
	}
}
