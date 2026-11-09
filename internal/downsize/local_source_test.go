package downsize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestLocalSourceCrashAndSharedJournal(t *testing.T) {
	for _, lost := range []string{"import", "verify", "none"} {
		t.Run(lost, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			open := func(name string) *chunkstore.Store {
				s, err := chunkstore.Open(filepath.Join(root, name), chunkstore.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				})
				return s
			}
			donor, target := open("donor"), open("target")
			config := chunkstore.MetricConfig{Name: "journal.metric", AggregationMethod: chunkstore.Average, Retentions: []chunkstore.Retention{{Step: 1, Count: 3600}}}
			if _, err := donor.Create(ctx, config); err != nil {
				t.Fatal(err)
			}
			if err := donor.UpdateMany(ctx, config.Name, []chunkstore.Point{{Timestamp: time.Now().Unix() - 60, Value: 42}}); err != nil {
				t.Fatal(err)
			}
			checkpoint := filepath.Join(root, "checkpoint")
			if err := donor.Checkpoint(checkpoint); err != nil {
				t.Fatal(err)
			}
			source, err := chunkstore.OpenTransferSource(checkpoint, chunkstore.TransferSourceOptions{CacheBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			var imports, verifies atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/transfer/import":
					imports.Add(1)
					records, err := chunkstore.ReadTransfer(r.Body, 160<<20)
					if err != nil {
						t.Error(err)
						http.Error(w, err.Error(), 400)
						return
					}
					proofs, err := target.ImportTransfer(r.Context(), records)
					if err != nil {
						t.Error(err)
						http.Error(w, err.Error(), 500)
						return
					}
					if lost == "import" {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
						} else {
							conn.Close()
						}
						return
					}
					_ = json.NewEncoder(w).Encode(proofs)
				case "/v1/transfer/verify":
					n := verifies.Add(1)
					var proofs []chunkstore.TransferProof
					if r.Header.Get("Content-Type") == chunkstore.TransferProofContentType {
						proofs, _ = chunkstore.ReadTransferProofs(r.Body)
					} else {
						_ = json.NewDecoder(r.Body).Decode(&proofs)
					}
					if len(proofs) == 0 {
						http.Error(w, "empty proofs", 400)
						return
					}
					if lost == "verify" && n == 1 {
						http.Error(w, "lost verification response", 500)
						return
					}
					if err := target.VerifyTransfer(r.Context(), proofs); err != nil {
						t.Error(err)
						http.Error(w, err.Error(), 500)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]int{"verified": len(proofs)})
				default:
					t.Error("unexpected request", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			state := filepath.Join(root, "state.jsonl")
			j, err := openJournal(state, "plan")
			if err != nil {
				t.Fatal(err)
			}
			task := task{Stream: "stream", End: 1, Source: "source", Target: server.URL, Names: []string{config.Name}}
			makeMover := func(j *journal) *mover {
				return &mover{client: server.Client(), journal: j, localSources: map[string]*chunkstore.TransferSource{"source": source}, tuner: newTestTuner([]string{"source", server.URL}, capacity{1, 1, 160 << 20, 1}, false, 0, 0)}
			}
			m := makeMover(j)
			err = m.execute(ctx, task)
			if (lost == "none") != (err == nil) {
				t.Fatalf("initial result %s: %v", lost, err)
			}
			if _, err := openJournal(state, "plan"); err == nil {
				t.Fatal("concurrent controller acquired shared journal")
			}
			j.close()
			j, err = openJournal(state, "plan")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			resumed := j.tasks[taskKey(task)]
			m = makeMover(j)
			switch lost {
			case "import":
				if resumed.Kind != "bulk-intent" || resumed.Phase != "verified" || len(resumed.Proofs) != 0 {
					t.Fatal("lost durable intent", resumed)
				}
				if err := m.execute(ctx, resumed); err == nil || !strings.Contains(err.Error(), "uncertain bulk import") {
					t.Fatal("ambiguous import was replayed", err)
				}
				// Older controllers see verified+empty-proofs and stop on verify.
				resumed.Kind = ""
				if err := m.execute(ctx, resumed); err == nil {
					t.Fatal("old-controller representation bypassed fence")
				}
			case "verify":
				if len(resumed.Proofs) != 1 {
					t.Fatal("import response not durable before verification")
				}
				if err := m.execute(ctx, resumed); err != nil {
					t.Fatal("receipt-only resume failed", err)
				}
			case "none":
				if resumed.Phase != "done" {
					t.Fatal("not complete")
				}
			}
			if imports.Load() != 1 {
				t.Fatal("name imported twice", imports.Load())
			}
		})
	}
}
