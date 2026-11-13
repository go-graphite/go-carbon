package chunkstore_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A normal-size transfer commits as one batch instead of ingesting one SST
// per time bucket (Pebble ingests one SST set at a time). Both paths must
// publish bit-identical points, keep the target's own points on a merge, and
// a transfer above the batch limit must still ingest SSTs.
func TestTransferBatchMatchesIngest(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{{"exact", false, false}, {"v6", true, false}, {"v7", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			source := transferStore(t, mode.continuation, mode.buckets)
			names := make([]string, 6)
			for i := range names {
				names[i] = fmt.Sprintf("transfer.batch.n%02d", i)
				var history []store.Point
				for ts := int64(940000 + i*70); ts < 999900; ts += 300 {
					history = append(history, store.Point{Timestamp: ts, Value: float64(ts) / 7})
				}
				putTransferPoints(t, source, names[i], history)
			}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			targets := map[string]*store.Store{}
			for _, limit := range []struct {
				name  string
				bytes int
			}{{"batch", 0}, {"ingest", -1}, {"oversize", 1}} {
				target := transferStore(t, mode.continuation, mode.buckets)
				target.SetTransferBatchBytesForTest(limit.bytes)
				// Half the names are live on the target: the merge keeps its points.
				for _, name := range names[:3] {
					putTransferPoints(t, target, name, []store.Point{{Timestamp: 999880, Value: 3}, {Timestamp: 999910, Value: 4}})
				}
				proofs, err := target.ImportTransfer(ctx, records)
				if err != nil {
					t.Fatal(err)
				}
				if err := target.VerifyTransfer(ctx, proofs); err != nil {
					t.Fatal(err)
				}
				st := target.Stats()
				if limit.name == "batch" {
					if st.TransferBatchCommits != 1 || st.TransferBatchEntries != uint64(len(names)) || st.TransferBatchBytes == 0 || st.IngestTables != 0 {
						t.Fatalf("batch path: commits=%d entries=%d bytes=%d ingest tables=%d", st.TransferBatchCommits, st.TransferBatchEntries, st.TransferBatchBytes, st.IngestTables)
					}
				} else if st.TransferBatchCommits != 0 || st.IngestTables == 0 || st.IngestEntries != uint64(len(names)) {
					t.Fatalf("%s path: batch commits=%d ingest tables=%d entries=%d", limit.name, st.TransferBatchCommits, st.IngestTables, st.IngestEntries)
				}
				for i, name := range names {
					if proofs[i].Merged != (i < 3) {
						t.Fatalf("%s %s merged=%v", limit.name, name, proofs[i].Merged)
					}
					got, err := target.Snapshot(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					if i < 3 {
						found := map[int64]float64{}
						for _, p := range got.Archives[0].Points {
							found[p.Timestamp] = p.Value
						}
						if found[999880] != 3 || found[999910] != 4 {
							t.Fatalf("%s %s lost target points", limit.name, name)
						}
					}
				}
				targets[limit.name] = target
			}
			for _, name := range names {
				want, err := targets["ingest"].Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				for _, other := range []string{"batch", "oversize"} {
					got, err := targets[other].Snapshot(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got.Archives, want.Archives) {
						t.Fatalf("%s: %s snapshot differs from SST ingestion", name, other)
					}
				}
			}
		})
	}
}
