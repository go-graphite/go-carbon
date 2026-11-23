package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A write that lands after the digest batch opened its view must not appear
// in the returned version: that version fences deletion of the source.
func TestSumDigestVersionMatchesDigestedPoints(t *testing.T) {
	_, db := testService(t)
	ctx := context.Background()
	const name = "sync.race"
	config := chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 10, Count: 12}}, AggregationMethod: chunkstore.Sum}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateManyForArchive(ctx, name, []chunkstore.Point{{Timestamp: 9950, Value: 5}}, 120); err != nil {
		t.Fatal(err)
	}
	before, err := db.Metadata(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	err = db.DigestBatch(ctx, []string{name}, func(batch context.Context, _ int) error {
		if err := db.UpdateManyForArchive(ctx, name, []chunkstore.Point{{Timestamp: 9990, Value: 6}}, 120); err != nil {
			return err
		}
		archives, version, err := db.SumDigestVersion(batch, name, 0, 0)
		if err != nil {
			return err
		}
		if version.Revision != before.Revision || archives[0].Count != 1 {
			t.Errorf("version revision %d with %d points, want revision %d with 1 point", version.Revision, archives[0].Count, before.Revision)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if after, _ := db.Metadata(ctx, name); after.Revision == before.Revision {
		t.Fatal("the write did not advance the revision; the test proves nothing")
	}
}

func TestSyncPullsOnlyDifferingNames(t *testing.T) {
	source, src := testService(t)
	destination, dst := testServiceWithSyncInterval(t, time.Hour)
	ctx := context.Background()
	create := func(db *chunkstore.Store, name string, points ...chunkstore.Point) {
		t.Helper()
		config := chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 10, Count: 12}}, AggregationMethod: chunkstore.Sum}
		if _, err := db.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateManyForArchive(ctx, name, points, 120); err != nil {
			t.Fatal(err)
		}
	}
	// same: identical on both. absent: only on source. overlap: target holds
	// 9980 with its own value and lacks 9990. nowhere: on neither.
	create(src, "sync.same", chunkstore.Point{Timestamp: 9990, Value: 1})
	create(dst, "sync.same", chunkstore.Point{Timestamp: 9990, Value: 1})
	create(src, "sync.absent", chunkstore.Point{Timestamp: 9990, Value: 2})
	create(src, "sync.overlap", chunkstore.Point{Timestamp: 9980, Value: 3}, chunkstore.Point{Timestamp: 9990, Value: 4})
	create(dst, "sync.overlap", chunkstore.Point{Timestamp: 9980, Value: 30})

	secret := []byte("sync-secret")
	source.secret, destination.secret = secret, secret
	exports := 0
	handler := source.Handler()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/export" {
			exports++
		}
		handler.ServeHTTP(w, r)
	}))
	defer remote.Close()
	root := token(t, secret, []string{"*"}, []string{"*"})
	var window [2]int64 // zero: full history
	recoverCopies := false
	sync := func(names ...string) (int, SyncReply) {
		t.Helper()
		data, err := json.Marshal(SyncRequest{Source: remote.URL, Names: names, From: window[0], Until: window[1], Recover: recoverCopies})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/transfer/sync", bytes.NewReader(data))
		req.Header.Set(authHeader, root)
		response := httptest.NewRecorder()
		destination.Handler().ServeHTTP(response, req)
		var reply SyncReply
		if response.Code == 200 {
			if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
		} else {
			t.Log(response.Body.String())
		}
		return response.Code, reply
	}

	names := []string{"sync.same", "sync.absent", "sync.overlap", "sync.nowhere"}
	if code, _ := sync(names...); code != 403 {
		t.Fatalf("sync from an unlisted source: %d", code)
	}
	destination.SetTransferSources([]string{"127.0.0.1"})

	// A source that is this daemon under another name must be refused, not
	// compared with itself and reported equal.
	self := httptest.NewServer(destination.Handler())
	data, _ := json.Marshal(SyncRequest{Source: self.URL, Names: []string{"sync.same"}})
	req := httptest.NewRequest(http.MethodPost, "/v1/transfer/sync", bytes.NewReader(data))
	req.Header.Set(authHeader, root)
	response := httptest.NewRecorder()
	destination.Handler().ServeHTTP(response, req)
	self.Close()
	if response.Code != http.StatusMisdirectedRequest {
		t.Fatalf("self-transfer: %d %s", response.Code, response.Body.String())
	}

	// A bounded comparison proves nothing about points outside it: the stores
	// match inside [9940, 9960), but the source also holds 9990. The equal
	// row must not carry a version that would delete the whole source metric.
	create(src, "sync.tail", chunkstore.Point{Timestamp: 9950, Value: 5}, chunkstore.Point{Timestamp: 9990, Value: 6})
	create(dst, "sync.tail", chunkstore.Point{Timestamp: 9950, Value: 5})
	window = [2]int64{9940, 9960}
	code, reply := sync("sync.tail")
	if code != 200 || reply.Rows[0].Status != SyncEqual || reply.Rows[0].Source != nil {
		t.Fatalf("bounded equal: code %d row %+v, want equal without a version", code, reply.Rows)
	}
	window = [2]int64{}
	code, reply = sync("sync.tail")
	// A merge can drop a source point into an occupied slot, so it never
	// authorizes a delete either.
	if code != 200 || reply.Rows[0].Status != SyncMerged || reply.Rows[0].Source != nil {
		t.Fatalf("full-history sync of the same name: code %d row %+v, want merged without a version", code, reply.Rows)
	}

	// Equal points over a shorter target retention are not the same metric:
	// the comparison must fall through to the import, which rejects it.
	create(src, "sync.policy", chunkstore.Point{Timestamp: 9990, Value: 7})
	short := chunkstore.MetricConfig{Name: "sync.policy", Retentions: []chunkstore.Retention{{Step: 10, Count: 6}}, AggregationMethod: chunkstore.Sum}
	if _, err := dst.Create(ctx, short); err != nil {
		t.Fatal(err)
	}
	if err := dst.UpdateManyForArchive(ctx, short.Name, []chunkstore.Point{{Timestamp: 9990, Value: 7}}, 60); err != nil {
		t.Fatal(err)
	}
	if code, _ := sync("sync.policy"); code != http.StatusConflict {
		t.Fatalf("retention mismatch: %d, want 409", code)
	}
	exports = 0

	code, reply = sync(names...)
	if code != 200 {
		t.Fatalf("sync: %d", code)
	}
	want := []string{SyncEqual, SyncCopied, SyncMerged, SyncAbsentOnSource}
	for i, row := range reply.Rows {
		if row.Name != names[i] || row.Status != want[i] {
			t.Errorf("row %d = %+v, want %s %s", i, row, names[i], want[i])
		}
		// Only a verbatim copy of an absent name authorizes a delete.
		if (row.Source != nil) != (want[i] == SyncCopied) {
			t.Errorf("row %d source version = %v", i, row.Source)
		}
	}
	fence := reply.Rows[1].Source
	if exports != 1 || reply.Bytes == 0 {
		t.Fatalf("exports = %d, bytes = %d; want one export of the two differing names", exports, reply.Bytes)
	}

	// Target wins at 9980 and gains the source's 9990.
	series, err := dst.Fetch(ctx, "sync.overlap", 9970, 10000)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int]float64{}
	for i, v := range series.Values {
		if v == v {
			got[series.FromTime+i*series.Step] = v
		}
	}
	if got[9980] != 30 || got[9990] != 4 {
		t.Fatalf("merged points = %v, want 9980:30 9990:4", got)
	}

	// A second run finds the copied name equal and exports only the name
	// whose target still differs from the source by design.
	exports = 0
	code, reply = sync("sync.same", "sync.absent")
	if code != 200 || exports != 0 || reply.Rows[0].Status != SyncEqual || reply.Rows[1].Status != SyncEqual ||
		reply.Rows[0].Source != nil || reply.Rows[1].Source != nil {
		t.Fatalf("rerun: code %d exports %d rows %+v, want equal without versions", code, exports, reply.Rows)
	}

	// A retry after a lost reply asks to recover: the durable import receipt
	// proves sync.absent is a verbatim copy of the unchanged source export,
	// so its deletion version comes back. sync.same was never copied and
	// sync.overlap was merged: neither is recoverable.
	recoverCopies = true
	code, reply = sync("sync.same", "sync.absent", "sync.overlap")
	recoverCopies = false
	if code != 200 || reply.Rows[0].Status != SyncEqual || reply.Rows[0].Source != nil ||
		reply.Rows[1].Status != SyncCopied || reply.Rows[1].Source == nil || *reply.Rows[1].Source != *fence ||
		reply.Rows[2].Source != nil {
		t.Fatalf("recover: code %d rows %+v, want only sync.absent copied with version %+v", code, reply.Rows, *fence)
	}

	// A later write to the target copy keeps the receipt but can displace a
	// copied point, so the copy is no longer recoverable. Rewriting an
	// existing point keeps the digests equal and only advances the revision.
	create(src, "sync.rewritten", chunkstore.Point{Timestamp: 9990, Value: 8})
	if _, reply = sync("sync.rewritten"); reply.Rows[0].Status != SyncCopied {
		t.Fatalf("copy: %+v", reply.Rows)
	}
	if err := dst.UpdateManyForArchive(ctx, "sync.rewritten", []chunkstore.Point{{Timestamp: 9990, Value: 8}}, 120); err != nil {
		t.Fatal(err)
	}
	recoverCopies = true
	code, reply = sync("sync.rewritten")
	recoverCopies = false
	if code != 200 || reply.Rows[0].Source != nil {
		t.Fatalf("recover after a target write: code %d rows %+v, want no version", code, reply.Rows)
	}

	// The version of the copied row fences deletion on the source.
	data, err = json.Marshal([]chunkstore.TransferVersion{*fence})
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/transfer/delete", bytes.NewReader(data))
	req.Header.Set(authHeader, root)
	result := httptest.NewRecorder()
	source.Handler().ServeHTTP(result, req)
	if result.Code != 200 {
		t.Fatalf("fenced delete: %d %s", result.Code, result.Body.String())
	}
	if _, err := src.Metadata(ctx, "sync.absent"); err == nil {
		t.Fatal("source copy survived fenced delete")
	}
}
