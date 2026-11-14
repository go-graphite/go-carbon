package buckyd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestTransferDigest(t *testing.T) {
	service, db := testService(t)
	secret := []byte("digest-secret")
	service.secret = secret
	ctx := context.Background()
	// The store clock is 10000: the 10 s archive covers [9880, 10000), the
	// 60 s archive reaches back to 4000.
	retentions := []chunkstore.Retention{{Step: 10, Count: 12}, {Step: 60, Count: 100}}
	base := []chunkstore.Point{{Timestamp: 9900, Value: 1}, {Timestamp: 9910, Value: 2}, {Timestamp: 9920, Value: 3}, {Timestamp: 9930, Value: 4}}
	write := func(name string, points []chunkstore.Point) {
		t.Helper()
		if _, err := db.Create(ctx, chunkstore.MetricConfig{Name: name, Retentions: retentions, AggregationMethod: chunkstore.Average}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(ctx, name, points); err != nil {
			t.Fatal(err)
		}
	}
	write("digest.a", base)
	write("digest.b", append(append([]chunkstore.Point(nil), base...), chunkstore.Point{Timestamp: 9940, Value: math.NaN()}))
	differ := append([]chunkstore.Point(nil), base...)
	differ[2].Value = 3.5
	write("digest.c", differ)
	old := []chunkstore.Point{{Timestamp: 9000, Value: 7}, {Timestamp: 9060, Value: 8}, {Timestamp: 9120, Value: 9}}
	write("digest.old", old)

	root := token(t, secret, []string{"*"}, []string{"*"})
	invoke := func(body any, jwt string) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/transfer/digest", bytes.NewReader(data))
		req.Header.Set(authHeader, jwt)
		response := httptest.NewRecorder()
		service.Handler().ServeHTTP(response, req)
		return response
	}
	digest := func(body any) []DigestResult {
		t.Helper()
		response := invoke(body, root)
		if response.Code != 200 {
			t.Fatalf("digest %d %s", response.Code, response.Body.String())
		}
		var out []DigestResult
		if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	all := digest(DigestRequest{Names: []string{"digest.a", "digest.missing", "digest.b", "digest.c"}, From: 9900, Until: 9950})
	if len(all) != 4 || all[0].Name != "digest.a" || all[1].Name != "digest.missing" || all[2].Name != "digest.b" || all[3].Name != "digest.c" {
		t.Fatalf("order %+v", all)
	}
	if all[1].Found || len(all[1].Archives) != 0 {
		t.Fatalf("unknown name %+v", all[1])
	}
	a, b, c := all[0].Archives, all[2].Archives, all[3].Archives
	if !all[0].Found || len(a) != 1 || a[0].Step != 10 || a[0].Points != 12 || a[0].Count != 4 || len(a[0].Hash) != 16 {
		t.Fatalf("digest.a %+v", all[0])
	}
	if b[0] != a[0] {
		t.Fatalf("NaN must be skipped and equal sets hash equal: %+v vs %+v", b[0], a[0])
	}
	if c[0].Count != a[0].Count || c[0].Hash == a[0].Hash {
		t.Fatalf("one differing point must change the hash: %+v vs %+v", c[0], a[0])
	}
	if again := digest(DigestRequest{Names: []string{"digest.a"}, From: 9900, Until: 9950}); again[0].Archives[0] != a[0] {
		t.Fatal("digest not stable")
	}

	// From is inclusive, Until exclusive.
	window := digest(DigestRequest{Names: []string{"digest.a", "digest.c"}, From: 9910, Until: 9930})
	h := sha256.New()
	var buf [16]byte
	for _, p := range base[1:3] {
		binary.BigEndian.PutUint64(buf[:8], uint64(p.Timestamp))
		binary.BigEndian.PutUint64(buf[8:], math.Float64bits(p.Value))
		h.Write(buf[:])
	}
	want := hex.EncodeToString(h.Sum(nil)[:8])
	if got := window[0].Archives[0]; got.Count != 2 || got.Hash != want {
		t.Fatalf("window %+v want count 2 hash %s", got, want)
	}
	if window[1].Archives[0].Hash == want {
		t.Fatal("window over the differing point must differ")
	}
	empty := digest(DigestRequest{Names: []string{"digest.a"}, From: 9960, Until: 9990})
	nothing := sha256.Sum256(nil)
	if got := empty[0].Archives; len(got) != 1 || got[0].Step != 10 || got[0].Count != 0 || got[0].Hash != hex.EncodeToString(nothing[:8]) {
		t.Fatalf("empty window %+v", got)
	}

	// A window older than the first archive is served by the coarser one,
	// exactly like a render of the same range.
	coarse := digest(DigestRequest{Names: []string{"digest.old", "digest.a"}, From: 9000, Until: 9180})
	if got := coarse[0].Archives; len(got) != 1 || got[0].Step != 60 || got[0].Points != 100 || got[0].Count != 3 {
		t.Fatalf("coarse window %+v", coarse[0])
	}
	if got := coarse[1].Archives; len(got) != 1 || got[0].Step != 60 || got[0].Count != 0 {
		t.Fatalf("coarse window without points %+v", coarse[1])
	}
	// A window wholly outside the retention reads nothing: found, no archive.
	if gone := digest(DigestRequest{Names: []string{"digest.a"}, From: 1, Until: 2}); !gone[0].Found || len(gone[0].Archives) != 0 {
		t.Fatalf("window outside retention %+v", gone[0])
	}

	for _, bad := range []any{
		DigestRequest{From: 9900, Until: 9950},
		DigestRequest{Names: []string{"digest.a", "digest.a"}, From: 9900, Until: 9950},
		DigestRequest{Names: []string{""}, From: 9900, Until: 9950},
		DigestRequest{Names: []string{"digest.a"}},
		DigestRequest{Names: []string{"digest.a"}, Until: 9950},
		DigestRequest{Names: []string{"digest.a"}, From: 9900},
		DigestRequest{Names: []string{"digest.a"}, From: 20, Until: 10},
		DigestRequest{Names: []string{"digest.a"}, From: 20, Until: 20},
		map[string]any{"Names": []string{"digest.a"}, "Extra": 1},
	} {
		if response := invoke(bad, root); response.Code != 400 {
			t.Fatalf("bad request %+v: %d %s", bad, response.Code, response.Body.String())
		}
	}
	if response := invoke(DigestRequest{Names: []string{"digest.a"}, From: 9900, Until: 9950}, token(t, secret, []string{"digest.*"}, []string{"*"})); response.Code != 403 {
		t.Fatal("scoped token accepted", response.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/transfer/digest", nil)
	req.Header.Set(authHeader, root)
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, req)
	if response.Code != 405 {
		t.Fatal("GET accepted", response.Code)
	}
}

func TestTransferSumDigest(t *testing.T) {
	service, db := testService(t)
	ctx := context.Background()
	if _, err := db.Create(ctx, chunkstore.MetricConfig{Name: "sum.a", Retentions: []chunkstore.Retention{{Step: 10, Count: 12}, {Step: 60, Count: 100}}, AggregationMethod: chunkstore.Average}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(ctx, "sum.a", []chunkstore.Point{{Timestamp: 9900, Value: 1}, {Timestamp: 9910, Value: math.Copysign(0, -1)}}); err != nil {
		t.Fatal(err)
	}
	r := DigestRequest{Names: []string{"sum.a", "missing"}, Mode: "sum-v1"}
	if err := r.validate(); err != nil {
		t.Fatal(err)
	}
	got, err := service.digest(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if !got[0].Found || len(got[0].Archives) != 2 || got[1].Found || got[0].Mode != "sum-v1" || got[1].Mode != "sum-v1" {
		t.Fatalf("%+v", got)
	}
	var expected chunkstore.PointSum
	expected.Add(9900, 1)
	expected.Add(9910, math.Copysign(0, -1))
	if got[0].Archives[0].Count != int(expected.Count) || got[0].Archives[0].Hash != fmt.Sprintf("%016x", expected.Sum) {
		t.Fatalf("ordinary sum differs: %+v vs %+v", got[0].Archives[0], expected)
	}
	secret := []byte("sum-digest-secret")
	service.secret = secret
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/transfer/digest", bytes.NewReader(data))
	req.Header.Set(authHeader, token(t, secret, []string{"*"}, []string{"read"}))
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("sum HTTP: %d %s", response.Code, response.Body.String())
	}
	var wire []DigestResult
	if err := json.Unmarshal(response.Body.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire) != len(got) || len(wire[0].Archives) != 2 || wire[0].Archives[0] != got[0].Archives[0] {
		t.Fatalf("sum HTTP mismatch: %+v", wire)
	}
	r.Step = 60
	selected, err := service.digest(ctx, r)
	if err != nil || len(selected[0].Archives) != 1 || selected[0].Archives[0] != got[0].Archives[1] {
		t.Fatalf("selected archive: %+v, %v", selected, err)
	}

	r.Mode = "unknown"
	if r.validate() == nil {
		t.Fatal("unknown mode accepted")
	}
}
