package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func digestInputFixture(t *testing.T, host, body string) digestInput {
	t.Helper()
	var b bytes.Buffer
	z := gzip.NewWriter(&b)
	if _, e := z.Write([]byte(body)); e != nil {
		t.Fatal(e)
	}
	if e := z.Close(); e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(t.TempDir(), host)
	if e := os.WriteFile(p, b.Bytes(), 0600); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(b.Bytes())
	return digestInput{Host: host, Path: p, Bytes: int64(b.Len()), SHA256: hex.EncodeToString(sum[:])}
}

func TestDigestJoinExplicitAbsenceIsDrilledAndCountsAreVerified(t *testing.T) {
	target := digestInputFixture(t, "target", "[\"a\",null]\n")
	target.Names, target.AbsentNames, target.RequireCounts = 1, 1, true
	donor := digestInputFixture(t, "donor", "[\"a\",{\"archives\":[[60,20]],\"buckets\":[[0,1,1,\"finite\",86400,86400]]}]\n")
	donor.Names, donor.RequireCounts = 1, true
	var output bytes.Buffer
	counts, err := joinDigests(digestJoinSpec{Survivor: target, Donors: []digestInput{donor}}, &output)
	if err != nil || counts.Drilled != 1 || counts.Equal != 0 || counts.UnavailableSurvivor != 1 {
		t.Fatal(counts, err)
	}
	if !bytes.Contains(output.Bytes(), []byte(`"survivor_native_present":false`)) {
		t.Fatal(output.String())
	}
	for _, wrong := range []digestInput{
		{Host: target.Host, Path: target.Path, Bytes: target.Bytes, SHA256: target.SHA256, Names: 2, AbsentNames: 1, RequireCounts: true},
		{Host: target.Host, Path: target.Path, Bytes: target.Bytes, SHA256: target.SHA256, Names: 1, AbsentNames: 0, RequireCounts: true},
	} {
		if _, err := joinDigests(digestJoinSpec{Survivor: wrong, Donors: []digestInput{donor}}, &bytes.Buffer{}); err == nil {
			t.Fatal("incorrect full-stream name/absence count accepted")
		}
	}
}

func TestDigestJoinArchiveMappingExtrasAmbiguityAndIntegrity(t *testing.T) {
	target := digestInputFixture(t, "target", `["a",{"archives":[[300,10],[60,20]],"buckets":[[1,1,2,"same",1,2],[0,2,3,"extra",3,5]]}]
["extra",{"archives":[],"buckets":[]}]
`)
	donor := digestInputFixture(t, "donor", `["a",{"archives":[[60,20],[300,10]],"buckets":[[0,1,2,"same",1,2],[1,2,1,"different",3,3],[1,3,1,"absent",6,6]]}]
["b",null]
`)
	var out bytes.Buffer
	counts, e := joinDigests(digestJoinSpec{Survivor: target, Donors: []digestInput{donor}}, &out)
	if e != nil {
		t.Fatal(e)
	}
	if counts.Equal != 1 || counts.Drilled != 2 || counts.UnavailableDonor != 1 || counts.DonorNames != 2 {
		t.Fatal(counts)
	}
	var row struct {
		Name, Donor string
		Buckets     []struct{ Day int64 }
	}
	if e = json.Unmarshal(bytes.TrimSpace(out.Bytes()), &row); e != nil {
		t.Fatal(e)
	}
	if row.Name != "a" || row.Donor != "donor" || len(row.Buckets) != 2 || row.Buckets[0].Day != 2 || row.Buckets[1].Day != 3 {
		t.Fatal(row)
	}
	target.SHA256 = "bad"
	if _, e = joinDigests(digestJoinSpec{Survivor: target, Donors: []digestInput{donor}}, &bytes.Buffer{}); e == nil {
		t.Fatal("unverified trailing survivor stream accepted")
	}
}

func TestDigestJoinAmbiguityForcesDrillAndSortedness(t *testing.T) {
	body := "[\"a\",{\"archives\":[[60,20]],\"buckets\":[[0,1,2,\"same\",86400,86401]],\"ambiguous\":[[0,86400,2]]}]\n"
	target := digestInputFixture(t, "target", body)
	donor := digestInputFixture(t, "donor", body)
	c, e := joinDigests(digestJoinSpec{Survivor: target, Donors: []digestInput{donor}}, &bytes.Buffer{})
	if e != nil || c.Drilled != 1 || c.Equal != 0 {
		t.Fatal(c, e)
	}
	donor = digestInputFixture(t, "donor", body+body)
	if _, e = joinDigests(digestJoinSpec{Survivor: target, Donors: []digestInput{donor}}, &bytes.Buffer{}); e == nil {
		t.Fatal("duplicate name accepted")
	}
}

func TestDigestAdditionalWindowsDoNotMutatePrimary(t *testing.T) {
	raw := digestFixture(t, []store.Point{{Timestamp: 1, Value: 1}, {Timestamp: 101, Value: 2}})
	var a, b bytes.Buffer
	if e := digestFrames(bytes.NewReader(raw), &a, 0, 10); e != nil {
		t.Fatal(e)
	}
	if e := digestFramesWindows(bytes.NewReader(raw), &b, 0, 10, []window{{From: 100, Until: 110}, {From: 200, Until: 210}}); e != nil {
		t.Fatal(e)
	}
	var before, after map[string]map[string]any
	if e := json.Unmarshal(a.Bytes(), &before); e != nil {
		t.Fatal(e)
	}
	if e := json.Unmarshal(b.Bytes(), &after); e != nil {
		t.Fatal(e)
	}
	primary, _ := json.Marshal(before["x"]["buckets"])
	same, _ := json.Marshal(after["x"]["buckets"])
	if !bytes.Equal(primary, same) {
		t.Fatal("primary changed", a.String(), b.String())
	}
	windows := after["x"]["windows"].(map[string]any)
	if len(windows) != 1 || windows["0"] == nil || after["x"]["windows_complete"] != true {
		t.Fatal(after)
	}
}

func TestWindowJoinReadsEveryNameAndOnlyRequestedWindows(t *testing.T) {
	body := `["a",{"archives":[[60,100]],"buckets":[[0,1,10,"ignored",1,2]],"windows_complete":true,"windows":{"0":{"archives":[[60,100]],"buckets":[[0,5,1,"same",432001,432001]]},"1":{"archives":[[60,100]],"buckets":[[0,6,2,"different",518401,518402]]}}}]
["b",{"archives":[[60,100]],"windows_complete":true,"windows":{}}]
`
	a := digestInputFixture(t, "a-host", body)
	b := digestInputFixture(t, "b-host", body)
	a.Windows = []window{{From: 432000, Until: 432100}, {From: 518400, Until: 518500}, {From: 600000, Until: 600100}}
	b.Windows = append([]window(nil), a.Windows...)
	var out bytes.Buffer
	spec := windowJoinSpec{Inputs: []digestInput{a, b}, Edges: []windowEdge{{Block: "block-a", Target: 0, Reference: 1, Windows: []int{0}}}}
	counts, err := joinWindowDigests(spec, &out)
	if err != nil || counts["block-a"].Equal != 1 || counts["block-a"].Drilled != 0 || out.Len() != 0 {
		t.Fatal(counts, err, out.String())
	}
	if counts["block-a"].ByWindow[0].Equal != 1 || counts["block-a"].ByWindow[0].DonorNames != 2 {
		t.Fatal("window finite coverage not retained", counts)
	}
	spec.Edges[0].Windows = []int{2}
	counts, err = joinWindowDigests(spec, &out)
	if err != nil || counts["block-a"].Drilled != 0 {
		t.Fatal(counts, err)
	}
	if counts["block-a"].ByWindow[2].Equal != 0 || counts["block-a"].ByWindow[2].DonorNames != 2 {
		t.Fatal("empty window cannot be distinguished from a checked finite bucket", counts)
	}
	spec.Edges[0].Windows = []int{3}
	if _, err = joinWindowDigests(spec, &out); err == nil {
		t.Fatal("unscanned window accepted as empty")
	}
	spec.Edges[0].Windows = []int{0}
	b.Windows[0].Until++
	spec.Inputs[1] = b
	if _, err = joinWindowDigests(spec, &out); err == nil {
		t.Fatal("different replica window accepted")
	}
	b.Windows[0].Until--
	b.SHA256 = "bad"
	spec.Inputs[1] = b
	if _, err = joinWindowDigests(spec, &out); err == nil {
		t.Fatal("unverified EOF accepted")
	}
	bad := digestInputFixture(t, "old-format", "[\"a\",{\"archives\":[[60,100]]}]\n")
	bad.Windows = a.Windows
	spec.Inputs[1] = bad
	if _, err = joinWindowDigests(spec, &out); err == nil {
		t.Fatal("unscanned windows accepted as empty")
	}
}
