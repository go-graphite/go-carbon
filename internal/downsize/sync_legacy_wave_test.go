package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
)

func TestLegacyWaveMembership(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{"format", "enlarged", "replaced", "selected", "unknown", "wrong-source", "wrong-target", "extra-identity", "absent-input"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "missing-batch-r1-s0.names")
			original := []string{"a", "b", "d"}
			identity := map[string]string{"from": syncD1, "to": syncT, "prefix": "", "ring_sha256": "", "names_sha256": fmt.Sprintf("%x", sha256.Sum256(syncNamesBytes(original)))}
			data := "a\nb\nd\nz\n"
			switch variant {
			case "format":
				data = "d\r\nb\r\na\r\n"
			case "replaced":
				data = "z\n"
			case "unknown":
				identity["names_sha256"] = strings.Repeat("0", 64)
			case "wrong-source":
				identity["from"] = syncD2
			case "wrong-target":
				identity["to"] = syncD2
			case "extra-identity":
				identity["other"] = "x"
			}
			if variant != "absent-input" {
				if err := os.WriteFile(path, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			blob, _ := json.Marshal(identity)
			header := copyRow{Kind: "header", Identity: string(blob)}
			receipt := syncRow{Kind: "copied", Phase: "missing-batch", Round: 1, Source: syncD1, First: "a", Last: "d", Names: 3, Copied: 3}
			if variant == "selected" {
				receipt.Selected = original
			}
			missing := []syncMissing{{"a", 1}, {"b", 1}, {"d", 1}, {"z", 1}}
			s := &syncer{o: syncOptions{from: []string{syncD1}, to: syncT}}
			got, err := s.legacyWaveNames(header, path, 0, 1, []syncRow{receipt}, missing)
			reject := variant == "unknown" || variant == "wrong-source" || variant == "wrong-target" || variant == "extra-identity"
			if reject {
				if err == nil {
					t.Fatal("accepted unauthenticated names/identity")
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, original) {
				t.Fatalf("names=%v err=%v", got, err)
			}
			if variant != "absent-input" {
				after, _ := os.ReadFile(path)
				if string(after) != data {
					t.Fatal("legacy input changed")
				}
			}
		})
	}
}

func TestLegacyRecoveredPartialWaveKeepsReceipts(t *testing.T) {
	t.Parallel()
	f, source, target := newFakeCopy(t, []string{"a", "b", "c"})
	dir := t.TempDir()
	journal := filepath.Join(dir, "missing-batch-r1-s0.jsonl")
	input := strings.TrimSuffix(journal, ".jsonl") + ".names"
	original := []string{"a", "b", "c"}
	if err := os.WriteFile(input, syncNamesBytes(original), 0600); err != nil {
		t.Fatal(err)
	}
	identity, err := copyIdentity(copyOptions{froms: []string{source}, to: target, names: input})
	if err != nil {
		t.Fatal(err)
	}
	j, err := openCopyJournal(journal, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.done(source, "a", "b", 2, 20, time.Second); err != nil {
		t.Fatal(err)
	}
	j.close()
	before, _ := os.ReadFile(journal)
	overwritten := []byte("c\nd\ne\n")
	if err := os.WriteFile(input, overwritten, 0600); err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: syncOptions{state: dir}}
	path, err := s.namesFile("missing-batch", 1, 0, original)
	if err != nil || path != journal {
		t.Fatalf("%s %v", path, err)
	}
	recovered, err := syncCopyInput(path, original)
	if err != nil {
		t.Fatal(err)
	}
	if recovered == input {
		t.Fatal("overwrote retained input")
	}
	options := copyOptions{froms: []string{source}, to: target, names: recovered}
	got, err := copyIdentity(options)
	if err != nil || got != identity {
		t.Fatalf("identity changed: %s %v", got, err)
	}
	j, err = openCopyJournal(journal, got)
	if err != nil {
		t.Fatal(err)
	}
	cover := j.cover(source)
	if !cover("a") || !cover("b") || cover("c") {
		t.Fatal("partial receipts changed")
	}
	j.close()
	after, _ := os.ReadFile(journal)
	if string(after) != string(before) {
		t.Fatal("journal changed")
	}
	after, _ = os.ReadFile(input)
	if string(after) != string(overwritten) {
		t.Fatal("overwritten legacy input changed")
	}
	template := copyTestOptions(t, source, target)
	live := newSyncLiveBackend(syncLiveOptions{copyStreams: 1, copyAttempts: 1, hostWait: time.Second, copyTemplate: &template})
	for attempt := 0; attempt < 2; attempt++ {
		res, err := live.Copy(context.Background(), source, target, original, journal)
		if err != nil || res.Copied != 3 || len(res.Failed) != 0 {
			t.Fatalf("resume: %+v %v", res, err)
		}
	}
	if !reflect.DeepEqual(f.imported, map[string]int{"c": 1}) {
		t.Fatalf("repeated or skipped imports: %v", f.imported)
	}
	after, _ = os.ReadFile(journal)
	if !strings.HasPrefix(string(after), string(before)) {
		t.Fatal("old journal receipts changed during copy")
	}
}

// Optional, local-only captures retain private names outside the repository.
// Each subdirectory contains the unmodified child journals/inputs, parent
// receipt excerpts and a source-filtered missing.tsv excerpt from discovery.
func TestLegacyWaveCapturedFixtures(t *testing.T) {
	t.Parallel()
	root := os.Getenv("STORECTL_LEGACY_FIXTURES")
	if root == "" {
		t.Skip("set STORECTL_LEGACY_FIXTURES to local read-only capture directories")
	}
	dirs, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	tested := 0
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		t.Run(dir.Name(), func(t *testing.T) {
			state := filepath.Join(root, dir.Name())
			raw, err := os.ReadFile(filepath.Join(state, "config.json"))
			if err != nil {
				t.Fatal(err)
			}
			var config struct {
				Flags map[string]string `json:"flags"`
			}
			if err := json.Unmarshal(raw, &config); err != nil {
				t.Fatal(err)
			}
			s := &syncer{o: syncOptions{from: strings.Split(config.Flags["source"], ","), to: config.Flags["to"], state: state}}
			missing, err := s.readMissing()
			if err != nil {
				t.Fatal(err)
			}
			raw, err = os.ReadFile(filepath.Join(state, "parent.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			var parents []syncRow
			for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
				var row syncRow
				if err := json.Unmarshal([]byte(line), &row); err != nil {
					t.Fatal(err)
				}
				parents = append(parents, row)
			}
			for _, r := range parents {
				if r.Kind != "copied" {
					continue
				}
				src := -1
				for i, source := range s.o.from {
					if source == r.Source {
						src = i
					}
				}
				if src < 0 {
					t.Fatal("unknown source")
				}
				journal := filepath.Join(state, fmt.Sprintf("missing-batch-r%d-s%d.jsonl", r.Round, src))
				input := strings.TrimSuffix(journal, ".jsonl") + ".names"
				before, err := os.ReadFile(journal)
				if err != nil {
					t.Fatal(err)
				}
				namesBefore, err := os.ReadFile(input)
				if err != nil {
					t.Fatal(err)
				}
				var header copyRow
				if err := json.Unmarshal([]byte(strings.SplitN(string(before), "\n", 2)[0]), &header); err != nil {
					t.Fatal(err)
				}
				names, err := s.legacyWaveNames(header, input, src, r.Round, parents, missing)
				if err != nil {
					t.Fatal(err)
				}
				if len(names) != r.Names || names[0] != r.First || names[len(names)-1] != r.Last {
					t.Fatal("membership differs from original parent receipt")
				}
				failed, err := copyJournalFailures(journal, r.Source, names)
				if err != nil || !reflect.DeepEqual(failed, r.Failed) && !(len(failed) == 0 && len(r.Failed) == 0) {
					t.Fatalf("changed old receipts: got %d failures, want %d: %v", len(failed), len(r.Failed), err)
				}
				after, _ := os.ReadFile(journal)
				namesAfter, _ := os.ReadFile(input)
				if string(before) != string(after) || string(namesBefore) != string(namesAfter) {
					t.Fatal("capture changed")
				}
				tested++
			}
		})
	}
	if tested == 0 {
		t.Fatal("no captured fixtures tested")
	}
	t.Logf("authenticated %d captured legacy waves and retained every child receipt", tested)
}

// Compare with the original string-set scheduling oracle: reservations are
// exact even for overlapping, stale, duplicate or unsorted historical inputs.
func TestMissingWaveReservations(t *testing.T) {
	t.Parallel()
	for _, size := range []int{0, 1, 63, 64, 65, 10000} {
		names := make([]string, size)
		for i := range names {
			names[i] = fmt.Sprintf("name.%08d", i*2)
		}
		waves := []syncRow{{Selected: []string{"absent", "name.00000000", "name.00000001", "zzzz"}}, {}}
		for start := 0; start < size; start += 101 {
			var selected []string
			for i := start; i < min(start+100, size); i += 3 {
				selected = append(selected, names[i])
			}
			waves = append(waves, syncRow{Selected: selected})
		}
		if size > 64 {
			waves = append(waves, syncRow{Selected: []string{names[size-1], names[64], names[0], names[64]}})
		}
		oracle := map[string]bool{}
		for _, wave := range waves {
			for _, n := range wave.Selected {
				oracle[n] = true
			}
		}
		got := reserveMissingWaves(names, waves)
		if len(got)*8 > (size+7)/8+7 {
			t.Fatalf("unbounded reservation storage: %d bytes", len(got)*8)
		}
		for i, n := range names {
			if reserved := got[i/64]&(uint64(1)<<uint(i%64)) != 0; reserved != oracle[n] {
				t.Fatalf("size=%d name=%s got=%v want=%v", size, n, reserved, oracle[n])
			}
		}
	}
}

func BenchmarkMissingWaveReservations(b *testing.B) {
	names := make([]string, 1000000)
	for i := range names {
		names[i] = fmt.Sprintf("metric.%08d.%s", i, strings.Repeat("x", 128))
	}
	var waves []syncRow
	for i := 0; i < len(names); i += 10000 {
		waves = append(waves, syncRow{Selected: names[i:min(i+10000, len(names))]})
	}
	// Historical waves may also contain names absent from today's candidate set.
	candidates := names[100000:900000]
	for _, bitmap := range []bool{false, true} {
		b.Run(fmt.Sprintf("bitmap=%v", bitmap), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if bitmap {
					reserved := reserveMissingWaves(candidates, waves)
					if reserved[0]&1 == 0 {
						b.Fatal("missing reservation")
					}
				} else {
					reserved := map[string]bool{}
					for _, wave := range waves {
						for _, n := range wave.Selected {
							reserved[n] = true
						}
					}
					if !reserved[candidates[0]] {
						b.Fatal("missing reservation")
					}
				}
			}
		})
	}
}

func TestLegacyResumeAbsentSourceRetainsFailures(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{"absent", "pending", "present", "missing-evidence", "missing-policy", "unsupported", "forbidden", "wrong-identity", "complete"} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			var evidenceCalls atomic.Int64
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/expiration/evidence" {
					t.Errorf("unexpected request %s", r.URL.Path)
					http.Error(w, "unexpected", 400)
					return
				}
				evidenceCalls.Add(1)
				if variant == "unsupported" {
					http.Error(w, "unsupported", 501)
					return
				}
				if variant == "forbidden" {
					http.Error(w, "forbidden", 403)
					return
				}
				var req struct {
					Names        []string
					ReceiptsOnly bool
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !req.ReceiptsOnly || !reflect.DeepEqual(req.Names, []string{"d"}) {
					t.Errorf("evidence must cover only unresolved names: %+v %v", req, err)
				}
				evidence := map[string]buckyd.ExpirationEvidence{"d": {Policy: "policy"}}
				switch variant {
				case "pending":
					evidence["d"] = buckyd.ExpirationEvidence{Policy: "policy", Pending: true}
				case "present":
					evidence["d"] = buckyd.ExpirationEvidence{Policy: "policy", Latest: 1}
				case "missing-evidence":
					delete(evidence, "d")
				case "missing-policy":
					evidence["d"] = buckyd.ExpirationEvidence{}
				}
				_ = json.NewEncoder(w).Encode(evidence)
			}))
			defer source.Close()
			dir := t.TempDir()
			path := filepath.Join(dir, "missing-batch-r1-s0.jsonl")
			input := strings.TrimSuffix(path, ".jsonl") + ".names"
			names := []string{"a", "b", "c", "d"}
			if err := os.WriteFile(input, syncNamesBytes(names), 0600); err != nil {
				t.Fatal(err)
			}
			identity, err := copyIdentity(copyOptions{froms: []string{source.URL}, to: syncT, names: input})
			if err != nil {
				t.Fatal(err)
			}
			if variant == "wrong-identity" {
				identity = "different copy"
			}
			j, err := openCopyJournal(path, identity)
			if err != nil {
				t.Fatal(err)
			}
			count, last := 3, "c"
			if variant == "complete" {
				count, last = 4, "d"
			}
			if err := j.done(source.URL, "a", last, count, 100, time.Second); err != nil {
				t.Fatal(err)
			}
			j.close()
			before, _ := os.ReadFile(path)
			b := newSyncLiveBackend(syncLiveOptions{copyStreams: 1, copyAttempts: 1, hostWait: time.Second})
			copyCalls := 0
			b.copy = func(context.Context, copyOptions) error { copyCalls++; return errors.New("ordinary copy attempted") }
			var replayed int64
			ctx := context.WithValue(context.Background(), syncCopyDurableProgressKey{}, func(n int64, replay bool) {
				if replay {
					replayed = n
				}
			})
			result, err := b.Copy(ctx, source.URL, syncT, names, path)
			if variant == "wrong-identity" || variant == "forbidden" {
				if err == nil || copyCalls != 0 {
					t.Fatalf("must fail closed: result=%+v err=%v copies=%d", result, err, copyCalls)
				}
			} else {
				if err != nil || result.Copied != count {
					t.Fatalf("result=%+v err=%v", result, err)
				}
				if variant == "absent" || variant == "complete" {
					if copyCalls != 0 || replayed != int64(count) {
						t.Fatalf("repeated transfer or lost replay: copies=%d replay=%d", copyCalls, replayed)
					}
				} else if copyCalls == 0 {
					t.Fatal("incomplete/pending/present evidence suppressed ordinary copy")
				}
				if variant != "complete" && !reflect.DeepEqual(result.Failed, []string{"d"}) {
					t.Fatalf("absent name must remain failed: %+v", result)
				}
			}
			after, _ := os.ReadFile(path)
			if string(before) != string(after) {
				t.Fatal("durable header or completion receipts changed")
			}
			if (variant == "wrong-identity" || variant == "complete") && evidenceCalls.Load() != 0 {
				t.Fatal("unexpected evidence read")
			}
		})
	}
}
