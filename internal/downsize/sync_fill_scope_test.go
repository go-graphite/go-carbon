package downsize

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestSyncFillCountsSourceNamesInsteadOfArchiveHoles(t *testing.T) {
	f := newFakeSync()
	o := convergeOptions(t)
	o.fullHistory = true
	for _, host := range []string{syncT, syncD1, syncP} {
		f.put(host, "broken", syncTestSchema)
	}
	delete(f.store(syncT).points["broken"][60], syncTestNow-86400)
	delete(f.store(syncT).points["broken"][3600], syncTestNow-86400)
	out, err := os.CreateTemp(t.TempDir(), "events-")
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	outputMu.Lock()
	os.Stdout = out
	outputMu.Unlock()
	defer func() { outputMu.Lock(); os.Stdout = original; outputMu.Unlock(); out.Close() }()
	err = runSync(context.Background(), o, f)
	outputMu.Lock()
	os.Stdout = original
	outputMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != 2 {
		t.Fatalf("source-name transfers: %v", f.copies)
	}
	data, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var event struct {
			Event                  string `json:"event"`
			Round, Holes, Fillable int
		}
		if err = json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if event.Event == "sync_round" && event.Round == 1 {
			checked = true
			if event.Holes != 4 || event.Fillable != 2 {
				t.Fatalf("two archives times two donors: holes=%d source-names=%d", event.Holes, event.Fillable)
			}
		}
	}
	if !checked {
		t.Fatal("missing first-round selection")
	}
	for _, step := range []int64{60, 3600} {
		if _, ok := f.store(syncT).points["broken"][step][syncTestNow-86400]; !ok {
			t.Fatal("repair lost", step)
		}
	}
}
