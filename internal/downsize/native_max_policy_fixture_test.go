package downsize

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A small immutable plan/approval binding shared by the original MAX tests.
// It creates no donor-floor or other superseded admission policy.
func nativeMaxPolicyFixture(t *testing.T) (*mover, struct{ ApprovalSHA256 string }) {
	t.Helper()
	root := t.TempDir()
	p := &manifest{PlanID: strings.Repeat("a", 64), root: root,
		Spec: specification{Cluster: "pool-f", Old: topology{Nodes: []member{{ID: "keep", Endpoint: "http://keep:4242"}, {ID: "drop", Endpoint: "http://drop:4242"}}},
			New: topology{Nodes: []member{{ID: "keep", Endpoint: "http://keep:4242"}}}},
		Streams: []route{{sourceURL: "http://drop:4242", targetURL: "http://keep:4242"}}}
	raw := []byte(`{"immutable":"original plan"}`)
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	approval, _ := json.Marshal(map[string]any{"approved_plan_id": p.PlanID, "approved_plan_sha256": hex.EncodeToString(sum[:]), "phase": "copy-only", "delete": false})
	if err := os.WriteFile(filepath.Join(root, "WRITE-OK"), approval, 0600); err != nil {
		t.Fatal(err)
	}
	sum = sha256.Sum256(approval)
	j, err := openJournal(filepath.Join(root, "same-state.jsonl"), "same-plan-and-flags")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.close() })
	m := &mover{planData: p, journal: j, tuner: newTestTuner([]string{"http://drop:4242", "http://keep:4242"}, capacity{8, 32, 1 << 20, 100}, true, 0, 2)}
	return m, struct{ ApprovalSHA256 string }{hex.EncodeToString(sum[:])}
}
