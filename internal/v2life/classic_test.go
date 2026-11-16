package v2life

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The classic policy expirer and trie are intentionally private. A Go overlay
// compiles the reused differential fixtures inside their owning packages without
// editing them or exporting test-only APIs. No generated files enter the tree.
func TestClassicDifferential(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for target, fixture := range map[string]string{"carbon/v2life_expiration_test.go": "classic_expiration_test.go", "carbonserver/v2life_quota_test.go": "classic_quota_test.go"} {
		files[filepath.Join(root, target)] = filepath.Join(root, "internal/v2life/testdata", fixture)
	}
	data, err := json.Marshal(map[string]any{"Replace": files})
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(t.TempDir(), "overlay.json")
	if err = os.WriteFile(overlay, data, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=vendor", "-overlay", overlay, "-vet=off", "./carbon", "./carbonserver", "-run", "^TestV2Life", "-count=1", "-v")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("classic differential: %v\n%s", err, output)
	}
	t.Log(string(output))
}
