package carbon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestV2IDKeysDefault(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"omitted", "[v2]\nenabled = true\n", true},
		{"false", "[v2]\nid-keys = false\n", false},
		{"true", "[v2]\nid-keys = true\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "go-carbon.conf")
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.V2.IDKeys != tc.want {
				t.Fatalf("id-keys = %v, want %v", cfg.V2.IDKeys, tc.want)
			}
		})
	}
}
