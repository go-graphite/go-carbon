package persister

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadQuotaTrim(t *testing.T) {
	for _, tt := range []struct {
		name, body   string
		valid        bool
		option, drop string
	}{
		{"legacy", "metrics=3\ndropping-policy=none", true, "", "none"},
		{"enabled", "metrics=3\nover-quota=delete-newest", true, "delete-newest", "new"},
		{"explicit", "metrics=3\nover-quota=delete-newest\ndropping-policy=new", true, "delete-newest", "new"},
		{"disabled", "metrics=3\nover-quota=none", true, "", ""},
		{"unknown", "metrics=3\nover-quota=delete-oldest", false, "", ""},
		{"missing cap", "over-quota=delete-newest", false, "", ""},
		{"zero cap", "metrics=0\nover-quota=delete-newest", false, "", ""},
		{"negative cap", "metrics=-1\nover-quota=delete-newest", false, "", ""},
		{"conflict", "metrics=3\nover-quota=delete-newest\ndropping-policy=none", false, "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "quotas.conf")
			if err := os.WriteFile(path, []byte("[apps.*]\n"+tt.body+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			rules, err := ReadWhisperQuotas(path)
			if !tt.valid {
				if err == nil || !strings.Contains(err.Error(), "over-quota") || !strings.Contains(err.Error(), "apps.*") {
					t.Fatalf("missing actionable parse error: %v", err)
				}
				return
			}
			if err != nil || len(rules) != 1 {
				t.Fatalf("rules=%v error=%v", rules, err)
			}
			if rules[0].OverQuota != tt.option || rules[0].DroppingPolicy != tt.drop {
				t.Fatalf("unexpected rule %+v", rules[0])
			}
		})
	}
}
