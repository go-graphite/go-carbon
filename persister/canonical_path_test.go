package persister

import (
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
)

func legacyCanonicalMetricPath(root, path, metric string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && filepath.IsLocal(relative) && relative == strings.ReplaceAll(metric, ".", string(filepath.Separator))+".wsp"
}

func TestCanonicalMetricPathMatchesRel(t *testing.T) {
	roots := []string{"/var/lib/carbon/whisper", "/var/lib/carbon/whisper/", "/", "rel/root", "/a/./b", "", ".", "./", "..", "../root", "a/.."}
	metrics := []string{"a", "a.b.c", "a..b", ".a", "a.", ".", "..", "a...b", "a.b.", "x.y_z-1.wsp", "aa.bb.cc.dd.ee"}
	for _, root := range roots {
		for _, metric := range metrics {
			paths := []string{
				filepath.Join(root, strings.ReplaceAll(metric, ".", "/")+".wsp"),
				root + "/" + strings.ReplaceAll(metric, ".", "/") + ".wsp",
				filepath.Join(root, "other", strings.ReplaceAll(metric, ".", "/")+".wsp"),
				filepath.Join(root, strings.ReplaceAll(metric, ".", "/")+".wspx"),
			}
			for _, path := range paths {
				if got, want := canonicalMetricPath(root, path, metric), legacyCanonicalMetricPath(root, path, metric); got != want {
					t.Errorf("root=%q path=%q metric=%q: got %v, legacy %v", root, path, metric, got, want)
				}
			}
		}
	}
}

func TestCanonicalMetricPathRandomizedParity(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	alphabet := []byte("abc./\\;_-\x00\xc3\xa9")
	word := func() string {
		b := make([]byte, rng.Intn(48))
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(b)
	}
	for i := 0; i < 10000; i++ {
		root, metric := word(), word()
		path := filepath.Join(root, strings.ReplaceAll(metric, ".", string(filepath.Separator))+".wsp")
		switch i % 4 {
		case 0:
			path = word()
		case 1:
			path = root + string(filepath.Separator) + strings.ReplaceAll(metric, ".", string(filepath.Separator)) + ".wsp"
		case 2:
			path = filepath.Join(root, "other", strings.ReplaceAll(metric, ".", string(filepath.Separator))+".wsp")
		}
		if got, want := canonicalMetricPath(root, path, metric), legacyCanonicalMetricPath(root, path, metric); got != want {
			t.Fatalf("root=%q path=%q metric=%q: got %v, legacy %v", root, path, metric, got, want)
		}
	}
}

func BenchmarkCanonicalMetricPath(b *testing.B) {
	root, metric := "/var/lib/carbon/whisper", "cluster.app.region.host.metric_name.count"
	path := filepath.Join(root, strings.ReplaceAll(metric, ".", "/")+".wsp")
	for _, tc := range []struct {
		name  string
		check func(string, string, string) bool
	}{{"legacy", legacyCanonicalMetricPath}, {"validated", canonicalMetricPath}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !tc.check(root, path, metric) {
					b.Fatal("rejected")
				}
			}
		})
	}
}
