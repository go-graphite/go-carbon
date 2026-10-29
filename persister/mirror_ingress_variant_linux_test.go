package persister

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// Verify the old-source build overlay before spending time on a native replay.
func TestNativeIngressAppendVariant(t *testing.T) {
	variant := os.Getenv("GRAPHITE_INGRESS_APPEND_VARIANT")
	if variant == "" {
		t.Skip("native before/after build identity check")
	}
	if variant != "before" && variant != "after" {
		t.Fatal("invalid variant")
	}
	w, err := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "source.wsp"), whisper.MustParseRetentionDefs("1s:1h"), whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	now := int(time.Now().Unix())
	input := []*whisper.TimeSeriesPoint{{Time: now - 10, Value: .1}, {Time: now - 10, Value: .2}}
	if got := w.CanMirrorMonotonicBatch(input, int64(now)); got != (variant == "after") {
		t.Fatalf("build overlay ignored or wrong variant: proof=%v variant=%s", got, variant)
	}
}
