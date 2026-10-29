package carbonserver

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func TestPendingMetricStorableMatchesPath(t *testing.T) {
	t.Parallel()
	segment := strings.Repeat("x", 255)
	names := []string{
		"", "a.value", "空间.値", ".a", "a.", "a..b", "a/b", "a\\b", "C:a", "/a/../b",
		segment + ".value", segment + "x.value", segment[:251], segment[:252],
		strings.Repeat("a.", 2100) + "value", strings.Repeat("long", 2000),
		strings.Repeat("long", 2000) + ";tag=value",
		"namespace:" + strings.Repeat("long", 2000), "namespace\\" + strings.Repeat("long", 2000),
	}
	random := rand.New(rand.NewSource(23))
	for range 1000 {
		var name strings.Builder
		for j := range random.Intn(25) + 1 {
			if j != 0 {
				name.WriteByte('.')
			}
			name.WriteString(strings.Repeat("x", random.Intn(260)+1))
		}
		names = append(names, name.String())
	}
	l := NewCarbonserverListener(nil)
	for _, root := range []string{"", "/", "/var/lib/carbon/whisper", "/" + segment + "x", "/" + strings.Repeat("a/", 1900)} {
		l.SetWhisperData(root)
		for _, name := range names {
			_, want := l.storableMetricPath(name)
			if got := l.pendingMetricStorable(name); got != want {
				t.Fatalf("root %d bytes, metric %d bytes: got %t, want %t", len(root), len(name), got, want)
			}
			if got := l.PendingMetricStorableBytes([]byte(name)); got != want {
				t.Fatalf("borrowed name: root %d bytes, metric %d bytes: got %t, want %t", len(root), len(name), got, want)
			}
		}
	}
}

func FuzzPendingMetricStorable(f *testing.F) {
	f.Add("/root", "a.value")
	f.Add("/root", strings.Repeat("long", 2000))
	f.Add("/root", "a..b;tag=value")
	f.Fuzz(func(t *testing.T, root, name string) {
		l := &CarbonserverListener{whisperData: root}
		_, want := l.storableMetricPath(name)
		if got := l.pendingMetricStorable(name); got != want {
			t.Fatalf("root %q, metric %q: got %t, want %t", root, name, got, want)
		}
		if got := l.PendingMetricStorableBytes([]byte(name)); got != want {
			t.Fatalf("borrowed root %q, metric %q: got %t, want %t", root, name, got, want)
		}
	})
}

func BenchmarkInsertPendingMetric(b *testing.B) {
	for _, size := range []int{80, 8000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			l := NewCarbonserverListener(nil)
			l.SetWhisperData("/var/lib/carbon/whisper")
			u := &fileListUpdate{listener: l, trieIdx: newTrie(".wsp", 0, nil)}
			name := "application." + strings.Repeat("x", size) + ".value"
			b.ReportAllocs()
			for b.Loop() {
				if err := u.insertPendingMetric(name); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRejectPendingMetricWithUnixLiteral(b *testing.B) {
	for _, separator := range []string{":", "\\"} {
		b.Run(separator, func(b *testing.B) {
			l := NewCarbonserverListener(nil)
			l.SetWhisperData("/var/lib/carbon/whisper")
			u := &fileListUpdate{listener: l, trieIdx: newTrie(".wsp", 0, nil)}
			name := "application" + separator + strings.Repeat("x", 8000) + ".value"
			b.ReportAllocs()
			for b.Loop() {
				if err := u.insertPendingMetric(name); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPrepareRejectedPendingNames(b *testing.B) {
	builder := recovery.NewBuilder(func(string) bool { return false })
	var source []byte
	suffix := strings.Repeat("x", 8000)
	for i := range 10000 {
		p := points.OnePoint(fmt.Sprintf("namespace:metric%05d.%s", i, suffix), 1, 1)
		raw := p.AppendBinary(nil)
		source = append(source, raw...)
		if err := builder.Add(0, p, len(raw)); err != nil {
			b.Fatal(err)
		}
	}
	var raw bytes.Buffer
	if err := builder.Write(&raw); err != nil {
		b.Fatal(err)
	}
	index, err := recovery.Open(raw.Bytes(), source, nil)
	if err != nil {
		b.Fatal(err)
	}
	l := NewCarbonserverListener(nil)
	l.SetWhisperData("/var/lib/carbon/whisper")
	b.Run("owned", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := index.NewNames(func(name string) error {
				if l.pendingMetricStorable(name) {
					b.Fatal("unstorable name admitted")
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("borrowed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := index.VisitNewNames(func(name []byte) error {
				if l.PendingMetricStorableBytes(name) {
					b.Fatal("unstorable name admitted")
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
