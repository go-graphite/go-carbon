package carbonserver

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestPendingMetricStorableMatchesPath(t *testing.T) {
	segment := strings.Repeat("x", 255)
	names := []string{
		"", "a.value", "空间.値", ".a", "a.", "a..b", "a/b", "a\\b", "C:a", "/a/../b",
		segment + ".value", segment + "x.value", segment[:251], segment[:252],
		strings.Repeat("a.", 2100) + "value", strings.Repeat("long", 2000),
		strings.Repeat("long", 2000) + ";tag=value",
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
