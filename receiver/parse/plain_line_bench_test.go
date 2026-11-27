package parse

import (
	"strings"
	"syscall"
	"testing"
)

func BenchmarkPlainLineTrim(b *testing.B) {
	for _, ending := range []string{"", "\n", "\r\n", " \n"} {
		b.Run("ending="+strings.ReplaceAll(strings.ReplaceAll(ending, "\r", "CR"), "\n", "LF"), func(b *testing.B) {
			line := []byte(strings.Repeat("metric.", 36) + "request 42.125 1794800000" + ending)
			b.ReportAllocs()
			for range b.N {
				if _, _, _, err := PlainLine(line); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// CPU time excludes time descheduled by other workers on a shared benchmark host.
func BenchmarkPlainLineTrimCPU(b *testing.B) {
	line := []byte(strings.Repeat("metric.", 36) + "request 42.125 1794800000\r\n")
	for _, impl := range []struct {
		name  string
		parse func([]byte) ([]byte, float64, int64, error)
	}{
		{"reference", referencePlainLine}, {"direct", PlainLine},
	} {
		b.Run(impl.name, func(b *testing.B) {
			var before, after syscall.Rusage
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for range b.N {
				if _, _, _, err := impl.parse(line); err != nil {
					b.Fatal(err)
				}
			}
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
				b.Fatal(err)
			}
			cpu := after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano()
			b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
		})
	}
}
