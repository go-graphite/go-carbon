package main

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func writeData(t *testing.T) string {
	p := filepath.Join(t.TempDir(), "d.txt")
	var b strings.Builder
	for ts := int64(1000); ts < 1000+7200; ts += 60 {
		b.WriteString("a.b 1.5 " + itoa(ts) + "\nc.d 2 " + itoa(ts) + "\n")
	}
	b.WriteString("late.only 3 8140\n")
	if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func TestSummaryWindowsAndPick(t *testing.T) {
	s, err := summarize(writeData(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.paths) != 3 || s.tmin != 1000 || s.tmax != 8140 {
		t.Fatalf("summary %+v", s)
	}
	if !s.lastHour["late.only"] || !s.lastHour["a.b"] || s.lastHour["x"] {
		t.Fatalf("last hour %v", s.lastHour)
	}
	if f, u := s.window("1h"); u-f != 3600 || u != 8141 {
		t.Fatalf("window %d %d", f, u)
	}
	if got := pick(s.paths, 2); len(got) != 2 || got[0] != s.paths[0] {
		t.Fatalf("pick %v", got)
	}
}

func TestSendShardsByPath(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	counts := make(chan int, 4)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 1<<20)
				n, total := 0, 0
				for {
					k, err := c.Read(buf)
					n += strings.Count(string(buf[:k]), "\n")
					total += k
					if err != nil {
						break
					}
				}
				counts <- n
			}()
		}
	}()
	if err := send(writeData(t), l.Addr().String(), 2); err != nil {
		t.Fatal(err)
	}
	sum := <-counts + <-counts
	if sum != 2*120+1 {
		t.Fatalf("lines received %d", sum)
	}
}
