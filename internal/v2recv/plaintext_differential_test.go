package v2recv_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"github.com/klauspost/compress/zstd"
)

type parsedPoint struct {
	name  string
	point points.Point
}

// retainingOwner keeps the borrowed names without cloning them, so later batch
// text reuse would show up as corrupted names.
type retainingOwner struct {
	mu  sync.Mutex
	got []parsedPoint
}

func (o *retainingOwner) Append(names []string, _ []uint64, data []points.Point, _ []uint64, _ int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := range names {
		o.got = append(o.got, parsedPoint{names[i], data[i]})
	}
	return nil
}

type retainingByteOwner struct{ *retainingOwner }

func (o retainingByteOwner) AppendBytes(text []byte, ends []int, hashes []uint64, data []points.Point, sequence []uint64, clock int64) error {
	names := make([]string, len(ends))
	start := 0
	for i, end := range ends {
		names[i] = string(text[start:end])
		start = end
	}
	return o.Append(names, hashes, data, sequence, clock)
}

func (o *retainingOwner) Seal() error  { return nil }
func (o *retainingOwner) Drain() error { return nil }

// mixedPlaintext has timestamps seed*1e6+line, short and long names, consecutive repeats, repeats across
// batches, CRLF and padded lines, and malformed lines.
func mixedPlaintext(seed int64, lines int) []byte {
	r := rand.New(rand.NewSource(seed))
	names := make([]string, 97)
	for i := range names {
		names[i] = fmt.Sprintf("app.%d.%s.m%d", i%5, strings.Repeat("x", r.Intn(200)), i)
	}
	var b bytes.Buffer
	name := names[0]
	for i := 0; i < lines; i++ {
		if r.Intn(3) != 0 {
			name = names[r.Intn(len(names))]
		}
		switch r.Intn(23) {
		case 0:
			b.WriteString("bad line\n")
		case 1:
			fmt.Fprintf(&b, "  %s %d.5 %d \r\n", name, i, seed*1e6+int64(i))
		case 2:
			b.WriteString("\n")
		default:
			fmt.Fprintf(&b, "%s %.17g %d\n", name, r.NormFloat64()*1e6, seed*1e6+int64(i))
		}
	}
	return b.Bytes()
}

// referencePlaintext routes each line as the per-line string receiver did.
func referencePlaintext(t *testing.T, input []byte, owners int) [][]parsedPoint {
	t.Helper()
	want := make([][]parsedPoint, owners)
	for _, line := range bytes.SplitAfter(input, []byte("\n")) {
		if len(line) <= 1 {
			continue
		}
		name, value, ts, err := parse.PlainLine(line)
		if err != nil {
			continue
		}
		n := string(name)
		o := v2recv.OwnerFor(n, owners)
		want[o] = append(want[o], parsedPoint{n, points.Point{Value: value, Timestamp: ts}})
	}
	return want
}

func TestPlaintextBatchTextDifferential(t *testing.T) {
	for _, tc := range []struct {
		batch, owners, buffer int
		zstd, byteNames       bool
	}{{7, 5, 0, false, false}, {1024, 3, 0, false, false}, {64, 4, 0, true, false}, {1, 1, 0, false, false}, {7, 5, 0, false, true}, {1024, 3, 0, false, true}, {64, 4, 0, true, true}, {1, 1, 0, false, true},
		{1024, 5, 600, false, false}, {1024, 5, 600, false, true}, {64, 4, 1, true, true}} {
		t.Run(fmt.Sprintf("batch%d-owners%d-buffer%d-zstd%v-bytes%v", tc.batch, tc.owners, tc.buffer, tc.zstd, tc.byteNames), func(t *testing.T) {
			conns := [][]byte{mixedPlaintext(1, 5000), mixedPlaintext(2, 3000), mixedPlaintext(3, 1)}
			owners := make([]v2recv.Owner, tc.owners)
			recorded := make([]*retainingOwner, tc.owners)
			for i := range owners {
				recorded[i] = &retainingOwner{}
				owners[i] = recorded[i]
				if tc.byteNames {
					owners[i] = retainingByteOwner{recorded[i]}
				}
			}
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			s, err := v2recv.Start(l, owners, v2recv.Config{Logger: quiet(), BatchSize: tc.batch, BufferBytes: tc.buffer})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for _, payload := range conns {
				if tc.zstd {
					e, _ := zstd.NewWriter(nil)
					payload = e.EncodeAll(payload, nil)
					e.Close()
				}
				c, err := net.Dial("tcp", l.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer c.Close()
					for len(payload) > 0 { // odd write sizes split lines across reads
						n := min(len(payload), 1+len(payload)%4093)
						if _, err := c.Write(payload[:n]); err != nil {
							t.Error(err)
							return
						}
						payload = payload[n:]
					}
				}()
			}
			wg.Wait()
			// Wait closes the listener; a dialed but unaccepted connection would be dropped.
			for deadline := time.Now().Add(10 * time.Second); s.Stats().Connections < uint64(len(conns)); time.Sleep(time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("accepted %d of %d connections", s.Stats().Connections, len(conns))
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err = s.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			total := 0
			for c, payload := range conns {
				want := referencePlaintext(t, payload, tc.owners)
				for i := range want {
					total += len(want[i])
					var got []parsedPoint // this connection's points, in owner order
					for _, p := range recorded[i].got {
						if p.point.Timestamp/1e6 == int64(c+1) {
							got = append(got, p)
						}
					}
					if len(got) != len(want[i]) {
						t.Fatalf("conn %d owner %d: got %d points, want %d", c, i, len(got), len(want[i]))
					}
					for j := range got {
						if got[j] != want[i][j] {
							t.Fatalf("conn %d owner %d point %d: got %q %v, want %q %v", c, i, j, got[j].name, got[j].point, want[i][j].name, want[i][j].point)
						}
					}
				}
			}
			if st := s.Stats(); st.Accepted != uint64(total) || st.ParseErrors == 0 {
				t.Fatalf("stats %+v, want %d accepted and parse errors", st, total)
			}
		})
	}
}
