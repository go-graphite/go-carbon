package tcp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

func TestStreamDictionaryConfigAtStartup(t *testing.T) {
	e, err := codec.NewEncoder(codec.Options{DictionaryBytes: 4 << 20, PeerDictionaryBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	const rows = 3000
	for i := range rows {
		if err := e.Add([]byte(fmt.Sprintf("example.%s.%d", strings.Repeat("component.", 30), i)), float64(i), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	wire, _ := codec.AppendPreamble(nil, 0)
	wire, err = e.Flush(wire)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{1 << 20, 2 << 20} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			cfg := struct {
				TCP *Options `toml:"tcp"`
			}{NewOptions()}
			_, err := toml.Decode(fmt.Sprintf("[tcp]\nlisten='127.0.0.1:0'\nstream-dictionary-bytes=%d\nstream-dictionary-total-bytes=%d\n", limit, 3<<20), &cfg)
			if err != nil {
				t.Fatal(err)
			}
			var received atomic.Uint32
			r, err := newTCP("tcp", cfg.TCP, func(*points.Points) { received.Add(1) })
			if err != nil {
				t.Fatal(err)
			}
			defer r.Stop()
			if r.streamOptions.DictionaryBytes != limit || r.streamDictionaryBudget != 3<<20 {
				t.Fatal("configured byte limits were not installed")
			}
			conn, err := net.Dial("tcp", r.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write(wire); err != nil {
				t.Fatal(err)
			}
			if limit == 1<<20 {
				var b [1]byte
				if _, err := conn.Read(b[:]); !errors.Is(err, io.EOF) {
					t.Fatalf("rejected dictionary did not close connection: %v", err)
				}
				if received.Load() != 0 {
					t.Fatal("over-budget frame emitted rows")
				}
			} else {
				until := time.Now().Add(5 * time.Second)
				for received.Load() != rows {
					if time.Now().After(until) {
						t.Fatalf("received %d rows, want %d", received.Load(), rows)
					}
					time.Sleep(time.Millisecond)
				}
			}
		})
	}
}
