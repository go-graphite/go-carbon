package v2recv_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

func cpu() float64 {
	var u syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
}

// TestMeasureTCP is an opt-in finite, close-inclusive diagnostic. Fixtures are
// encoded before timing; sender socket-copy CPU remains inside process CPU.
// The exact full-core scan runs after timing. No capacity claim is implied.
func TestMeasureTCP(t *testing.T) {
	root := os.Getenv("V2_TCP_ROOT")
	if root == "" {
		t.Skip("set V2_TCP_ROOT to a fresh experiment directory")
	}
	mode := os.Getenv("V2_TCP_MODE")
	if mode == "" {
		mode = "binary"
	}
	workers := 48
	shards := 12
	connections := 48
	rounds := 256
	const width = 512
	if x := os.Getenv("V2_TCP_ROUNDS"); x != "" {
		rounds, _ = strconv.Atoi(x)
	}
	if rounds < 1 {
		t.Fatal("invalid rounds")
	}
	total := uint64(connections * width * rounds)
	payloads := make([][]byte, connections)
	rows := make([][]v2core.Row, connections)
	var wireBytes uint64
	for c := range connections {
		if mode == "l0" {
			rows[c] = make([]v2core.Row, 0, width*rounds)
		}
		var enc *codec.Encoder
		var err error
		if mode == "binary" || mode == "decode" {
			enc, err = codec.NewEncoder(codec.Options{Flags: codec.FlagZstd, MaxRows: 32768})
			if err != nil {
				t.Fatal(err)
			}
			payloads[c], _ = codec.AppendPreamble(nil, codec.FlagZstd)
		}
		for i := 0; i < width*rounds; i++ {
			id := c*width + i%width
			ts := int64(1794700800 + i/width)
			v := float64(id) + .125
			switch mode {
			case "l0":
				rows[c] = append(rows[c], v2core.Row{ID: uint64(id + 1), Time: ts, Bits: math.Float64bits(v), Seq: uint64(i/width + 1)})
			case "plaintext":
				payloads[c] = fmt.Appendf(payloads[c], "bench.%d %.17g %d\n", id, v, ts)
			case "binary", "decode":
				if err = enc.Add([]byte(fmt.Sprintf("bench.%d", id)), v, ts); err != nil {
					t.Fatal(err)
				}
				if enc.Ready() {
					payloads[c], err = enc.Flush(payloads[c])
					if err != nil {
						t.Fatal(err)
					}
				}
			default:
				t.Fatal("unknown mode", mode)
			}
		}
		if enc != nil {
			payloads[c], err = enc.Flush(payloads[c])
			enc.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		wireBytes += uint64(len(payloads[c]))
	}
	cfg := v2daemon.Config{Layer: 2, Producers: workers, Shards: shards}
	if mode == "l0" {
		cfg.Layer = 0
	}
	var store *v2daemon.Store
	var err error
	var srv *v2recv.Server
	var l net.Listener
	if mode != "decode" {
		store, err = v2daemon.Open(root, cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
	if mode != "l0" {
		l, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		rc := v2recv.Config{Logger: quiet()}
		if mode == "decode" {
			owners := make([]v2recv.Owner, workers)
			for i := range owners {
				owners[i] = &fakeOwner{}
			}
			srv, err = v2recv.Start(l, owners, rc)
		} else {
			srv, err = store.ReceiveTCP(l, rc)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	var profile *os.File
	if p := os.Getenv("V2_TCP_PROFILE"); p != "" {
		profile, err = os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		if err = pprof.StartCPUProfile(profile); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	cpu0 := cpu()
	var wg sync.WaitGroup
	if mode == "l0" {
		// Same tuple corpus; numeric owners receive private slices and identical
		// chunking. This bypasses names, journal, receive queues and TCP decoding.
		for p := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var cursor uint64
				for c := p; c < connections; c += workers {
					for lo := 0; lo < len(rows[c]); lo += 1024 {
						hi := min(lo+1024, len(rows[c]))
						cursor += uint64(hi - lo)
						if e := store.Producers[p].AppendNumeric(rows[c][lo:hi]); e != nil {
							t.Error(e)
							return
						}
						if hi%32768 == 0 {
							if e := store.Producers[p].Seal(); e != nil {
								t.Error(e)
								return
							}
						}
					}
					_ = cursor
				}
			}()
		}
	} else {
		for c := range connections {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, e := net.Dial("tcp", l.Addr().String())
				if e != nil {
					t.Error(e)
					return
				}
				defer conn.Close()
				data := payloads[c]
				for len(data) > 0 {
					n, e := conn.Write(data)
					if e != nil {
						t.Error(e)
						return
					}
					data = data[n:]
				}
			}()
		}
	}
	wg.Wait()
	sentSeconds := time.Since(start).Seconds()
	if srv != nil {
		deadline := time.Now().Add(2 * time.Minute)
		for srv.Stats().Accepted < total && srv.Stats().SinkErrors == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err = srv.Wait(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if got := srv.Stats(); got.Accepted != total || got.Rejected+got.ParseErrors+got.TransportErrors+got.SinkErrors != 0 {
			t.Fatal(got)
		}
	}
	if store != nil {
		if err = store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	seconds := time.Since(start).Seconds()
	cpuSeconds := cpu() - cpu0
	if profile != nil {
		pprof.StopCPUProfile()
		_ = profile.Close()
	}
	result := map[string]any{"started_unix_nano": start.UnixNano(), "completed_unix_nano": start.Add(time.Duration(seconds * float64(time.Second))).UnixNano(), "mode": mode, "points": total, "connections": connections, "workers": workers, "shards": shards, "sent_seconds": sentSeconds, "complete_seconds": seconds, "points_per_second": float64(total) / seconds, "cpu_seconds": cpuSeconds, "cpu_ns_per_point": cpuSeconds * 1e9 / float64(total), "wire_bytes": wireBytes, "wire_bytes_per_point": float64(wireBytes) / float64(total), "sender_cpu_included": true}
	if srv != nil {
		result["receiver_stats"] = srv.Stats()
		if srv.Stats().WireBytes != wireBytes {
			t.Fatalf("socket byte mismatch: got %d want %d", srv.Stats().WireBytes, wireBytes)
		}
	}
	if store != nil {
		reader, e := v2core.Open(filepath.Join(root, "placement-group-00000001", "core"), v2core.Config{JournalIdentity: "core-first-fixed-v1", Producers: workers, Shards: shards, Owners: shards, MaxRows: 2 << 20, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1"}, v2core.Hooks{})
		if e != nil {
			t.Fatal(e)
		}
		defer reader.Close()
		cats := make([]*partlog.NameCatalog, workers)
		if mode != "l0" {
			for p := range cats {
				cats[p], e = partlog.OpenNameCatalog(filepath.Join(root, "placement-group-00000001", "journal", fmt.Sprintf("producer-%03d", p), "catalog"))
				if e != nil {
					t.Fatal(e)
				}
				defer cats[p].Close()
			}
		}
		seen := make([]bool, total)
		var checked uint64
		for _, db := range reader.DBs() {
			snap := db.NewSnapshot()
			e = v2core.ScanRaw(snap, func(r v2core.Row) error {
				id := int(r.ID) - 1
				if mode != "l0" {
					p := int((r.ID - 1) % uint64(workers))
					local := (r.ID-1)/uint64(workers) + 1
					name, _, ok := cats[p].Lookup(local)
					if !ok {
						return fmt.Errorf("missing name")
					}
					if _, err := fmt.Sscanf(name, "bench.%d", &id); err != nil {
						return err
					}
				}
				round := int(r.Time - 1794700800)
				if id < 0 || id >= connections*width || round < 0 || round >= rounds || r.Bits != math.Float64bits(float64(id)+.125) {
					return fmt.Errorf("changed tuple %+v", r)
				}
				ordinal := id*rounds + round
				if seen[ordinal] {
					return fmt.Errorf("duplicate tuple")
				}
				seen[ordinal] = true
				checked++
				return nil
			})
			snap.Close()
			if e != nil {
				t.Fatal(e)
			}
		}
		if checked != total {
			t.Fatal("missing rows", checked, total)
		}
		result["exact_core_points"] = checked
	}
	b, _ := json.Marshal(result)
	fmt.Println("V2_TCP_RESULT", string(b))
}
