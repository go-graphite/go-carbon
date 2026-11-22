package v2daemon

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

func TestCheckNameLimits(t *testing.T) {
	for name, ok := range map[string]bool{
		strings.Repeat("a.", points.MaxMetricNameBytes/2):       true, // 4096 bytes, short components
		strings.Repeat("a.", points.MaxMetricNameBytes/2) + "b": false,
		strings.Repeat("d", 255) + ".leaf":                      true,
		strings.Repeat("d", 256) + ".leaf":                      false,
		"dir." + strings.Repeat("l", 251):                       true, // 251 + ".wsp" = 255
		"dir." + strings.Repeat("l", 252):                       false,
		"a..b.":                                                 true,
	} {
		if err := checkNameLimits(name); (err == nil) != ok || (err != nil && !errors.Is(err, ErrNameTooLong)) {
			t.Errorf("len=%d: err=%v, want ok=%t", len(name), err, ok)
		}
	}
}

// One over-long name, one over-long path component and one name without a
// policy, among normal names, refuse only their own points. The good names'
// stored records and read bits match the same stream without them, over TCP
// (the bad points arrive on the first of two connections) and through direct
// Append.
func TestReceiverRejectsBadNamesLocally(t *testing.T) {
	type image struct {
		Records map[string][]sealrollup.Record
		Reads   map[string][]uint64
	}
	tooLong := "bad." + strings.Repeat("segment.", 550)      // ~4.4 KB, short components
	longPart := "bad.dir." + strings.Repeat("c", 300) + ".x" // one 300-byte component
	noPolicy := "unresolvable.name"
	bad := map[int]string{10: tooLong, 25: longPart, 40: noPolicy}
	run := func(tcp, withBad bool) (image, int, int) {
		r, err := v2read.New(2, 16<<20, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		r.Identities = &v2core.PolicyCatalog{}
		s, err := Open(t.TempDir(), Config{Layer: 3, Producers: 2, Shards: 2, MaxCoreRows: 4096, GroupInterval: time.Hour, Hooks: v2core.Hooks{Identities: r.Identities, PublishBatches: r.Publish}})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		resolve := func(name string) ([]byte, error) {
			if name == noPolicy {
				return nil, fmt.Errorf("no v2 schema for %q", name)
			}
			return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
		}
		var long, invalid int
		counts := make(chan bool, 16) // owner workers report; read after Wait or Drain
		reject := func(name string, err error) { counts <- errors.Is(err, ErrNameTooLong) }
		owners := make([]v2recv.Owner, 2)
		for i, p := range s.Producers {
			owners[i] = fixedReceiverClock{&applicationOwner{p: p, resolve: resolve, reject: reject}}
		}
		var srv *v2recv.Server
		if tcp {
			l, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			if srv, e = v2recv.Start(l, owners, v2recv.Config{BatchSize: 7, SyncInterval: 5 * time.Millisecond}); e != nil {
				t.Fatal(e)
			}
		}
		bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
		type sample struct {
			name string
			p    points.Point
		}
		var stream []sample
		for i := range 60 {
			if name, ok := bad[i]; ok && withBad {
				stream = append(stream, sample{name, points.Point{Timestamp: 1000, Value: 7}})
			}
			stream = append(stream, sample{fmt.Sprintf("receiver.differential.%d", (i/5)%2), points.Point{Timestamp: 999 + int64(i%9), Value: math.Float64frombits(bits[i%len(bits)])}})
		}
		split := len(stream) - 15 // the second connection starts after every bad point
		want := uint64(len(stream))
		sent := uint64(0)
		for _, part := range [][]sample{stream[:split], stream[split:]} {
			// Per-name order needs the first connection consumed before the
			// second. The short sync tick appends pending points without the
			// extra core seal a Fence (Flush) would add.
			for deadline := time.Now().Add(30 * time.Second); tcp && srv.Stats().Accepted < sent; time.Sleep(time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("first connection not consumed: %+v", srv.Stats())
				}
			}
			sent += uint64(len(part))
			if !tcp {
				for _, x := range part {
					if err = owners[v2recv.OwnerFor(x.name, 2)].Append([]string{x.name}, nil, []points.Point{x.p}, nil, 1010); err != nil {
						t.Fatal(err)
					}
				}
				continue
			}
			encoder, e := codec.NewEncoder(codec.Options{MaxRows: 19, MaxNames: 16})
			if e != nil {
				t.Fatal(e)
			}
			wire, _ := codec.AppendPreamble(nil, 0)
			for _, x := range part {
				if err = encoder.Add([]byte(x.name), x.p.Value, x.p.Timestamp); err != nil {
					t.Fatal(err)
				}
				if encoder.Ready() {
					if wire, err = encoder.Flush(wire); err != nil {
						t.Fatal(err)
					}
				}
			}
			if wire, err = encoder.Flush(wire); err != nil {
				t.Fatal(err)
			}
			encoder.Close()
			c, e := net.Dial("tcp", srv.Addr().String())
			if e != nil {
				t.Fatal(e)
			}
			for len(wire) > 0 {
				n := min(137, len(wire))
				if _, err = c.Write(wire[:n]); err != nil {
					t.Fatal(err)
				}
				wire = wire[n:]
			}
			c.Close()
		}
		if tcp {
			deadline := time.Now().Add(5 * time.Second)
			for srv.Stats().Accepted < want && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err = srv.Wait(ctx); err != nil {
				t.Fatal("receiver stopped:", err)
			}
			if got := srv.Stats(); got.Accepted != want || got.Connections != 2 || got.Rejected+got.ParseErrors+got.SinkErrors+got.TransportErrors != 0 {
				t.Fatalf("receiver stats %+v, want %d accepted on 2 connections", got, want)
			}
		} else {
			for _, o := range owners {
				if err = o.Drain(); err != nil {
					t.Fatal(err)
				}
			}
		}
		close(counts)
		for isLong := range counts {
			if isLong {
				long++
			} else {
				invalid++
			}
		}
		for i, p := range s.Producers {
			if err = s.Core.AppendBatches(i, nil, p.cursor, true); err != nil {
				t.Fatal(err)
			}
		}
		im := image{Records: map[string][]sealrollup.Record{}, Reads: map[string][]uint64{}}
		for _, db := range s.Core.DBs() {
			snap := db.NewSnapshot()
			err = v2core.ScanRollups(snap, func(p v2core.RollupPolicy, _ v2core.RollupMetadata, records []sealrollup.Record) error {
				im.Records[string(p.Name)] = append(im.Records[string(p.Name)], records...)
				return nil
			})
			snap.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		for name, records := range im.Records {
			sort.Slice(records, func(i, j int) bool {
				if records[i].Archive != records[j].Archive {
					return records[i].Archive < records[j].Archive
				}
				return records[i].Slot < records[j].Slot
			})
			if err = r.FetchMany(context.Background(), []string{name}, 990, 1010, 1010, func(_ string, series *v2read.Series) error {
				for _, v := range series.Values {
					im.Reads[name] = append(im.Reads[name], math.Float64bits(v))
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		b, err := json.Marshal(im)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("tcp=%t bad=%t names=%d too-long=%d invalid=%d stored-records-reads-sha256=%x", tcp, withBad, len(im.Records), long, invalid, sha256.Sum256(b))
		return im, long, invalid
	}
	for _, tcp := range []bool{false, true} {
		clean, _, _ := run(tcp, false)
		if len(clean.Records) != 2 {
			t.Fatal("missing names", len(clean.Records))
		}
		got, long, invalid := run(tcp, true)
		if long != 2 || invalid != 1 {
			t.Fatalf("tcp=%t: too-long=%d invalid=%d, want 2 and 1", tcp, long, invalid)
		}
		if !reflect.DeepEqual(got, clean) {
			t.Fatalf("tcp=%t: bad names changed the good names' stored records or read bits", tcp)
		}
	}
}
