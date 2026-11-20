package v2daemon

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"reflect"
	"sort"
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

// Fix clocks and seal each append so the final correction flags are identical
// across the direct and socket batching schedules.
type fixedReceiverClock struct{ *applicationOwner }

func (o fixedReceiverClock) Append(n []string, h []uint64, p []points.Point, seq []uint64, _ int64) error {
	if err := o.applicationOwner.Append(n, h, p, seq, 1010); err != nil {
		return err
	}
	if err := o.p.Drain(); err != nil {
		return err
	}
	return o.p.engine.AppendBatches(o.p.index, nil, o.p.cursor, true)
}

func (o fixedReceiverClock) AppendBytes(text []byte, ends []int, h []uint64, p []points.Point, seq []uint64, _ int64) error {
	if err := o.applicationOwner.AppendBytes(text, ends, h, p, seq, 1010); err != nil {
		return err
	}
	if err := o.p.Drain(); err != nil {
		return err
	}
	return o.p.engine.AppendBatches(o.p.index, nil, o.p.cursor, true)
}

func TestReceiverStoredRecordsAndReadsDifferential(t *testing.T) {
	type image struct {
		Records map[string][]sealrollup.Record
		Reads   map[string][]uint64
	}
	var images []image
	for _, mode := range []string{"direct", "binary", "bytes"} {
		tcp := mode == "binary"
		func() {
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
				return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
			}
			owners := make([]v2recv.Owner, 2)
			for i, p := range s.Producers {
				owners[i] = fixedReceiverClock{&applicationOwner{p: p, resolve: resolve}}
			}
			var srv *v2recv.Server
			var c net.Conn
			encoder, err := codec.NewEncoder(codec.Options{MaxRows: 19, MaxNames: 16})
			if err != nil {
				t.Fatal(err)
			}
			defer encoder.Close()
			wire, _ := codec.AppendPreamble(nil, 0)
			if tcp {
				l, e := net.Listen("tcp", "127.0.0.1:0")
				if e != nil {
					t.Fatal(e)
				}
				srv, e = v2recv.Start(l, owners, v2recv.Config{BatchSize: 7, SyncInterval: time.Hour})
				if e != nil {
					t.Fatal(e)
				}
				c, e = net.Dial("tcp", l.Addr().String())
				if e != nil {
					t.Fatal(e)
				}
			}
			bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
			for i := range 210 {
				name := fmt.Sprintf("receiver.differential.%d", (i/5)%2)
				p := points.Point{Timestamp: 999 + int64(i%9), Value: math.Float64frombits(bits[i%len(bits)])}
				if tcp {
					if err = encoder.Add([]byte(name), p.Value, p.Timestamp); err != nil {
						t.Fatal(err)
					}
					if encoder.Ready() {
						wire, err = encoder.Flush(wire)
						if err != nil {
							t.Fatal(err)
						}
					}
				} else {
					owner := owners[v2recv.OwnerFor(name, 2)]
					if mode == "bytes" {
						err = owner.(v2recv.ByteOwner).AppendBytes([]byte(name), []int{len(name)}, nil, []points.Point{p}, nil, 1010)
					} else {
						err = owner.Append([]string{name}, nil, []points.Point{p}, nil, 1010)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if tcp {
				wire, err = encoder.Flush(wire)
				if err != nil {
					t.Fatal(err)
				}
				for len(wire) > 0 {
					n := min(137, len(wire))
					if _, err = c.Write(wire[:n]); err != nil {
						t.Fatal(err)
					}
					wire = wire[n:]
				}
				c.Close()
				deadline := time.Now().Add(5 * time.Second)
				for srv.Stats().Accepted < 210 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err = srv.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				if got := srv.Stats(); got.Accepted != 210 || got.Rejected+got.ParseErrors+got.SinkErrors+got.TransportErrors != 0 {
					t.Fatal(got)
				}
			} else {
				for _, o := range owners {
					if err = o.Drain(); err != nil {
						t.Fatal(err)
					}
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
				err = r.FetchMany(context.Background(), []string{name}, 990, 1010, 1010, func(_ string, series *v2read.Series) error {
					for _, v := range series.Values {
						im.Reads[name] = append(im.Reads[name], math.Float64bits(v))
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(im.Records) != 2 {
				t.Fatal("missing names", len(im.Records))
			}
			b, err := json.Marshal(im)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("mode=%s stored-records-reads-sha256=%x", mode, sha256.Sum256(b))
			images = append(images, im)
		}()
	}
	for i := 1; i < len(images); i++ {
		if !reflect.DeepEqual(images[0], images[i]) {
			t.Fatalf("input mode %d changed stored records or read bits", i)
		}
	}
}
