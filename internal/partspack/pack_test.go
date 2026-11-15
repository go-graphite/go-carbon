package partspack

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func assertPoints(t *testing.T, want, got []chunkstore.Point) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("counts %d/%d", len(want), len(got))
	}
	for i := range want {
		if want[i].Timestamp != got[i].Timestamp || math.Float64bits(want[i].Value) != math.Float64bits(got[i].Value) {
			t.Fatalf("point %d differs", i)
		}
	}
}
func TestColdPackBytesAndBits(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 23))
	for _, n := range []int{1, 2, 144, 256, 1440, 4096, 8192} {
		for _, kind := range []string{"random", "counter", "gaps"} {
			t.Run(fmt.Sprintf("%s/%d", kind, n), func(t *testing.T) {
				ps := make([]chunkstore.Point, n)
				ts := int64(1700000000)
				for i := range ps {
					v := math.Float64frombits(rng.Uint64())
					if kind == "counter" {
						v = float64(i)
					}
					if kind == "gaps" && i%7 == 0 {
						ts += 60
					}
					ps[i] = chunkstore.Point{Timestamp: ts, Value: v}
					ts += 60
				}
				want, err := chunkstore.EncodeSealedPoints(ps, 60)
				if err != nil {
					t.Fatal(err)
				}
				got, err := EncodeBlock(ps, 60)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(want, got) {
					t.Fatal("cold-pack byte delta")
				}
				decoded, err := DecodeBlock(got)
				if err != nil {
					t.Fatal(err)
				}
				assertPoints(t, ps, decoded)
			})
		}
	}
}
func TestFallbackIEEEAndCorruption(t *testing.T) {
	ps := []chunkstore.Point{{Timestamp: -1, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: math.MinInt64, Value: math.Copysign(0, -1)}, {Timestamp: math.MaxInt64, Value: math.Float64frombits(1)}}
	b, err := EncodeBlock(ps, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBlock(b)
	if err != nil {
		t.Fatal(err)
	}
	assertPoints(t, ps, got)
	for i := range b {
		bad := bytes.Clone(b)
		bad[i] ^= 1
		if _, err := DecodeBlock(bad); err == nil {
			t.Fatalf("corruption %d accepted", i)
		}
	}
	for n := 0; n < len(b); n++ {
		if _, err := DecodeBlock(b[:n]); err == nil {
			t.Fatalf("truncation %d accepted", n)
		}
	}
	empty, err := EncodeBlock(nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if points, err := DecodeBlock(empty); err != nil || len(points) != 0 {
		t.Fatal(points, err)
	}
	if _, err := EncodeBlock(make([]chunkstore.Point, MaxPoints+1), 1); err == nil {
		t.Fatal("bound")
	}
	if _, err := EncodeBlock(ps, 0); err == nil {
		t.Fatal("step")
	}
}
func TestPolicyAndWidths(t *testing.T) {
	p := Policy{Method: 1, XFilesFactorBits: math.Float32bits(.5), Archives: []Retention{{Step: 60, Slots: 10080}, {Step: 600, Slots: 2520}, {Step: 6000, Slots: 630}, {Step: 60000, Slots: 157}}}
	b, err := p.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParsePolicy(b)
	if err != nil || !reflect.DeepEqual(p, got) {
		t.Fatal(got, err)
	}
	d, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	got.Method++
	other, _ := got.Digest()
	if d == other {
		t.Fatal("policy digest collision")
	}
	for n := 0; n < len(b); n++ {
		if _, err := ParsePolicy(b[:n]); err == nil {
			t.Fatal("truncated policy")
		}
	}
	options := PartitionOptions{BaseSeconds: 86400, MaxSeconds: 30 * 86400, TargetPoints: 4096, HorizonDivisor: 8}
	want := []int64{86400, 189000, 472500, 1177500}
	for i, a := range p.Archives {
		width, err := options.Width(a)
		if err != nil || width != want[i] {
			t.Fatal(width, err)
		}
	}
	if _, err := options.Width(Retention{Step: math.MaxUint32, Slots: math.MaxUint32}); err == nil {
		t.Fatal("overflow accepted")
	}
}
