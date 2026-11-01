package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
)

type observation struct {
	name string
	ts   int64
	bits uint64
}

func observe(dst *[]observation) Visit {
	return func(_ uint32, name []byte, value float64, ts int64, _ *any) {
		*dst = append(*dst, observation{string(name), ts, math.Float64bits(value)})
	}
}
func encoder(t testing.TB, o Options) *Encoder {
	t.Helper()
	e, err := NewEncoder(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}
func decoder(t testing.TB, o Options) *Decoder {
	t.Helper()
	d, err := NewDecoder(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d
}
func flush(t testing.TB, e *Encoder, dst []byte) []byte {
	t.Helper()
	b, err := e.Flush(dst)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func add(t testing.TB, e *Encoder, name string, value float64, ts int64) {
	t.Helper()
	if err := e.Add([]byte(name), value, ts); err != nil {
		t.Fatal(err)
	}
}
func decodeAll(t testing.TB, d *Decoder, data []byte, visit Visit) []FrameInfo {
	t.Helper()
	r := bytes.NewReader(data)
	var infos []FrameInfo
	for r.Len() > 0 {
		info, err := d.ReadFrame(r, visit)
		if err != nil {
			t.Fatal(err)
		}
		infos = append(infos, info)
	}
	return infos
}

func TestRoundTrip(t *testing.T) {
	for _, flags := range []byte{0, FlagZstd} {
		t.Run(string(rune('0'+flags)), func(t *testing.T) {
			o := Options{Flags: flags, MaxRows: 97, MaxNames: 35, FrameSize: 256}
			e, d := encoder(t, o), decoder(t, o)
			rng := rand.New(rand.NewSource(42))
			want, got := map[string][]observation{}, map[string][]observation{}
			var wire []byte
			for i := range 4000 {
				name := strings.Repeat("example.application.worker.", 8) + string(rune('A'+rng.Intn(45)))
				ts := int64(rng.Uint64())
				bits := rng.Uint64()
				if i%7 == 0 {
					bits = 0x8000000000000000
				}
				if i%11 == 0 {
					bits = 0x7ff8000000000001
				}
				want[name] = append(want[name], observation{name, ts, bits})
				add(t, e, name, math.Float64frombits(bits), ts)
				if e.Ready() || i%13 == 0 {
					wire = flush(t, e, wire)
				}
			}
			wire = flush(t, e, wire)
			infos := decodeAll(t, d, wire, func(_ uint32, n []byte, v float64, ts int64, _ *any) {
				s := string(n)
				got[s] = append(got[s], observation{s, ts, math.Float64bits(v)})
			})
			if !reflect.DeepEqual(want, got) {
				t.Fatal("round trip changed per-name point order or bits")
			}
			var resets int
			for _, info := range infos {
				if info.Kind == ResetFrame {
					resets++
				}
			}
			if resets == 0 {
				t.Fatal("name cap did not reset")
			}
		})
	}
}

func TestSnapshotAndSlots(t *testing.T) {
	for _, flags := range []byte{0, FlagZstd} {
		o := Options{Flags: flags, FrameSize: 64, MaxFrameSize: 1024}
		e, d := encoder(t, o), decoder(t, o)
		for i := range 20 {
			add(t, e, strings.Repeat("metric.", 4)+string(rune('a'+i)), float64(i), 1000+int64(i))
		}
		decodeAll(t, d, flush(t, e, nil), func(_ uint32, _ []byte, _ float64, _ int64, slot *any) { *slot = "reference" })
		snapshot, err := d.Snapshot(nil)
		if err != nil {
			t.Fatal(err)
		}
		next := decoder(t, o)
		infos := decodeAll(t, next, snapshot, nil)
		if len(infos) < 3 {
			t.Fatal("snapshot was not split into bounded frames")
		}
		for _, e := range next.entries {
			if e.slot != nil {
				t.Fatal("snapshot retained caller slot")
			}
		}
		add(t, e, strings.Repeat("metric.", 4)+"a", 1.25, 1001)
		data := flush(t, e, nil)
		var want, got []observation
		decodeAll(t, d, data, observe(&want))
		decodeAll(t, next, data, observe(&got))
		if !reflect.DeepEqual(want, got) {
			t.Fatal("snapshot predictors differ")
		}
		if err := e.Reset(); err != nil {
			t.Fatal(err)
		}
		add(t, e, "after.reset", -1, 2000)
		decodeAll(t, d, flush(t, e, nil), func(id uint32, _ []byte, _ float64, _ int64, slot *any) {
			if id != 0 || *slot != nil {
				t.Fatal("reset retained ids or slots")
			}
		})
	}
}

func golden(t testing.TB) []byte {
	e := encoder(t, Options{})
	b, _ := AppendPreamble(nil, 0)
	add(t, e, "example.requests", 1, 1700000000)
	add(t, e, "example.latency", 1.25, 1700000000)
	add(t, e, "example.requests", 2, 1700000001)
	b = flush(t, e, b)
	add(t, e, "example.latency", 1.5, 1700000001)
	b = flush(t, e, b)
	if err := e.Reset(); err != nil {
		t.Fatal(err)
	}
	add(t, e, "example.requests", 3, 1700000002)
	return flush(t, e, b)
}

func TestGolden(t *testing.T) {
	wire := golden(t)
	data, err := os.ReadFile("testdata/stream-v1.hex")
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, want) {
		t.Fatalf("golden differs: %x", wire)
	}
	flags, err := ParsePreamble(wire[:PreambleSize])
	if err != nil || flags != 0 {
		t.Fatal("preamble", err)
	}
	var got []observation
	decodeAll(t, decoder(t, Options{}), want[PreambleSize:], observe(&got))
	if len(got) != 5 {
		t.Fatalf("got %d points", len(got))
	}
}

func repairCRC(b []byte) {
	binary.BigEndian.PutUint32(b[16:20], crc32.Checksum(b[HeaderSize:], castagnoli))
}
func TestRejectFrames(t *testing.T) {
	e := encoder(t, Options{})
	add(t, e, "x", 1, 1)
	valid := flush(t, e, nil)
	cases := []struct {
		name string
		edit func([]byte) []byte
	}{
		{"crc", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }},
		{"reserved", func(b []byte) []byte { b[1] = 1; return b }},
		{"kind", func(b []byte) []byte { b[0] = 4; return b }},
		{"length", func(b []byte) []byte { b[7]++; return b }},
		{"raw_length", func(b []byte) []byte { b[11]++; return b }},
		{"huge", func(b []byte) []byte { binary.BigEndian.PutUint32(b[4:8], 1<<30); return b }},
		{"rows", func(b []byte) []byte { binary.BigEndian.PutUint32(b[12:16], 1<<30); return b }},
		{"base", func(b []byte) []byte { b[HeaderSize] = 1; repairCRC(b); return b }},
		{"unknown_id", func(b []byte) []byte { b[HeaderSize+6] = 1; repairCRC(b); return b }},
		{"xor", func(b []byte) []byte { b[len(b)-3] = 255; repairCRC(b); return b }},
		{"trailing", func(b []byte) []byte {
			b = append(b, 0)
			binary.BigEndian.PutUint32(b[4:8], uint32(len(b)-HeaderSize))
			binary.BigEndian.PutUint32(b[8:12], uint32(len(b)-HeaderSize))
			repairCRC(b)
			return b
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := decoder(t, Options{})
			calls := 0
			if _, err := d.Decode(tc.edit(bytes.Clone(valid)), func(uint32, []byte, float64, int64, *any) { calls++ }); err == nil {
				t.Fatal("accepted invalid frame")
			}
			if calls != 0 {
				t.Fatal("visited rejected frame")
			}
			if _, err := d.Decode(valid, nil); !errors.Is(err, ErrBroken) {
				t.Fatal("decoder not terminal", err)
			}
		})
	}
	for n := 0; n < len(valid); n++ {
		d := decoder(t, Options{})
		_, err := d.ReadFrame(bytes.NewReader(valid[:n]), nil)
		if err == nil {
			t.Fatal("accepted truncated frame", n)
		}
		if !bytes.Equal(d.Pending(), valid[:n]) {
			t.Fatal("lost partial frame", n)
		}
		if n == 0 && !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		if n > 0 && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
		if _, err := d.ReadFrame(bytes.NewReader(valid[n:]), nil); err != nil {
			t.Fatal("cannot resume partial frame", n, err)
		}
	}
}

func TestPreambleAndLimits(t *testing.T) {
	b, _ := AppendPreamble(nil, FlagZstd)
	for i := range b {
		bad := bytes.Clone(b)
		bad[i] ^= 128
		if _, err := ParsePreamble(bad); err == nil {
			t.Fatal("bad preamble", i)
		}
	}
	if _, err := NewEncoder(Options{MaxRows: -1}); err == nil {
		t.Fatal("negative limit")
	}
	e := encoder(t, Options{MaxNameSize: 2})
	if e.Add(nil, 0, 0) == nil || e.Add([]byte("abc"), 0, 0) == nil {
		t.Fatal("bad name accepted")
	}
}

func TestKnownNamesAllocateNothing(t *testing.T) {
	e := encoder(t, Options{MaxRows: 8})
	name := []byte("example.steady.state")
	for range 20 {
		add(t, e, string(name), 1, 1)
	}
	dst := flush(t, e, nil)
	allocs := testing.AllocsPerRun(100, func() {
		for range 8 {
			if err := e.Add(name, 1, 2); err != nil {
				panic(err)
			}
		}
		var err error
		dst, err = e.Flush(dst[:0])
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("steady state: %g allocations per batch", allocs)
	}
}

func FuzzDecoder(f *testing.F) {
	f.Add(golden(f))
	e := encoder(f, Options{Flags: FlagZstd})
	add(f, e, "example.compressed", 1, 2)
	wire, _ := AppendPreamble(nil, FlagZstd)
	f.Add(flush(f, e, wire))
	f.Add([]byte(Magic))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) < PreambleSize {
			return
		}
		flags, err := ParsePreamble(b[:PreambleSize])
		if err != nil {
			return
		}
		d := decoder(t, Options{Flags: flags, MaxFrameSize: 64 << 10, MaxNames: 1000, MaxRows: 1000, MaxNameSize: 1024})
		r := bytes.NewReader(b[PreambleSize:])
		for r.Len() > 0 {
			if _, err := d.ReadFrame(r, func(_ uint32, n []byte, _ float64, _ int64, _ *any) {
				if len(n) == 0 {
					t.Fatal("empty name")
				}
			}); err != nil {
				return
			}
		}
	})
}

// Keep lengths and CRC valid so mutations reach the column/state parsers too.
func FuzzFramePayload(f *testing.F) {
	wire := golden(f)[PreambleSize:]
	size := binary.BigEndian.Uint32(wire[4:8])
	f.Add(DataFrame, uint32(3), wire[HeaderSize:HeaderSize+int(size)])
	f.Add(StateFrame, uint32(0), []byte{0, 1, 1, 'x', 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, kind byte, rows uint32, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		o := Options{MaxFrameSize: 64 << 10, MaxNames: 1024, MaxRows: 1024, MaxNameSize: 1024}
		d := decoder(t, o)
		frame, err := appendFrame(nil, raw, kind%3, int(rows%1025), nil, o.MaxFrameSize)
		if err != nil {
			return
		}
		if _, err = d.Decode(frame, nil); err != nil {
			return
		}
		snapshot, err := d.Snapshot(nil)
		if err != nil {
			t.Fatal(err)
		}
		decodeAll(t, decoder(t, o), snapshot, nil)
	})
}
