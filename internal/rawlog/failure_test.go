package rawlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

func testFrame(t *testing.T) []byte {
	t.Helper()
	e, err := codec.NewEncoder(codec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err = e.Add([]byte("shared"), 1, 10); err != nil {
		t.Fatal(err)
	}
	frame, err := e.Flush(nil)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}
func TestCaptureEnvelopeAndReplayPoison(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(dir)
	if err != nil {
		t.Fatal(err)
	}
	pre, _ := codec.AppendPreamble(nil, 0)
	for _, id := range []uint64{0, 1} {
		if _, err = w.Begin(id, pre); err != nil {
			t.Fatal(err)
		}
	}
	good := testFrame(t)
	bad := bytes.Clone(good)
	bad[len(bad)-1] ^= 1
	if _, err = w.Stage(0, bad, 1); err == nil {
		t.Fatal("accepted bad CRC")
	}
	// Valid envelope, invalid dictionary start. Capture must not decode it.
	bad = bytes.Clone(good)
	bad[codec.HeaderSize] = 100
	binary.BigEndian.PutUint32(bad[16:20], crc32.Checksum(bad[20:], table))
	if _, err = w.Stage(0, bad, 1); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Stage(0, good, 1); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Stage(1, good, 1); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "00000000000000000000.raw"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var applied int
	st, err := Replay(f, ReplayOptions{Workers: 2}, func(_ uint64, p *points.Points) error { applied += len(p.Data); return nil })
	if err == nil || st.Broken != 2 || applied != 1 {
		t.Fatalf("stats=%+v points=%d err=%v", st, applied, err)
	}
}
func TestReplayRejectsEveryTruncationAndMetadataDamage(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(dir)
	if err != nil {
		t.Fatal(err)
	}
	pre, _ := codec.AppendPreamble(nil, 0)
	_, err = w.Begin(0, pre)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Stage(0, testFrame(t), 123)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "00000000000000000000.raw"))
	if err != nil {
		t.Fatal(err)
	}
	// Prefixes ending at complete record boundaries are valid crash tails; all
	// other cuts must fail without guessing a resynchronization point.
	valid := map[int]bool{len(magic) + headerSize: true, len(magic) + 2*headerSize + len(pre): true, len(data): true}
	for n := 0; n <= len(data); n++ {
		_, err := Replay(bytes.NewReader(data[:n]), ReplayOptions{}, nil)
		if (err == nil) != valid[n] {
			t.Fatalf("cut %d accepted=%v", n, err == nil)
		}
	}
	for _, index := range []int{0, len(magic), len(magic) + headerSize + 8, len(data) - 1} {
		bad := bytes.Clone(data)
		bad[index] ^= 1
		if _, err := Replay(bytes.NewReader(bad), ReplayOptions{}, nil); err == nil {
			t.Fatalf("accepted corruption at %d", index)
		}
	}
}
func TestSyncFailureDoesNotAdvanceFrontier(t *testing.T) {
	w, err := Create(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pre, _ := codec.AppendPreamble(nil, 0)
	seq, err := w.Begin(0, pre)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.file.Close(); err != nil {
		t.Fatal(err)
	}
	frontier, err := w.Sync()
	if err == nil || frontier >= seq {
		t.Fatalf("false durable ack %d/%d %v", frontier, seq, err)
	}
	if _, err = w.Stage(0, testFrame(t), 1); err == nil {
		t.Fatal("writer resumed after IO failure")
	}
}
func TestApplyErrorStopsFurtherApplyAndDrains(t *testing.T) {
	var b bytes.Buffer
	b.WriteString(magic)
	pre, _ := codec.AppendPreamble(nil, 0)
	for _, r := range []record{{kind: readyRecord}, {kind: openRecord, seq: 1, conn: 0, data: pre}, {kind: frameRecord, seq: 2, conn: 0, data: testFrame(t)}} {
		if err := r.write(&b); err != nil {
			t.Fatal(err)
		}
	}
	sentinel := errors.New("apply failed")
	_, err := Replay(&b, ReplayOptions{Workers: 2}, func(uint64, *points.Points) error { return sentinel })
	if !errors.Is(err, sentinel) || errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestWireBytesAndReceiptFrontier(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(dir)
	if err != nil {
		t.Fatal(err)
	}
	pre, _ := codec.AppendPreamble(nil, 0)
	_, err = w.Begin(12, pre)
	if err != nil {
		t.Fatal(err)
	}
	frame := testFrame(t)
	seq, err := w.Stage(12, frame, 5678)
	if err != nil {
		t.Fatal(err)
	}
	st := w.Stats()
	if st.Durable >= seq || st.DurablePoints != 0 || st.Points != 1 {
		t.Fatalf("premature receipt %+v", st)
	}
	ack, err := w.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if ack != seq || w.Stats().DurablePoints != 1 {
		t.Fatal("sync omitted receipt")
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "00000000000000000000.raw"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var found bool
	if err = walk(f, func(r record) error {
		if r.kind == frameRecord {
			found = true
			if !bytes.Equal(r.data, frame) || r.arrival != 5678 || r.conn != 12 || r.seq != seq {
				t.Fatal("wire/metadata changed")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("missing frame")
	}
}
