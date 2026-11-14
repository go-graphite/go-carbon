package inventory

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

// An independently built legacy frame checks the command's complete-file
// boundary and timestamp selection, in addition to the stream decoder tests.
func commandFixture(t *testing.T) manifest {
	t.Helper()
	raw := (&points.Points{Metric: "z.inf", Data: []points.Point{{Timestamp: 100, Value: math.Inf(1)}, {Timestamp: 110, Value: math.Inf(-1)}, {Timestamp: 120, Value: 9}}}).AppendBinary(nil)
	raw = (&points.Points{Metric: "a.zero", Data: []points.Point{{Timestamp: 105, Value: math.Copysign(0, -1)}}}).AppendBinary(raw)
	raw = (&points.Points{Metric: "only.nan", Data: []points.Point{{Timestamp: 105, Value: math.NaN()}}}).AppendBinary(raw)
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	payload := encoder.EncodeAll(raw, nil)
	crc := crc32.MakeTable(crc32.Castagnoli)
	header := make([]byte, 64)
	copy(header, "GCINLOG1")
	binary.LittleEndian.PutUint64(header[8:], 1)
	binary.LittleEndian.PutUint64(header[16:], 31)
	binary.LittleEndian.PutUint32(header[32:], crc32.Checksum(header[:32], crc))
	frame := make([]byte, 52)
	binary.LittleEndian.PutUint32(frame, 0x4c494347)
	frame[4] = 1
	binary.LittleEndian.PutUint64(frame[8:], 31)
	binary.LittleEndian.PutUint32(frame[16:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[20:], uint32(len(raw)))
	binary.LittleEndian.PutUint32(frame[24:], 5)
	binary.LittleEndian.PutUint32(frame[44:], crc32.Checksum(payload, crc))
	binary.LittleEndian.PutUint32(frame[48:], crc32.Checksum(frame[:48], crc))
	wire := append(append(header, frame...), payload...)
	digest := sha256.Sum256(wire)
	path := filepath.Join(t.TempDir(), "copied.log")
	if err := os.WriteFile(path, wire, 0400); err != nil {
		t.Fatal(err)
	}
	return manifest{Version: 1, Snapshot: path, SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(wire)), Windows: []window{{Name: "first", From: 100, Until: 110}, {Name: "second", From: 110, Until: 120}}}
}

func TestInventoryVerifiesAndSelectsRawTimestamps(t *testing.T) {
	m := commandFixture(t)
	before, _ := os.ReadFile(m.Snapshot)
	var output bytes.Buffer
	if err := inventory(context.Background(), m, &output); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	if len(lines) != 3 {
		t.Fatalf("unexpected inventory %s", output.Bytes())
	}
	var header map[string]any
	if err := json.Unmarshal(lines[0], &header); err != nil || header["write_authority"] != false || header["names"] != float64(2) || header["selected_records"] != float64(3) {
		t.Fatalf("invalid summary: %s, %v", lines[0], err)
	}
	var a, z candidate
	if err := json.Unmarshal(lines[1], &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(lines[2], &z); err != nil {
		t.Fatal(err)
	}
	if a.Name != "a.zero" || a.Windows["first"] != 1 || z.Name != "z.inf" || z.Windows["first"] != 1 || z.Windows["second"] != 1 || z.First != 100 || z.Last != 110 {
		t.Fatalf("unexpected selections: %+v %+v", a, z)
	}
	after, _ := os.ReadFile(m.Snapshot)
	if !bytes.Equal(before, after) {
		t.Fatal("source snapshot changed")
	}
}

func TestInventoryEmitsNothingForUnverifiedEvidence(t *testing.T) {
	for _, kind := range []string{"sha", "size", "overlap-name", "window", "payload", "names", "names-ceiling", "dictionary", "dictionary-ceiling"} {
		t.Run(kind, func(t *testing.T) {
			m := commandFixture(t)
			switch kind {
			case "sha":
				m.SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
			case "size":
				m.Bytes++
			case "overlap-name":
				m.Windows[1].Name = m.Windows[0].Name
			case "window":
				m.Windows[1].Until = m.Windows[1].From
			case "names":
				m.MaxNames = 1
			case "names-ceiling":
				m.MaxNames = 16_000_001
			case "dictionary":
				m.MaxDictionaryBytes = -1
			case "dictionary-ceiling":
				m.MaxDictionaryBytes = (2 << 30) + 1
			case "payload":
				wire, _ := os.ReadFile(m.Snapshot)
				wire[len(wire)-1] ^= 1
				if err := os.Chmod(m.Snapshot, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(m.Snapshot, wire, 0400); err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(wire)
				m.SHA256 = hex.EncodeToString(digest[:])
			}
			var output bytes.Buffer
			if err := inventory(context.Background(), m, &output); err == nil || output.Len() != 0 {
				t.Fatalf("unverified inventory emitted: err=%v bytes=%d", err, output.Len())
			}
		})
	}
}

func TestCommandPublishesOnlyVerifiedNewOutput(t *testing.T) {
	m := commandFixture(t)
	dir := t.TempDir()
	manifestPath, output := filepath.Join(dir, "manifest.json"), filepath.Join(dir, "candidates.gz")
	writeManifest := func() {
		t.Helper()
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	goodHash := m.SHA256
	m.SHA256 = strings.Repeat("0", 64)
	writeManifest()
	args := []string{"-manifest", manifestPath, "-output", output}
	if err := Main(args); err == nil {
		t.Fatal("accepted wrong hash")
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("left unverified output: %v", err)
	}
	m.SHA256 = goodHash
	writeManifest()
	if err := Main(args); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := gzip.NewReader(bytes.NewReader(before))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(compressed)
	if err != nil {
		t.Fatal(err)
	}
	compressed.Close()
	var want bytes.Buffer
	if err := inventory(context.Background(), m, &want); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, want.Bytes()) {
		t.Fatalf("command output differs: %s", raw)
	}
	if err := Main(args); err == nil {
		t.Fatal("overwrote existing output")
	}
	after, _ := os.ReadFile(output)
	if !bytes.Equal(before, after) {
		t.Fatal("existing output changed")
	}
}
