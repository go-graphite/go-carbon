package partlog

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestSealedIndexRecoveryAndFallback(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "torn-index", "stale", "crc", "identity", "sequence"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			var scans, indexed atomic.Int32
			opts := Options{Streams: 1, SegmentBytes: 160, Hook: func(_ int, phase string) {
				if phase == "recover-scan" {
					scans.Add(1)
				}
				if phase == "recover-index" {
					indexed.Add(1)
				}
			}}
			l, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 12 {
				if _, err = l.Append(0, int64(i), bytes.Repeat([]byte{byte(i)}, 16)); err != nil {
					t.Fatal(err)
				}
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
			paths, _ := filepath.Glob(filepath.Join(dir, "*.plog"))
			path := paths[0]
			switch mode {
			case "missing":
				err = os.Remove(path + ".index")
			case "torn-index":
				err = os.WriteFile(path+".index", []byte("torn"), 0600)
			case "stale":
				// Same bytes, replacement inode: must validate the payload again.
				var data []byte
				data, err = os.ReadFile(path)
				if err == nil {
					err = os.Remove(path)
				}
				if err == nil {
					err = os.WriteFile(path, data, 0600)
				}
			case "crc", "identity", "sequence":
				var data []byte
				data, err = os.ReadFile(path)
				if mode == "crc" {
					data[headerSize*2] ^= 1
				}
				if mode == "identity" {
					data[16] ^= 1
				}
				if mode == "sequence" {
					data[headerSize+8] ^= 1
				}
				if err == nil {
					err = os.WriteFile(path, data, 0600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			l, err = Open(dir, opts)
			if mode == "crc" || mode == "identity" || mode == "sequence" {
				if err == nil {
					l.Close()
					t.Fatal("changed corrupt segment accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			if mode == "valid" && (scans.Load() != 0 || indexed.Load() != int32(len(paths))) {
				t.Fatalf("scans=%d indexed=%d", scans.Load(), indexed.Load())
			}
			if mode != "valid" && scans.Load() != 1 {
				t.Fatalf("scans=%d", scans.Load())
			}
			var n uint64
			if err = l.Replay(0, 0, func(r Record) error {
				n++
				if r.Cursor != n || r.Clock != int64(n-1) || !bytes.Equal(r.Payload, bytes.Repeat([]byte{byte(n - 1)}, 16)) {
					t.Fatalf("changed record %+v", r)
				}
				return nil
			}); err != nil || n != 12 {
				t.Fatal(n, err)
			}
		})
	}
}

func TestIndexedPayloadStillValidatedBeforeCallback(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Streams: 1}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = l.Append(0, 17, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "*.plog"))
	path := paths[0]
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[headerSize*2] ^= 1
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	// Simulate media corruption invisible to stat by rebinding the test sidecar
	// to the changed stat. Even a trusted sidecar never bypasses payload CRC.
	raw, err := os.ReadFile(path + ".index")
	if err != nil {
		t.Fatal(err)
	}
	var idx segmentIndex
	if err = json.Unmarshal(raw[4:], &idx); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	idx.Stamp, _ = fileStamp(info)
	body, _ := json.Marshal(idx)
	raw = binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, checksum))
	raw = append(raw, body...)
	if err = os.WriteFile(path+".index", raw, 0600); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	n := 0
	err = l.Replay(0, 0, func(Record) error { n++; return nil })
	if err == nil || n != 0 {
		t.Fatalf("callbacks=%d err=%v", n, err)
	}
}
