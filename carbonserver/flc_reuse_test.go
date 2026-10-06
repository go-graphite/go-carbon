package carbonserver

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestFileListCacheV2ReusePreservesRecords pins the disk format and path ownership.
func TestFileListCacheV2ReusePreservesRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "files.gz")
	want := []FLCEntry{
		{Path: "/a/b.wsp", LogicalSize: 10, PhysicalSize: 20, DataPoints: 30, FirstSeenAt: 40},
		{Path: "/" + strings.Repeat("long", 1000) + ".wsp", LogicalSize: 50, PhysicalSize: 60, DataPoints: 70, FirstSeenAt: 80},
		{Path: "/short.wsp", LogicalSize: 90, PhysicalSize: 100, DataPoints: 110, FirstSeenAt: -1},
	}
	writer, err := NewFileListCache(path, FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}
	var oracle bytes.Buffer
	oracle.WriteString(version2MagicString)
	for i := range want {
		if err := writer.Write(&want[i]); err != nil {
			t.Fatal(err)
		}
		// Independent encoding oracle pins the existing on-disk format.
		_ = binary.Write(&oracle, binary.BigEndian, uint64(len(want[i].Path)))
		oracle.WriteString(want[i].Path)
		for _, value := range []int64{want[i].LogicalSize, want[i].PhysicalSize, want[i].DataPoints, want[i].FirstSeenAt} {
			_ = binary.Write(&oracle, binary.BigEndian, uint64(value))
		}
		oracle.WriteByte('\n')
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	compressed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(z)
	z.Close()
	if err != nil || !bytes.Equal(got, oracle.Bytes()) {
		t.Fatalf("cache encoding changed: %v", err)
	}
	reader, err := NewFileListCache(path, FLCVersion2, 'r')
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var entry FLCEntry
	var entries []FLCEntry
	for range want {
		if err := reader.(*fileListCacheV2).readInto(&entry); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatal("reusing the decode buffer changed previously returned entries")
	}
	if err := reader.(*fileListCacheV2).readInto(&entry); !errors.Is(err, io.EOF) {
		t.Fatalf("expected clean EOF, got %v", err)
	}
}

// TestFileListCacheV2RejectsIncompleteRecord rejects partial and malformed entries.
func TestFileListCacheV2RejectsIncompleteRecord(t *testing.T) {
	var record bytes.Buffer
	_ = binary.Write(&record, binary.BigEndian, uint64(5))
	record.WriteString("a.wsp")
	record.Write(make([]byte, 32))
	record.WriteByte('\n')
	for cut := 1; cut < record.Len(); cut++ {
		assertInvalidFLCRecord(t, record.Bytes()[:cut])
	}
	badSeparator := append([]byte(nil), record.Bytes()...)
	badSeparator[len(badSeparator)-1] = 0
	assertInvalidFLCRecord(t, badSeparator)
	for _, length := range []uint64{8193, 1 << 63, ^uint64(0)} {
		var header [8]byte
		binary.BigEndian.PutUint64(header[:], length)
		assertInvalidFLCRecord(t, header[:])
	}
}

func assertInvalidFLCRecord(t *testing.T, record []byte) {
	t.Helper()
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	_, _ = z.Write([]byte(version2MagicString))
	_, _ = z.Write(record)
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "invalid.gz")
	if err := os.WriteFile(path, compressed.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	reader, err := NewFileListCache(path, FLCVersion2, 'r')
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Read(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("record of %d bytes accepted as complete/clean EOF: %v", len(record), err)
	}
}
