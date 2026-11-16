package v2replay

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestTornTailEveryByte(t *testing.T) {
	record := func(s string) []byte { return append(binary.LittleEndian.AppendUint32(nil, uint32(len(s))), s...) }
	first, second := record("complete"), record("second-payload")
	for cut := 0; cut < len(second); cut++ {
		t.Run(string(rune('a'+cut)), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "log")
			data := append(append([]byte{}, first...), second[:cut]...)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			calls := 0
			offset, err := Scan(path, true, func(b []byte) error {
				calls++
				if string(b) != "complete" {
					t.Fatal("changed prefix")
				}
				return nil
			})
			if err != nil || offset != int64(len(first)) || calls != 1 {
				t.Fatal(offset, calls, err)
			}
			st, _ := os.Stat(path)
			if st.Size() != int64(len(first)) {
				t.Fatal("torn tail retained")
			}
		})
	}
}
func TestRejectCorruptionAndOldTornTail(t *testing.T) {
	for _, kind := range []string{"payload", "length", "old-tail"} {
		t.Run(kind, func(t *testing.T) {
			data := append(binary.LittleEndian.AppendUint32(nil, 3), []byte("bad")...)
			if kind == "length" {
				binary.LittleEndian.PutUint32(data, MaxRecord+1)
			}
			if kind == "old-tail" {
				data = data[:5]
			}
			path := filepath.Join(t.TempDir(), "log")
			_ = os.WriteFile(path, data, 0600)
			if _, err := Scan(path, kind != "old-tail", func([]byte) error { return errors.New("checksum mismatch") }); err == nil {
				t.Fatal("corruption accepted")
			}
			st, _ := os.Stat(path)
			if st.Size() != int64(len(data)) {
				t.Fatal("corrupt file truncated")
			}
		})
	}
}
