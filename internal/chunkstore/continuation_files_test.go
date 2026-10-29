package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	whisper "github.com/go-graphite/go-whisper"
)

func TestContinuationMemoryFilesMatchOS(t *testing.T) {
	t.Parallel()
	fs := &continuationMemoryFiles{files: make(map[string]*continuationMemoryData)}
	actual, err := fs.OpenFile("metric", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer actual.Close()
	oracle, err := os.Create(filepath.Join(t.TempDir(), "oracle"))
	if err != nil {
		t.Fatal(err)
	}
	defer oracle.Close()
	for step, op := range []struct {
		offset int64
		data   []byte
		size   int64
	}{
		{offset: 4094, data: []byte("across-page-boundary")},
		{size: 32771},
		{offset: 16391, data: bytes.Repeat([]byte{0x80}, 8193)},
		{size: 4097},
		{size: 32771},
		{offset: 4095, data: []byte{0, 1, 0xff, 0}},
		{offset: 40000, data: []byte{}},
		{size: 0},
		{size: 32771},
	} {
		if op.data != nil {
			got, gotErr := actual.WriteAt(op.data, op.offset)
			want, wantErr := oracle.WriteAt(op.data, op.offset)
			if got != want || !errors.Is(gotErr, wantErr) {
				t.Fatalf("step%d write: %d/%v, want %d/%v", step, got, gotErr, want, wantErr)
			}
		} else if err := errors.Join(actual.Truncate(op.size), oracle.Truncate(op.size)); err != nil {
			t.Fatal(err)
		}
		stat, err := oracle.Stat()
		if err != nil {
			t.Fatal(err)
		}
		gotStat, err := fs.Stat("metric")
		if err != nil || gotStat.Size() != stat.Size() {
			t.Fatalf("step%d size: %v/%v, want %d", step, gotStat, err, stat.Size())
		}
		for _, offset := range []int64{0, 3, 4095, max(0, stat.Size()-2), stat.Size(), stat.Size() + 1} {
			got, want := bytes.Repeat([]byte{0xaa}, 32780), bytes.Repeat([]byte{0xaa}, 32780)
			gn, ge := actual.ReadAt(got, offset)
			wn, we := oracle.ReadAt(want, offset)
			if gn != wn || !errors.Is(ge, we) || !bytes.Equal(got, want) {
				t.Fatalf("step%d offset%d: %d/%v, want %d/%v; bytes equal=%v", step, offset, gn, ge, wn, we, bytes.Equal(got, want))
			}
		}
	}
}

func TestContinuationExtentsPreserveEveryNonzeroRecord(t *testing.T) {
	t.Parallel()
	for _, archiveStart := range []int64{0, 28, 112} {
		t.Run(fmt.Sprint(archiveStart), func(t *testing.T) {
			const records = 1024
			data := make([]byte, int(archiveStart)+records*whisper.PointSize)
			for _, record := range []int{0, 1, 340, 341, 342, 682, 683, records - 1} {
				for i := range whisper.PointSize {
					data[int(archiveStart)+record*whisper.PointSize+i] = byte(1 + (record+i)%255)
				}
			}
			fs := &continuationMemoryFiles{files: map[string]*continuationMemoryData{"metric": continuationMemoryImage(data)}, typed: true}
			memory, err := fs.OpenFile("metric", os.O_RDONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer memory.Close()
			inputs := []whisper.StorageFile{memory}
			if continuationPhysicalReadFS != nil {
				name := filepath.Join(t.TempDir(), "physical")
				if err := os.WriteFile(name, data, 0600); err != nil {
					t.Fatal(err)
				}
				physical, err := continuationPhysicalReadFS.OpenFile(name, os.O_RDONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer physical.Close()
				inputs = append(inputs, physical)
			}
			for _, input := range inputs {
				got := make([]byte, len(data))
				for offset := archiveStart; offset < int64(len(data)); {
					from, until, err := input.(whisper.StorageExtentReader).NextStorageExtent(offset, int64(len(data)), archiveStart)
					if err != nil || from < offset || until < from || until > int64(len(data)) || (from-archiveStart)%whisper.PointSize != 0 || (until-archiveStart)%whisper.PointSize != 0 {
						t.Fatalf("invalid extent %d..%d after%d: %v", from, until, offset, err)
					}
					if from == int64(len(data)) {
						break
					}
					if until == offset {
						t.Fatal("extent did not advance")
					}
					if _, err := input.ReadAt(got[from:until], from); err != nil && err != io.EOF {
						t.Fatal(err)
					}
					offset = until
				}
				if !bytes.Equal(got, data) {
					t.Fatal("extent walk omitted nonzero bytes or read partial records")
				}
			}
		})
	}
}
