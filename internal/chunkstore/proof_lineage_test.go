package chunkstore

import (
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

type proofFaultFS struct {
	vfs.FS
	phase string
	armed bool
}
type proofFaultFile struct {
	vfs.File
	fail bool
}

func (f proofFaultFile) Sync() error {
	if f.fail {
		return errors.New("interrupted proof sync")
	}
	return f.File.Sync()
}
func (f *proofFaultFS) Create(path string) (vfs.File, error) {
	if strings.Contains(path, "PROOF-") {
		f.armed = true
	}
	v, e := f.FS.Create(path)
	if e != nil {
		return nil, e
	}
	return proofFaultFile{v, f.phase == "file_sync" && strings.HasSuffix(path, "PROOF-LINEAGE.tmp")}, nil
}
func (f *proofFaultFS) OpenDir(path string) (vfs.File, error) {
	v, e := f.FS.OpenDir(path)
	if e != nil {
		return nil, e
	}
	return proofFaultFile{v, f.phase == "dir_sync" && f.armed}, nil
}
func (f *proofFaultFS) Rename(old, new string) error {
	if f.phase == "rename" && strings.HasSuffix(new, "PROOF-LINEAGE") {
		return errors.New("interrupted proof rename")
	}
	return f.FS.Rename(old, new)
}
func TestProofLineageInterruptedPublish(t *testing.T) {
	for _, phase := range []string{"file_sync", "rename", "dir_sync"} {
		t.Run(phase, func(t *testing.T) {
			fs := &proofFaultFS{FS: vfs.NewMem()}
			o := Options{ExactBlocks: true}.WithFS(fs)
			s, e := Open("/store", o)
			if e != nil {
				t.Fatal(e)
			}
			seedProof(t, s)
			before, _ := windowProof(t, s)
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			fs.armed = false
			fs.phase = phase
			if failed, e := Open("/store", o); e == nil {
				failed.Close()
				t.Fatal("interrupted publication succeeded")
			}
			fs.phase = ""
			s, e = Open("/store", o)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			after, _ := windowProof(t, s)
			if before == after {
				t.Fatal("interrupted process publication retained old proof")
			}
		})
	}
}
