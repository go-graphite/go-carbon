package v2convert

import (
	"errors"
	"io"
	"os"
)

// partFile is a raw sstable.Writable over a file for fixtures; production
// output uses v2part.Writer.
type partFile struct{ f *os.File }

func (p *partFile) Write(b []byte) error {
	n, e := p.f.Write(b)
	if e == nil && n != len(b) {
		e = io.ErrShortWrite
	}
	return e
}
func (p *partFile) Finish() error { return errors.Join(p.f.Sync(), p.f.Close()) }
func (p *partFile) Abort()        { p.f.Close() }
