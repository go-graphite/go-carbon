package v2core

import (
	"bytes"
	"errors"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// partKeyMagic prefixes every name-keyed (GCP3) part record key; idKeyMagic
// every ID-keyed (GCI2) one. headerCursor passes other keys
// (raw rows, catalogs) through, so it can wrap the store's own Pebble keyspace,
// where installed copied and portable parts keep their records.
var partKeyMagic = []byte{'G', 'C', 'P', 3}

// headerCursor presents a part's records as the logical stream every reader
// expects. A dense run with a folded envelope (v2part.SplitHeader) is
// followed by that envelope at its metric's envelope key, exactly where parts
// written before header-in-run store it as its own KV. Both forms read the
// same, so readers never branch on the part's age.
type headerCursor struct {
	archiveCursor
	upper    []byte // the wrapped iterator's exclusive upper bound, or nil
	run, env []byte
	key      []byte // envelope key of the current folded run
	onEnv    bool
	err      error
}

func newHeaderCursor(c archiveCursor, upper []byte) *headerCursor {
	return &headerCursor{archiveCursor: c, upper: upper}
}

// load reads the wrapped cursor's current record.
func (c *headerCursor) load(valid bool) bool {
	c.onEnv, c.env, c.run = false, nil, nil
	if !valid {
		return false
	}
	k, v := c.archiveCursor.Key(), c.archiveCursor.Value()
	c.run = v
	tail := partKeyTailOf(k)
	if tail == 0 || isEnvelopeKey(k, tail) {
		return true
	}
	c.env, c.run, c.err = v2part.SplitHeader(v)
	if c.env != nil {
		c.key = appendEnvelopeKey(c.key[:0], k, tail)
		if c.upper != nil && bytes.Compare(c.key, c.upper) >= 0 {
			c.env = nil
		}
	}
	return c.Valid()
}

func (c *headerCursor) First() bool { return c.load(c.archiveCursor.First()) }

// SeekGE lands on a folded envelope when the run before key carries one at or
// after key; the envelope sorts after all of its metric's runs. A run key
// answered by a run of the same metric needs one seek: that metric's envelope
// sorts after it. An envelope key looks back first, where a folded part has it.
func (c *headerCursor) SeekGE(key []byte) bool {
	tail := partsformat.NameKeyTail
	if bytes.HasPrefix(key, idKeyMagic) {
		tail = partsformat.IDKeyTail
	}
	if n := len(key) - tail; n >= 0 && !isEnvelopeKey(key, tail) {
		valid := c.load(c.archiveCursor.SeekGE(key))
		if k := c.archiveCursor.Key(); c.err != nil || valid && len(k) == len(key) && bytes.Equal(k[:n], key[:n]) {
			return valid
		}
		if c.archiveCursor.Error() != nil {
			return false
		}
	}
	if c.load(c.archiveCursor.SeekLT(key)) && c.env != nil && bytes.Compare(c.key, key) >= 0 {
		c.onEnv = true
		return true
	}
	if c.err != nil {
		return false
	}
	return c.load(c.archiveCursor.SeekGE(key))
}

func (c *headerCursor) SeekLT(key []byte) bool {
	if c.load(c.archiveCursor.SeekLT(key)) && c.env != nil && bytes.Compare(c.key, key) < 0 {
		c.onEnv = true
	}
	return c.Valid()
}

func (c *headerCursor) Next() bool {
	if !c.onEnv && c.env != nil && c.Valid() {
		c.onEnv = true
		return true
	}
	return c.load(c.archiveCursor.Next())
}

func (c *headerCursor) Valid() bool { return c.err == nil && c.archiveCursor.Valid() }

func (c *headerCursor) Key() []byte {
	if c.onEnv {
		return c.key
	}
	return c.archiveCursor.Key()
}

func (c *headerCursor) Value() []byte {
	if c.onEnv {
		return c.env
	}
	return c.run
}

func (c *headerCursor) Error() error { return errors.Join(c.err, c.archiveCursor.Error()) }
