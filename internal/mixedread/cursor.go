package mixedread

import (
	"bytes"
	"errors"
)

// Cursor routes a verified, durable name prefix exclusively to v2. Before is
// an exclusive bytewise bound. Its JSON []byte encoding preserves even names
// containing invalid UTF-8; a JSON string would silently replace their bytes.
// Done means every name is converted. It is separate from an empty bound, which
// means none are converted. The caller persists and publishes cursor advancement
// only after target verification/durability, and drains old readers before any
// source reclamation. Cursor alone does not grant permission to delete data.
type Cursor struct {
	Version int    `json:"version"`
	Before  []byte `json:"before_base64,omitempty"`
	Done    bool   `json:"done,omitempty"`
}

func (c Cursor) Validate() error {
	if c.Version != 1 {
		return errors.New("unsupported mixed reader cursor version")
	}
	if c.Done && len(c.Before) != 0 {
		return errors.New("completed mixed reader cursor has a name bound")
	}
	return nil
}
func (c Cursor) Suppresses(name string) bool {
	return c.Done || bytes.Compare([]byte(name), c.Before) < 0
}

// Filter validates and freezes a cursor for Options.SuppressLegacy.
func (c Cursor) Filter() (func(string) bool, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	bound := string(c.Before)
	done := c.Done
	return func(name string) bool { return done || name < bound }, nil
}
