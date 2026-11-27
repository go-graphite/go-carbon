package v2core

import (
	"bytes"
	"errors"
	"strings"

	"github.com/go-graphite/go-carbon/internal/v2dict"
)

// PolicyRecovery validates existing dictionary-backed identities in bounded
// batches. It belongs to one serial catalog open and must Finish before that
// catalog is published. It retains at most 1 MiB of name bytes plus 4096 headers.
type PolicyRecovery struct {
	engine *Engine
	names  []v2dict.NameID
	bytes  int
}

func (e *coreEngine) NewPolicyRecovery() *PolicyRecovery { return &PolicyRecovery{engine: e} }

func (r *PolicyRecovery) Bind(id uint64, name string, definition []byte) error {
	h := r.engine.identities.lookup(id)
	if h == nil || h.name.Load() != nil || h.dictID.Load() == 0 {
		_, err := r.engine.RegisterApplicationPolicy(id, name, definition)
		return err
	}
	// Rebuild from supplied bytes, never by decoding the dictionary name.
	// Exact name/ID equality is deferred to Finish, before Open returns success.
	if uint32(len(name)) != h.nameLen || !bytes.Equal(h.appendDefinitionName(nil, name), definition) {
		return errors.New("stored rollup policy changed")
	}
	if len(r.names) >= 4096 || r.bytes+len(name) > 1<<20 {
		if err := r.Finish(); err != nil {
			return err
		}
	}
	r.names = append(r.names, v2dict.NameID{Group: string(h.policy.Group), VShard: h.vshard, Name: strings.Clone(name), ID: h.dictID.Load() - 1})
	r.bytes += len(name)
	return nil
}

func (r *PolicyRecovery) Finish() error {
	if len(r.names) == 0 {
		return nil
	}
	err := r.engine.identities.partDict().CheckNames(r.names)
	clear(r.names)
	r.names = r.names[:0]
	r.bytes = 0
	return err
}
