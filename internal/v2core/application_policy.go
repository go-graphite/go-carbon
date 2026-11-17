package v2core

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// ApplicationPolicy is a validated immutable catalog definition. Its fields
// are private so callers cannot change routing after registration.
type ApplicationPolicy struct {
	policy []byte
	vshard uint16
}

func NewApplicationPolicy(name string, encoded []byte) (*ApplicationPolicy, error) {
	var p RollupPolicy
	if err := json.Unmarshal(encoded, &p); err != nil {
		return nil, err
	}
	canonical, err := EncodeRollupPolicy(p)
	if err != nil {
		return nil, err
	}
	if string(p.Name) != name || !bytes.Equal(canonical, encoded) {
		return nil, errors.New("invalid application identity/policy")
	}
	return &ApplicationPolicy{policy: canonical, vshard: partsformat.Route(p.Name)}, nil
}
