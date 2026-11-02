package codec

// HashedEncoder is the encoder with a prehashed Add API. It uses the same compact
// dictionary as Add; there is no second map or mirrored framing implementation.
type HashedEncoder = Encoder

func NewHashedEncoder(o Options) (*HashedEncoder, error) { return NewEncoder(o) }
