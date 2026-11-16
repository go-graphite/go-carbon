package partlog

// PredictorV3Accounting counts serialized bytes before joint zstd compression.
// Fields add exactly to the raw payload; Headers adds PDC3 and outer journal
// headers. Encoded is the actual post-compression size including those headers.
// Joint compression cannot assign unique compressed bytes to individual fields.
type PredictorV3Accounting struct {
	Points, Names, IDs, Timestamps, Values, Clocks, Framing, Definitions, Headers, Encoded int64
}

func (a *PredictorV3Accounting) add(b PredictorV3Accounting) {
	a.Points += b.Points
	a.Names += b.Names
	a.IDs += b.IDs
	a.Timestamps += b.Timestamps
	a.Values += b.Values
	a.Clocks += b.Clocks
	a.Framing += b.Framing
	a.Definitions += b.Definitions
	a.Headers += b.Headers
	a.Encoded += b.Encoded
}
