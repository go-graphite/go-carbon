package chunkstore

// CaptureIdentity binds a retained original tree to one conversion destination.
type CaptureIdentity struct {
	Source, Copy, SourceID, Destination string
	Live                                bool
}

// CaptureReceipt records full verification or an explicit operator assertion of
// a completed, fully verified baseline. Partial conversions cannot assert it.
type CaptureReceipt struct {
	Version      int
	Identity     CaptureIdentity
	Verification string
	// BaselineMaxMetricID is the immutable catalog ceiling at full baseline
	// verification, before any mirrored writer. Zero permits only first use.
	BaselineMaxMetricID uint64 `json:",omitempty"`
}
