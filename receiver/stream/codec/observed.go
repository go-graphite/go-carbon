package codec

// AddObserved reports definitions and automatic resets for metrics.
func (e *Encoder) AddObserved(name []byte, value float64, ts int64) (defined, reset bool, err error) {
	return e.AddHashed(name, nameHash(name), value, ts)
}
