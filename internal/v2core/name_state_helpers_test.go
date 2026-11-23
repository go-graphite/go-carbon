package v2core

// metaOf returns the committed scalars of the metric installed under id.
func (r *coreRollups) metaOf(id uint64) metricMetadata {
	return r.meta[r.slotOf(id, r.installed(id))]
}
