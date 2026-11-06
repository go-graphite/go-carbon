//go:build !linux

package hostcapacity

// Missing host observations cannot authorize growth. Other platforms retain
// the original allowance rather than guessing from process CPU alone.
func readHostSample() hostSample { return hostSample{} }
