package v2placement

import "fmt"

// AuthorizePartRetirement admits deletion only on the moved-away source after
// its own durable fence and successor adoption. A valid remote certificate by
// itself does not authorize this local store to delete its parts. The caller
// must additionally bind selected part IDs to the certified source inventory.
func (a *Authority) AuthorizePartRetirement(activation Activation) error {
	a.control.Lock()
	defer a.control.Unlock()
	state := a.current.Load()
	f := activation.Fence
	if state.blocked || f.Group != a.group || f.Replica != a.replica || f.Store != a.store || state.Fence == nil || state.Fence.Digest() != f.Digest() {
		return fmt.Errorf("part retirement requires this source's durable fence and adoption")
	}
	current, err := state.Document.Lookup(a.group, a.replica)
	if err != nil {
		return err
	}
	if current.Writer == a.store || current.Epoch != activation.Coverage.Epoch || state.Document.Digest() != activation.Coverage.MapDigest {
		return fmt.Errorf("part retirement requires adopted successor ownership")
	}
	for _, installed := range state.Activations {
		if installed.Fence.Digest() == f.Digest() && installed.Coverage.Digest() == activation.Coverage.Digest() {
			return nil
		}
	}
	return fmt.Errorf("part retirement activation is not durably installed")
}
