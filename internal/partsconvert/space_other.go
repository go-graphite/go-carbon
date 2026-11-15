//go:build !linux && !darwin

package partsconvert

import "errors"

type SpaceGuard struct {
	Path    string
	MinFree uint64
}

func (*SpaceGuard) Reserve(uint64) (func(), error) {
	return nil, errors.New("free-space guard unavailable on this platform")
}
func lockDirectory(string) (func(), error) {
	return nil, errors.New("conversion journal lock unavailable on this platform")
}
