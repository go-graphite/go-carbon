//go:build !linux

package v2core

import "errors"

func pinCoreThread(int) (func(), error) {
	return nil, errors.New("core thread affinity requires Linux")
}
