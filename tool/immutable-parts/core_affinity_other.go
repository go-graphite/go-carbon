//go:build !linux

package main

import "errors"

func pinCoreThread(int) (func(), error) {
	return nil, errors.New("core thread affinity requires Linux")
}
