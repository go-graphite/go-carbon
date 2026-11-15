//go:build !linux

package main

import "fmt"

func pinSnapshotReader() (func(), error) {
	return nil, fmt.Errorf("isolated read benchmark requires Linux")
}
