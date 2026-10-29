//go:build !linux && !darwin

package main

import (
	"errors"
	"os"
)

func checkpointFileIdentity(os.FileInfo) (uint64, uint64, int64, error) {
	return 0, 0, 0, errors.New("verified resume checkpoints require Linux or Darwin file identities")
}
