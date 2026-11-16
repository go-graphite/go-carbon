//go:build !linux

package main

import "testing"

func copyEnableCold(t *testing.T) {
	t.Fatal("owned-file cold cache advice is only supported on Linux")
}
