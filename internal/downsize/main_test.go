package downsize

import (
	"context"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Fixtures use made-up host names; they must not hit real DNS.
	lookupHost = func(_ context.Context, host string) ([]string, error) { return []string{host}, nil }
	os.Exit(m.Run())
}
