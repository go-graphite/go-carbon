package qa

import (
	"os"
	"testing"
)

// Root creates new test directory
func Root(t *testing.T, callback func(dir string)) {
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Fatal(err)
		}
	}()

	callback(tmpDir)
}
