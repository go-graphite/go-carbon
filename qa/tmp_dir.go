package qa

import (
	"io/ioutil"
	"os"
	"testing"
)

// Root creates new test directory
func Root(t *testing.T, callback func(dir string)) {
	tmpDir, err := ioutil.TempDir("", "")
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
