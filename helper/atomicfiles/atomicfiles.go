package atomicfiles

import (
	"io/ioutil"
	"os"
	"path"
)

func WriteFile(name string, body []byte) error {
	dir, _ := path.Split(name)

	tmpFile, err := ioutil.TempFile(dir, ".tmp")
	if err != nil {
		return err
	}

	tmpName := tmpFile.Name()

	_, err = tmpFile.Write(body)
	if err != nil {
		return err
	}

	err = tmpFile.Sync()
	if err != nil {
		return err
	}

	err = tmpFile.Close()
	if err != nil {
		return err
	}

	err = os.Rename(tmpName, name)
	if err != nil {
		return err
	}

	return nil
}