package logging

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestFsnotifyRotate(t *testing.T) {
	t.SkipNow()

	assert := assert.New(t)

	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Fatal(err)
		}
	}()

	fd, err := ioutil.TempFile(tmpDir, "log")
	if err != nil {
		t.Fatal(err)
	}

	filename := fd.Name()
	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}

	if err := SetFile(filename); err != nil {
		t.Fatal(err)
	}
	defer SetFile("")

	checkExists := func(text string) {
		fd, err := os.Open(filename)
		if err != nil {
			t.Fatal(err)
		}
		defer fd.Close()

		b, err := ioutil.ReadAll(fd)
		if err != nil {
			t.Fatal(err)
		}

		assert.Contains(string(b), text)
	}

	logrus.Info("First message")
	checkExists("First message")

	move := func(index int) {
		err := os.Rename(filename, fmt.Sprintf("%s-%d", filename, index))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		move(i)
		time.Sleep(30 * time.Millisecond)

		msg := fmt.Sprintf("Message #%d.", i)
		logrus.Info(msg)
		checkExists(msg)
	}
}
