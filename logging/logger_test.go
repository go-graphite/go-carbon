package logging

import (
	"bytes"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestSetLevel(t *testing.T) {
	assert := assert.New(t)

	table := []*struct {
		level       int
		levelString string
		checkString string
		writer      func(args ...interface{})
	}{
		{0, "debug", "_DebugMessage_", logrus.Debug},
		{1, "info", "_InfoMessage_", logrus.Info},
		{2, "warning", "_WarningMessage_", logrus.Warning},
		{2, "warn", "_WarnMessage_", logrus.Warn},
		{3, "error", "_ErrorMessage_", logrus.Error},
	}

	callLoggers := func() {
		for i := 0; i < len(table); i++ {
			table[i].writer(table[i].checkString)
		}
	}

	originalLevel := logrus.GetLevel()
	defer logrus.SetLevel(originalLevel)

	for testIndex := 0; testIndex < len(table); testIndex++ {
		err := SetLevel(table[testIndex].levelString)
		assert.NoError(err)

		checkLevel := table[testIndex].level

		Test(func(log *bytes.Buffer) {
			callLoggers()

			for i := 0; i < len(table); i++ {
				if table[i].level < checkLevel {
					assert.NotContains(log.String(), table[i].checkString)
				} else {
					assert.Contains(log.String(), table[i].checkString)
				}
			}
		})
	}

	err := SetLevel("unknown")
	assert.Error(err)
}
