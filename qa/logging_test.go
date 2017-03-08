package qa

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	logger, out := Logger()

	logger.Info("test message")

	assert.Contains(t, out(), "test message")
}
