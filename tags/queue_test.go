package tags

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/go-carbon/helper/qa"
)

func TestQueue(t *testing.T) {
	qa.Root(t, func(dir string) {
		assert := assert.New(t)

		buf := make(chan string, 100)

		q, err := NewQueue(dir, func(metric string) error {
			buf <- metric
			return nil
		})
		assert.NoError(err)
		assert.NotNil(q)

		defer q.Stop()

		q.Add("hello.world;key=value")
		q.Add("hello.world;key=value2")

		assert.Equal("hello.world;key=value", <-buf)
		assert.Equal("hello.world;key=value2", <-buf)
	})
}
