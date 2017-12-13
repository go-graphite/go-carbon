package tags

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/go-carbon/helper/qa"
)

func TestQueue(t *testing.T) {
	qa.Root(t, func(dir string) {
		assert := assert.New(t)

		buf := make(chan string, 100)

		q, err := NewQueue(dir, func(series []string) error {
			for i := 0; i < len(series); i++ {
				buf <- series[i]
			}
			return nil
		}, 1)
		assert.NoError(err)
		assert.NotNil(q)

		defer q.Stop()

		q.Add("hello.world;key=value")
		q.Add("hello.world;key=value2")

		assert.Equal("hello.world;key=value", <-buf)
		assert.Equal("hello.world;key=value2", <-buf)
	})
}

func TestQueueLag(t *testing.T) {
	qa.Root(t, func(dir string) {
		assert := assert.New(t)

		exit := make(chan bool, 0)

		q, err := NewQueue(dir, func(series []string) error {
			<-exit
			return nil
		}, 1)
		assert.NoError(err)
		assert.NotNil(q)

		defer q.Stop()

		q.Add("hello.world;key=value")

		assert.True(q.Lag() < time.Second)
		close(exit)
	})
}
