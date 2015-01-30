package carbon

// CacheValues one metric data
type CacheValues struct {
	Data []struct {
		Value     float64
		Timestamp int64
	}
}

// Append new metric value
func (values *CacheValues) Append(value float64, timestamp int64) {
	values.Data = append(values.Data, struct {
		Value     float64
		Timestamp int64
	}{
		Value:     value,
		Timestamp: timestamp,
	})
}

// Cache stores and aggregate metrics in memory
type Cache struct {
	data       map[string]*CacheValues
	size       int
	inputChan  chan *Message     // from receivers
	outputChan chan *CacheValues // to persisters
	exitChan   chan bool         // close for stop worker
}

// NewCache create Cache instance and run in/out goroutine
func NewCache() *Cache {
	cache := &Cache{
		data:       make(map[string]*CacheValues, 0),
		size:       0,
		inputChan:  make(chan *Message, 1024),
		outputChan: make(chan *CacheValues, 1024),
		exitChan:   make(chan bool, 0),
	}
	return cache
}

// Get any key/values pair from Cache
func (c Cache) Get() (string, *CacheValues) {
	for key, values := range c.data {
		return key, values
	}
	return "", nil
}

// Remove key from cache
func (c *Cache) Remove(key string) {
	if value, exists := c.data[key]; exists {
		c.size -= len(value.Data)
		delete(c.data, key)
	}
}

// Add value to cache
func (c *Cache) Add(key string, value float64, timestamp int64) {
	if values, exists := c.data[key]; exists {
		values.Append(value, timestamp)
	} else {
		values := &CacheValues{}
		values.Append(value, timestamp)
		c.data[key] = values
	}
	c.size++
}

// Size returns size
func (c *Cache) Size() int {
	return c.size
}

func (c *Cache) worker() {
	var key string
	var values *CacheValues
	var sendTo chan *CacheValues

	for {
		if values == nil {
			key, values = c.Get()

			if values != nil {
				c.Remove(key)
				sendTo = c.outputChan
			} else {
				sendTo = nil
			}
		}

		select {
		case sendTo <- values:
			values = nil
		case msg := <-c.inputChan:
			c.Add(msg.Name, msg.Value, msg.Timestamp)
		case <-c.exitChan:
			break
		}
	}

}

// Run worker
func (c *Cache) Run() {
	go c.worker()
}

// Stop worker
func (c *Cache) Stop() {
	close(c.exitChan)
}
