package zapwriter

import (
	"bytes"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _testBuffer testBuffer

type testBuffer struct {
	bytes.Buffer
	mu sync.Mutex
}

func (b *testBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *testBuffer) Sync() error {
	return nil
}

func (b *testBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func (b *testBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Buffer.Reset()
}

// Capture = String + Reset
func (b *testBuffer) Capture() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.Buffer.String()
	b.Buffer.Reset()
	return out
}

func Test() func() {
	cfg := NewConfig()
	return testWithConfig(cfg)
}

func testWithConfig(cfg Config) func() {
	encoder, _, _ := cfg.encoder()

	logger := zap.New(
		zapcore.NewCore(
			encoder,
			&_testBuffer,
			zapcore.DebugLevel,
		),
	)

	m := &manager{
		writers: make(map[string]WriteSyncer),
		cores:   make(map[string][]zapcore.Core),
		loggers: make(map[string]*zap.Logger),
	}

	m.loggers[""] = logger

	_testBuffer.Reset()
	prev := replaceGlobalManager(m)

	return func() {
		replaceGlobalManager(prev)
	}
}

func TestCapture() string {
	return _testBuffer.Capture()
}

func TestString() string {
	return _testBuffer.String()
}
