package qa

import (
	"bytes"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type buffer struct {
	bytes.Buffer
	mu sync.Mutex
}

func (b *buffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *buffer) Sync() error {
	return nil
}

func (b *buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

// Logger creates new test logger
func Logger() (*zap.Logger, func() string) {
	var buf buffer

	logger := zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(zapcore.EncoderConfig{
				MessageKey:     "message",
				LevelKey:       "level",
				TimeKey:        "time",
				NameKey:        "name",
				CallerKey:      "caller",
				StacktraceKey:  "stacktrace",
				EncodeLevel:    zapcore.CapitalLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.SecondsDurationEncoder,
			}),
			&buf,
			zapcore.DebugLevel,
		),
	)

	return logger, buf.String
}
