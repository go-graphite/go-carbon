package carbon

import (
	"context"
	"time"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Timestamp the durability boundary independently of log collection latency.
func observeV2Admission(b daemonstore.Backend) {
	if x, ok := b.(interface {
		FirstAccepted() <-chan struct{}
		FirstAcceptedUnixNano() int64
		AdmissionDone() <-chan struct{}
	}); ok {
		go func() {
			select {
			case <-x.FirstAccepted():
				zapwriter.Logger("v2").Info("v2 first durable acceptance", zap.Int64("accepted_unix_nano", x.FirstAcceptedUnixNano()))
			case <-x.AdmissionDone():
			}
		}()
	}
	if x, ok := b.(interface{ WaitReady(context.Context) error }); ok {
		go func() {
			if e := x.WaitReady(context.Background()); e == nil {
				zapwriter.Logger("v2").Info("v2 historical recovery ready", zap.Int64("ready_unix_nano", time.Now().UnixNano()))
			}
		}()
	}
}
