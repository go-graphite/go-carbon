package carbon

import (
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func coldPackOptions(cfg *Config) chunkstore.ColdPackOptions {
	w := cfg.Whisper
	o := chunkstore.ColdPackOptions{Mode: w.StoreColdPack, MinFreeBytes: w.StoreColdPackMinFree, BytesPerSecond: w.StoreColdPackRate, CPUFraction: w.StoreColdPackCPU}
	if w.StoreColdPackWindow != nil {
		o.WriteWindow = w.StoreColdPackWindow.Value()
	}
	if w.StoreColdPackInterval != nil {
		o.Interval = w.StoreColdPackInterval.Value()
	}
	return o
}
