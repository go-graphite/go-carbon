package pyroscope

import (
	"github.com/grafana/pyroscope-go/upstream"
)

var (
	sampleTypeConfigHeap = map[string]*upstream.SampleType{ //nolint:gochecknoglobals
		"alloc_objects": {
			Units:      "objects",
			Cumulative: false,
		},
		"alloc_space": {
			Units:      "bytes",
			Cumulative: false,
		},
		"inuse_space": {
			Units:       "bytes",
			Aggregation: "average",
			Cumulative:  false,
		},
		"inuse_objects": {
			Units:       "objects",
			Aggregation: "average",
			Cumulative:  false,
		},
	}
	sampleTypeConfigMutex = map[string]*upstream.SampleType{ //nolint:gochecknoglobals
		"contentions": {
			DisplayName: "mutex_count",
			Units:       "lock_samples",
			Cumulative:  false,
		},
		"delay": {
			DisplayName: "mutex_duration",
			Units:       "lock_nanoseconds",
			Cumulative:  false,
		},
	}
	sampleTypeConfigBlock = map[string]*upstream.SampleType{ //nolint:gochecknoglobals
		"contentions": {
			DisplayName: "block_count",
			Units:       "lock_samples",
			Cumulative:  false,
		},
		"delay": {
			DisplayName: "block_duration",
			Units:       "lock_nanoseconds",
			Cumulative:  false,
		},
	}
	sampleTypeConfigGoroutineLeak = map[string]*upstream.SampleType{ //nolint:gochecknoglobals
		"goroutineleak": {
			DisplayName: "goroutine_leak",
			Units:       "goroutines",
			Aggregation: "average",
		},
	}
)
