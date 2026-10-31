package tcp

import (
	"sync/atomic"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/prometheus/client_golang/prometheus"
)

type zstdStats struct {
	connections, frames, compressedBytes, decompressedBytes, decodeErrors uint64
	prom                                                                  [5]prometheus.Counter
}

func (s *zstdStats) stat(send helper.StatCallback) {
	counters := []*uint64{&s.connections, &s.frames, &s.compressedBytes, &s.decompressedBytes, &s.decodeErrors}
	names := []string{"zstdConnections", "zstdFrames", "zstdCompressedBytes", "zstdDecompressedBytes", "zstdDecodeErrors"}
	for i, counter := range counters {
		n := float64(atomic.SwapUint64(counter, 0))
		send(names[i], n)
		if s.prom[i] != nil {
			s.prom[i].Add(n)
		}
	}
}

func (s *zstdStats) initPrometheus(reg prometheus.Registerer) {
	names := []string{"connections", "frames", "compressed_bytes", "decompressed_bytes", "decode_errors"}
	for i, name := range names {
		s.prom[i] = prometheus.NewCounter(prometheus.CounterOpts{Name: "tcp_zstd_" + name + "_total", Help: "Zstd TCP input " + name + "."})
		reg.MustRegister(s.prom[i])
	}
}
