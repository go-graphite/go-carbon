package persister

import (
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func chunkConfig(c storageMetricConfig) chunkstore.MetricConfig {
	result := chunkstore.MetricConfig{Name: c.Name, AggregationMethod: chunkstore.AggregationMethod(c.AggregationMethod), XFilesFactor: c.XFilesFactor}
	for _, r := range c.Retentions {
		result.Retentions = append(result.Retentions, chunkstore.Retention{Step: r.SecondsPerPoint(), Count: r.NumberOfPoints()})
	}
	return result
}
func whisperConfig(c chunkstore.MetricConfig) storageMetricConfig {
	result := storageMetricConfig{Name: c.Name, AggregationMethod: whisper.AggregationMethod(c.AggregationMethod), XFilesFactor: c.XFilesFactor}
	for _, r := range c.Retentions {
		result.Retentions = append(result.Retentions, whisper.NewRetention(r.Step, r.Count))
	}
	return result
}
