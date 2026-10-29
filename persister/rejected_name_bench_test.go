package persister

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSharedRejectedNameSkipsPolicyMatching(t *testing.T) {
	// Nil patterns would panic if either policy matcher were reached.
	p := NewWhisper(t.TempDir(), WhisperSchemas{{}}, &WhisperAggregation{Data: []*WhisperAggregationItem{{}}}, nil, nil, nil, nil)
	name := "general.flink." + strings.Repeat("x", 300) + ".count"
	if err := p.createShared(context.Background(), name); !errors.Is(err, store.ErrNameTooLong) {
		t.Fatal(err)
	}
}

func BenchmarkPerCallRejectedName(b *testing.B) {
	name := "general.flink." + strings.Repeat("x", 300) + ".count"
	schemas := WhisperSchemas{{Pattern: regexp.MustCompile(`.*secondly.*`)}, {Pattern: regexp.MustCompile(`.*ten_secondly.*`)}, {Pattern: regexp.MustCompile(`.*`)}}
	aggregation := NewWhisperAggregation()
	for _, pattern := range []string{`.*\.sum$`, `.*\.min$`, `.*\.max$`, `.*\.last$`} {
		aggregation.Data = append(aggregation.Data, &WhisperAggregationItem{pattern: regexp.MustCompile(pattern)})
	}
	p := NewWhisper(b.TempDir(), schemas, aggregation, nil, nil, nil, nil)
	for _, legacy := range []bool{true, false} {
		label := "validated"
		if legacy {
			label = "original"
		}
		b.Run(label, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if legacy {
					if _, ok := schemas.Match(name); !ok {
						b.Fatal("no schema")
					}
					if aggregation.Match(name) == nil {
						b.Fatal("no aggregation")
					}
					if !errors.Is(store.ValidateMetricName(name), store.ErrNameTooLong) {
						b.Fatal("name accepted")
					}
				} else if !errors.Is(p.createShared(context.Background(), name), store.ErrNameTooLong) {
					b.Fatal("name accepted")
				}
			}
		})
	}
}
