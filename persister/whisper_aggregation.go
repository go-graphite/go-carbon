package persister

/*
Schemas read code from https://github.com/grobian/carbonwriter/
*/

import (
	"fmt"
	"regexp"
	"strconv"

	whisper "github.com/go-graphite/go-whisper"
)

type whisperAggregationItem struct {
	name                 string
	pattern              *regexp.Regexp
	xFilesFactor         float64
	aggregationMethodStr string
	aggregationMethod    whisper.AggregationMethod
}

// WhisperAggregation ...
type WhisperAggregation struct {
	Data    []*whisperAggregationItem
	Default *whisperAggregationItem
}

// NewWhisperAggregation create instance of WhisperAggregation
func NewWhisperAggregation() *WhisperAggregation {
	return &WhisperAggregation{
		Data: make([]*whisperAggregationItem, 0),
		Default: &whisperAggregationItem{
			name:                 "default",
			pattern:              nil,
			xFilesFactor:         0.5,
			aggregationMethodStr: "average",
			aggregationMethod:    whisper.Average,
		},
	}
}

// ReadWhisperAggregation ...
func ReadWhisperAggregation(filename string) (*WhisperAggregation, error) {
	config, err := parseIniFile(filename)
	if err != nil {
		return nil, err
	}

	result := NewWhisperAggregation()

	for _, section := range config {
		item := &whisperAggregationItem{}
		// this is mildly stupid, but I don't feel like forking
		// configparser just for this
		item.name = section["name"]

		item.pattern, err = regexp.Compile(section["pattern"])
		if err != nil {
			return nil, fmt.Errorf("failed to parse pattern %#v for [%s]: %s",
				section["pattern"], item.name, err.Error())
		}

		item.xFilesFactor, err = strconv.ParseFloat(section["xfilesfactor"], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse xFilesFactor %#v in %s: %s",
				section["xfilesfactor"], item.name, err.Error())
		}

		item.aggregationMethodStr = section["aggregationmethod"]
		switch item.aggregationMethodStr {
		case "average", "avg":
			item.aggregationMethod = whisper.Average
		case "sum":
			item.aggregationMethod = whisper.Sum
		case "last":
			item.aggregationMethod = whisper.Last
		case "max":
			item.aggregationMethod = whisper.Max
		case "min":
			item.aggregationMethod = whisper.Min
		default:
			return nil, fmt.Errorf("unknown aggregation method '%s'",
				section["aggregationmethod"])
		}

		result.Data = append(result.Data, item)
	}

	return result, nil
}

// Match find schema for metric
func (a *WhisperAggregation) match(metric string) *whisperAggregationItem {
	for _, s := range a.Data {
		if s.pattern.MatchString(metric) {
			return s
		}
	}
	return a.Default
}
