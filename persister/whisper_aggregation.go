package persister

/*
Schemas read code from https://github.com/grobian/carbonwriter/
*/

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	whisper "github.com/go-graphite/go-whisper"
	"github.com/lomik/go-carbon/helper"
)

// WhisperAggregationItem ...
type WhisperAggregationItem struct {
	name                 string
	pattern              *regexp.Regexp
	xFilesFactor         float64
	aggregationMethodStr string
	aggregationMethod    whisper.AggregationMethod
	mixAggregationSpecs  []whisper.MixAggregationSpec
}

// WhisperAggregation ...
type WhisperAggregation struct {
	Data    []*WhisperAggregationItem
	Default *WhisperAggregationItem
}

// NewWhisperAggregation create instance of WhisperAggregation
func NewWhisperAggregation() *WhisperAggregation {
	return &WhisperAggregation{
		Data: make([]*WhisperAggregationItem, 0),
		Default: &WhisperAggregationItem{
			name:                 "default",
			pattern:              nil,
			xFilesFactor:         0.5,
			aggregationMethodStr: "average",
			aggregationMethod:    whisper.Average,
		},
	}
}

// Name get name of aggregation method
func (wai *WhisperAggregationItem) Name() string {
	return wai.name
}

// Pattern get aggregation pattern
func (wai *WhisperAggregationItem) Pattern() *regexp.Regexp {
	return wai.pattern
}

// XFilesFactor get aggregation xFilesFactor
func (wai *WhisperAggregationItem) XFilesFactor() float64 {
	return wai.xFilesFactor
}

// AggregationMethod get aggregation method
func (wai *WhisperAggregationItem) AggregationMethod() whisper.AggregationMethod {
	return wai.aggregationMethod
}

// ReadWhisperAggregation ...
func ReadWhisperAggregation(filename string) (*WhisperAggregation, error) {
	config, err := parseIniFile(filename)
	if err != nil {
		return nil, err
	}

	result := NewWhisperAggregation()

	for _, section := range config {
		item := &WhisperAggregationItem{}
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

		var err error
		item.aggregationMethodStr = section["aggregationmethod"]
		if strings.Contains(item.aggregationMethodStr, ",") {
			item.aggregationMethod = whisper.Mix
			specStrs := strings.Split(item.aggregationMethodStr, ",")
			for _, specStr := range specStrs {
				var spec whisper.MixAggregationSpec
				spec.Method, spec.Percentile, err = helper.ParseAggregationMethod(specStr)
				if err != nil {
					break
				}
				item.mixAggregationSpecs = append(item.mixAggregationSpecs, spec)
			}
		} else {
			item.aggregationMethod, _, err = helper.ParseAggregationMethod(item.aggregationMethodStr)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse aggregation method '%s': %s", section["aggregationmethod"], err)
		}

		result.Data = append(result.Data, item)
	}

	return result, nil
}

// Match find schema for metric
func (a *WhisperAggregation) Match(metric string) *WhisperAggregationItem {
	for _, s := range a.Data {
		if s.pattern.MatchString(metric) {
			return s
		}
	}
	return a.Default
}
