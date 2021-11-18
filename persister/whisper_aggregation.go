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

// WhisperAggregationItem ...
type WhisperAggregationItem struct {
	name                 string
	pattern              *regexp.Regexp
	xFilesFactor         float64
	aggregationMethodStr string
	aggregationMethod    whisper.AggregationMethod

	// If not specified, migration flag is controlled by the global
	// migration flag set in persister.Whisper
	xffMigration         *bool
	aggregationMigration *bool
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
			return nil, fmt.Errorf("unknown aggregation method '%s' in section '%s'",
				section["aggregationmethod"], section["name"])
		}

		if xffm, ok := section["xffMigration"]; ok {
			migration := xffm == "true"
			item.xffMigration = &migration
		}
		if aggrm, ok := section["aggregationMethodMigration"]; ok {
			migration := aggrm == "true"
			item.aggregationMigration = &migration
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

func (a *WhisperAggregationItem) canMigrateXff(global bool) bool {
	if a.xffMigration != nil {
		return *a.xffMigration
	}

	return global
}

func (a *WhisperAggregationItem) canMigrateAggregationMethod(global bool) bool {
	if a.aggregationMigration != nil {
		return *a.aggregationMigration
	}

	return global
}
