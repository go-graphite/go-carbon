package persister

/*
Schemas read code from https://github.com/grobian/carbonwriter/
*/

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/alyu/configparser"
	"github.com/lomik/go-whisper"
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
func ReadWhisperAggregation(file string) (*WhisperAggregation, error) {
	config, err := configparser.Read(file)
	if err != nil {
		return nil, err
	}
	// pp.Println(config)
	sections, err := config.AllSections()
	if err != nil {
		return nil, err
	}

	result := NewWhisperAggregation()

	for _, s := range sections {
		item := &whisperAggregationItem{}
		// this is mildly stupid, but I don't feel like forking
		// configparser just for this
		item.name =
			strings.Trim(strings.SplitN(s.String(), "\n", 2)[0], " []")
		if item.name == "" || strings.HasPrefix(item.name, "#") {
			continue
		}

		item.pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			logrus.Errorf("[persister] Failed to parse pattern '%s'for [%s]: %s",
				s.ValueOf("pattern"), item.name, err.Error())
			return nil, err
		}

		item.xFilesFactor, err = strconv.ParseFloat(s.ValueOf("xFilesFactor"), 64)
		if err != nil {
			logrus.Errorf("failed to parse xFilesFactor '%s' in %s: %s",
				s.ValueOf("xFilesFactor"), item.name, err.Error())
			continue
		}

		item.aggregationMethodStr = s.ValueOf("aggregationMethod")
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
			logrus.Errorf("unknown aggregation method '%s'",
				s.ValueOf("aggregationMethod"))
			continue
		}

		logrus.Debugf("[persister] Adding aggregation [%s] pattern = %s aggregationMethod = %s xFilesFactor = %f",
			item.name, s.ValueOf("pattern"),
			item.aggregationMethodStr, item.xFilesFactor)

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
