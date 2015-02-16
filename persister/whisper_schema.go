package persister

/*
Schemas read code from https://github.com/grobian/carbonwriter/
*/

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/alyu/configparser"
	"github.com/lomik/go-whisper"
)

type whisperSchemaItem struct {
	name         string
	pattern      *regexp.Regexp
	retentionStr string
	retentions   whisper.Retentions
	priority     int64
}

type whisperSchemaItemByPriority []*whisperSchemaItem

func (v whisperSchemaItemByPriority) Len() int           { return len(v) }
func (v whisperSchemaItemByPriority) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v whisperSchemaItemByPriority) Less(i, j int) bool { return v[i].priority >= v[j].priority }

// WhisperSchemas ...
type WhisperSchemas struct {
	Data []*whisperSchemaItem
}

// ParseRetentionDefs copy of original ParseRetentionDefs from go-whisper
// With support where old format:
//   secondsPerPoint:numberOfPoints
func ParseRetentionDefs(retentionDefs string) (whisper.Retentions, error) {
	retentions := make(whisper.Retentions, 0)
	for _, retentionDef := range strings.Split(retentionDefs, ",") {
		// check if old format
		row := strings.Split(retentionDef, ":")
		if len(row) == 2 {
			val1, err1 := strconv.ParseInt(row[0], 10, 0)
			val2, err2 := strconv.ParseInt(row[1], 10, 0)

			if err1 == nil && err2 == nil {
				retentionDef = fmt.Sprintf("%d:%d", val1, val1*val2)
			}
		}

		// new format
		retention, err := whisper.ParseRetentionDef(retentionDef)
		if err != nil {
			return nil, err
		}
		retentions = append(retentions, retention)
	}
	return retentions, nil
}

// NewWhisperSchemas create instance of WhisperSchemas
func NewWhisperSchemas() *WhisperSchemas {
	return &WhisperSchemas{
		Data: make([]*whisperSchemaItem, 0),
	}
}

// ReadWhisperSchemas ...
func ReadWhisperSchemas(file string) (*WhisperSchemas, error) {
	config, err := configparser.Read(file)
	if err != nil {
		return nil, err
	}
	// pp.Println(config)
	sections, err := config.AllSections()
	if err != nil {
		return nil, err
	}

	result := NewWhisperSchemas()

	for index, s := range sections {
		item := &whisperSchemaItem{}
		// this is mildly stupid, but I don't feel like forking
		// configparser just for this
		item.name =
			strings.Trim(strings.SplitN(s.String(), "\n", 2)[0], " []")
		if item.name == "" {
			continue
		}
		item.pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			logrus.Errorf("[persister] Failed to parse pattern '%s'for [%s]: %s",
				s.ValueOf("pattern"), item.name, err.Error())
			return nil, err
		}
		item.retentionStr = s.ValueOf("retentions")
		item.retentions, err = ParseRetentionDefs(item.retentionStr)

		priorityStr := s.ValueOf("priority")

		var p int64
		if priorityStr == "" {
			p = 0
		} else {
			p, err = strconv.ParseInt(priorityStr, 10, 0)
			if err != nil {
				logrus.Errorf("[persister] wrong priority in schema: %s", err)
				return nil, err
			}
		}
		item.priority = int64(p)<<32 - int64(index) // for sort records with same priority
		// item.priority = (s.ValueOf("priority"))
		logrus.Debugf("[persister] Adding schema [%s] pattern = %s retentions = %s",
			item.name, s.ValueOf("pattern"), item.retentionStr)

		result.Data = append(result.Data, item)
	}

	sort.Sort(whisperSchemaItemByPriority(result.Data))

	return result, nil
}

// Match find schema for metric
func (s *WhisperSchemas) match(metric string) *whisperSchemaItem {
	for _, s := range s.Data {
		if s.pattern.MatchString(metric) {
			return s
		}
	}
	return nil
}
