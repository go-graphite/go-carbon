// this is a parser for graphite's storage-schemas.conf
// it supports old and new retention format
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
// based on https://github.com/grobian/carbonwriter but with some improvements
package persister

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/alyu/configparser"
	"github.com/lomik/go-whisper"
)

// Schema represents one schema setting
type Schema struct {
	Name         string
	Pattern      *regexp.Regexp
	RetentionStr string
	Retentions   whisper.Retentions
	Priority     int64
}

// WhisperSchemas contains schema settings
type WhisperSchemas []Schema

func (s WhisperSchemas) Len() int           { return len(s) }
func (s WhisperSchemas) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s WhisperSchemas) Less(i, j int) bool { return s[i].Priority >= s[j].Priority }

// Match finds the schema for metric or returns false if none found
func (s WhisperSchemas) Match(metric string) (Schema, bool) {
	for _, schema := range s {
		if schema.Pattern.MatchString(metric) {
			return schema, true
		}
	}
	return Schema{}, false
}

// ParseRetentionDefs parses retention definitions into a Retentions structure
func ParseRetentionDefs(retentionDefs string) (whisper.Retentions, error) {
	retentions := make(whisper.Retentions, 0)
	for _, retentionDef := range strings.Split(retentionDefs, ",") {
		retentionDef = strings.TrimSpace(retentionDef)
		parts := strings.Split(retentionDef, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad retentions spec %q", retentionDef)
		}

		// try old format
		val1, err1 := strconv.ParseInt(parts[0], 10, 0)
		val2, err2 := strconv.ParseInt(parts[1], 10, 0)

		if err1 == nil && err2 == nil {
			retention := whisper.NewRetention(int(val1), int(val2))
			retentions = append(retentions, &retention)
			continue
		}

		// try new format
		retention, err := whisper.ParseRetentionDef(retentionDef)
		if err != nil {
			return nil, err
		}
		retentions = append(retentions, retention)
	}
	return retentions, nil
}

// ReadWhisperSchemas reads and parses a storage-schemas.conf file and returns a sorted
// schemas structure
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
func ReadWhisperSchemas(file string) (WhisperSchemas, error) {
	config, err := configparser.Read(file)
	if err != nil {
		return nil, err
	}

	sections, err := config.AllSections()
	if err != nil {
		return nil, err
	}

	var schemas WhisperSchemas

	for i, sec := range sections {
		schema := Schema{}
		schema.Name =
			strings.Trim(strings.SplitN(sec.String(), "\n", 2)[0], " []")
		if schema.Name == "" || strings.HasPrefix(schema.Name, "#") {
			continue
		}

		patternStr := sec.ValueOf("pattern")
		if patternStr == "" {
			return nil, fmt.Errorf("[persister] Empty pattern for [%s]", schema.Name)
		}
		schema.Pattern, err = regexp.Compile(patternStr)
		if err != nil {
			return nil, fmt.Errorf("[persister] Failed to parse pattern %q for [%s]: %s",
				sec.ValueOf("pattern"), schema.Name, err.Error())
		}
		schema.RetentionStr = sec.ValueOf("retentions")
		schema.Retentions, err = ParseRetentionDefs(schema.RetentionStr)

		if err != nil {
			return nil, fmt.Errorf("[persister] Failed to parse retentions %q for [%s]: %s",
				sec.ValueOf("retentions"), schema.Name, err.Error())
		}

		priorityStr := sec.ValueOf("priority")

		p := int64(0)
		if priorityStr != "" {
			p, err = strconv.ParseInt(priorityStr, 10, 0)
			if err != nil {
				return nil, fmt.Errorf("[persister] Failed to parse priority %q for [%s]: %s", priorityStr, schema.Name, err)
			}
		}
		schema.Priority = int64(p)<<32 - int64(i) // to sort records with same priority by position in file

		schemas = append(schemas, schema)
	}

	sort.Sort(schemas)
	return schemas, nil
}
