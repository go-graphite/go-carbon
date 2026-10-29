package persister

import (
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

const (
	patternRegexp uint8 = iota
	patternContains
	patternPrefix
	patternSuffix
	patternExact
)

// Keep rule order unchanged; anchored literals can reject a rule before the
// regexp engine scans the metric. Other expressions retain regexp semantics.
type metricPattern struct {
	pattern        *regexp.Regexp
	prefix, suffix string
	literal        string
	mode           uint8
}

func compileMetricPattern(pattern *regexp.Regexp) metricPattern {
	m := metricPattern{pattern: pattern}
	re, err := syntax.Parse(pattern.String(), syntax.Perl)
	if err != nil {
		return m
	}
	var parts []*syntax.Regexp
	var flatten func(*syntax.Regexp)
	flatten = func(r *syntax.Regexp) {
		switch r.Op {
		case syntax.OpCapture, syntax.OpConcat:
			for _, sub := range r.Sub {
				flatten(sub)
			}
		default:
			parts = append(parts, r)
		}
	}
	flatten(re)
	start, end := 0, len(parts)
	if parts[start].Op == syntax.OpBeginText {
		start++
	}
	if parts[end-1].Op == syntax.OpEndText {
		end--
	}
	literal := func(r *syntax.Regexp) string {
		if r.Op != syntax.OpLiteral || r.Flags&syntax.FoldCase != 0 {
			return ""
		}
		for _, c := range r.Rune {
			// regexp treats invalid UTF-8 bytes as RuneError too.
			if c == utf8.RuneError {
				return ""
			}
		}
		return string(r.Rune)
	}
	if start < end {
		if start != 0 {
			m.prefix = literal(parts[start])
		}
		if end != len(parts) {
			m.suffix = literal(parts[end-1])
		}
		if end-start == 1 {
			m.literal = literal(parts[start])
			if m.literal != "" {
				switch {
				case start != 0 && end != len(parts):
					m.mode = patternExact
				case start != 0:
					m.mode = patternPrefix
				case end != len(parts):
					m.mode = patternSuffix
				default:
					m.mode = patternContains
				}
			}
		}
	}
	return m
}

func (m metricPattern) match(pattern *regexp.Regexp, metric string) bool {
	// Schema.Pattern is public and callers can construct or replace it.
	if m.pattern != pattern {
		return pattern.MatchString(metric)
	}
	switch m.mode {
	case patternContains:
		return strings.Contains(metric, m.literal)
	case patternPrefix:
		return strings.HasPrefix(metric, m.literal)
	case patternSuffix:
		return strings.HasSuffix(metric, m.literal)
	case patternExact:
		return metric == m.literal
	}
	return strings.HasPrefix(metric, m.prefix) && strings.HasSuffix(metric, m.suffix) && pattern.MatchString(metric)
}
