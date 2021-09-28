package carbonserver

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/go-graphite/go-carbon/tags"
)

// Graphite doc: https://graphite.readthedocs.io/en/latest/tags.html

type metricID int32

type tagsIndex struct {
	idIdx   metricID
	ids     map[metricID]string
	metrics map[string]metricID

	tags map[string]map[string][]metricID

	// tvTrigrams
	//  tag value trigrams -> value id
}

func newTagsIndex() *tagsIndex {
	return &tagsIndex{
		metrics: map[string]metricID{},
		ids:     map[metricID]string{},
		tags:    map[string]map[string][]metricID{},
	}
}

// my.series;tag1=value1;tag2=value2
//
// Tag names must have a length >= 1 and may contain any ascii
// characters except ;!^=. Tag values must also have a length >= 1,
// they may contain any ascii characters except ; and the first
// character must not be ~. UTF-8 characters may work for names and
// values, but they are not well tested and it is not recommended to
// use non-ascii characters in metric names or tags. Metric names get
// indexed under the special tag name, if a metric name starts with one
// or multiple ~ they simply get removed from the derived tag value
// because the ~ character is not allowed to be in the first position
// of the tag value. If a metric name consists of no other characters
// than ~, then it is considered invalid and may get dropped.

func (ti *tagsIndex) addMetric(taggedMetric string) error {
	taggedMetric, err := tags.Normalize(taggedMetric)
	if err != nil {
		return err
	}

	if _, ok := ti.metrics[taggedMetric]; ok {
		return nil
	}

	id := ti.idIdx
	ti.idIdx++

	ti.metrics[taggedMetric] = id
	ti.ids[id] = taggedMetric

	strs := strings.Split(taggedMetric, ";") // TODO: make it zero-allocation?

	// tagSet := map[string]string{}
	for i, tagp := range strs {
		var tag, val string
		if i == 0 {
			tag = "name" // TODO: allow tag using value of "name"
			val = tagp
		} else {
			kv := strings.Split(tagp, "=") // TODO: make it zero-allocation?
			tag = kv[0]
			val = kv[1]
		}

		vals, ok := ti.tags[tag]
		if !ok {
			ti.tags[tag] = map[string][]metricID{}
			vals = ti.tags[tag]
		}

		// TODO: sorted insert
		vals[val] = append(vals[val], id)
	}

	return nil
}

type tagMatcher int8

const (
	tagMatcherEqual tagMatcher = iota
	tagMatcherNotEqual
	tagMatcherMatch
	tagMatcherNotMatch
)

// # find all series that have tag1 set to value1, sorted by total
// seriesByTag('tag1=value1') | sortByTotal()
//
// # find all series where name matches the regular expression cpu\..*, AND tag1 is not value1
// seriesByTag('name=~cpu\..*', 'tag1!=value1')
//
// Tags expressions are strings, and may have the following formats:
//
// 	tag=spec    tag value exactly matches spec
// 	tag!=spec   tag value does not exactly match spec
// 	tag=~value  tag value matches the regular expression spec
// 	tag!=~spec  tag value does not match the regular expression spec
func (ti *tagsIndex) seriesByTag(tagqs []string) ([]string, error) {
	// case 1: high number of tags
	// 	solution: trigram index
	// case 2: high number of values in some tags
	// 	solution: trigram index
	// case 3: high number of metric in some tag values
	// 	solution: skip list or bit set (roaring)?

	var idSets [][]metricID
	for _, tagq := range tagqs {
		var tag, spec string
		var expr tagMatcher
		var specRegexp *regexp.Regexp
		for i := 0; i < len(tagq); i++ {
			if tagq[i] == '=' {
				tag = tagq[:i]

				// TODO: handle incomplete expr
				if tagq[i+1] == '~' {
					expr = tagMatcherMatch
					spec = tagq[i+2:]

					var err error
					specRegexp, err = regexp.Compile(spec)
					if err != nil {
						return nil, fmt.Errorf("tags: failed to parse regexp: %s: %s", tagq, err)
					}
				} else {
					expr = tagMatcherEqual
					spec = tagq[i+1:]
				}
			} else if tagq[i] == '!' {
				// TODO: handle incomplete expr
				if tagq[i+1] != '=' {
					return nil, fmt.Errorf("tags: broken tag expression: %s", tagq)
				}
				if tagq[i+2] == '~' {
					expr = tagMatcherNotMatch
					spec = tagq[i+3:]

					var err error
					specRegexp, err = regexp.Compile(spec)
					if err != nil {
						return nil, fmt.Errorf("tags: failed to parse regexp: %s: %s", tagq, err)
					}
				} else {
					expr = tagMatcherNotEqual
					spec = tagq[i+2:]
				}
			}
		}

		vals, ok := ti.tags[tag]
		if !ok {
			return nil, nil
		}

		for val, ids := range vals {
			switch expr {
			case tagMatcherEqual:
				if val == spec {
					idSets = append(idSets, ids)
				}
			case tagMatcherNotEqual:
				if val != spec {
					idSets = append(idSets, ids)
				}
			case tagMatcherMatch:
				if specRegexp.MatchString(val) {
					idSets = append(idSets, ids)
				}
			case tagMatcherNotMatch:
				if !specRegexp.MatchString(val) {
					idSets = append(idSets, ids)
				}
			}
		}
	}

	// interset idSets
	if len(idSets) == 0 {
		return nil, nil
	}
	ids := idSets[0]
	idSets = idSets[1:]
	for len(idSets) > 0 {
		ids1 := ids
		ids2 := idSets[0]

		ids = []metricID{}
		idSets = idSets[1:]

		sort.Slice(ids1, func(i, j int) bool { return ids1[i] < ids1[j] })
		sort.Slice(ids2, func(i, j int) bool { return ids2[i] < ids2[j] })

		for i, j := 0, 0; i < len(ids1) && j < len(ids2); {
			if ids1[i] == ids2[j] {
				ids = append(ids, ids1[i])
				i++
				j++
			} else if ids1[i] > ids2[j] {
				j++
			} else {
				i++
			}
		}
	}

	var metrics []string
	for _, id := range ids {
		metrics = append(metrics, ti.ids[id])
	}

	return metrics, nil
}

// curl -s "http://graphite/tags?pretty=1&filter=data"
//
// [
//   {
//     "tag": "datacenter"
//   }
// ]
func (ti *tagsIndex) getTags(filter string, limit int) error {
	return nil
}

// curl -s "http://graphite/tags/datacenter?pretty=1&filter=dc1"
//
// {
//   "tag": "datacenter",
//   "values": [
//     {
//       "count": 2,
//       "value": "dc1"
//     }
//   ]
// }
func (ti *tagsIndex) getTagValues(tag string, filter string, limit int) error {
	return nil
}
