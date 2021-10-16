package tags

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strings"
)

// Dedicated directory for tagged files/metrics
const Directory = "_tagged"

var UsingEncoding bool
var UsingDirMode bool // Must also set UsingEncoding to true

// as in https://github.com/graphite-project/carbon/blob/master/lib/carbon/util.py
func normalizeOriginal(s string) (string, error) {
	arr := strings.Split(s, ";")

	if len(arr[0]) == 0 {
		return "", fmt.Errorf("cannot parse path %#v, no metric found", s)
	}

	tags := make(map[string]string)

	for i := 1; i < len(arr); i++ {
		kv := strings.SplitN(arr[i], "=", 2)

		if len(kv) != 2 || len(kv[0]) == 0 {
			return "", fmt.Errorf("cannot parse path %#v, invalid segment %#v", s, arr[i])
		}

		tags[kv[0]] = kv[1]
	}

	tmp := make([]string, len(tags))
	i := 0
	for k, v := range tags {
		tmp[i] = fmt.Sprintf(";%s=%s", k, v)
		i++
	}

	sort.Strings(tmp)

	result := arr[0] + strings.Join(tmp, "")

	return result, nil
}

type byKey []string

func (a byKey) Len() int      { return len(a) }
func (a byKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool {
	p1 := strings.Index(a[i], "=")
	if p1 < 0 {
		p1 = len(a[i])
	}
	p2 := strings.Index(a[j], "=")
	if p2 < 0 {
		p2 = len(a[j])
	}

	return strings.Compare(a[i][:p1+1], a[j][:p2+1]) < 0
}

func Normalize(s string) (string, error) {
	if strings.IndexByte(s, ';') < 0 {
		return s, nil
	}

	arr := strings.Split(s, ";")

	if len(arr[0]) == 0 {
		return "", fmt.Errorf("cannot parse path %#v, no metric found", s)
	}

	// check tags
	for i := 1; i < len(arr); i++ {
		if strings.Index(arr[i], "=") < 1 {
			return "", fmt.Errorf("cannot parse path %#v, invalid segment %#v", s, arr[i])
		}
	}

	sort.Stable(byKey(arr[1:]))

	// uniq
	toDel := 0
	prevKey := ""
	for i := 1; i < len(arr); i++ {
		p := strings.Index(arr[i], "=")
		key := arr[i][:p]
		if key == prevKey {
			toDel++
		} else {
			prevKey = key
		}
		if toDel > 0 {
			arr[i-toDel] = arr[i]
		}
	}

	return strings.Join(arr[:len(arr)-toDel], ";"), nil
}

var simpleCharTable = func() (t [256]bool) {
	for i := 'a'; i < 'z'; i++ {
		t[i] = true
	}
	for i := 'A'; i < 'Z'; i++ {
		t[i] = true
	}
	for i := '0'; i < '9'; i++ {
		t[i] = true
	}

	t['='] = true
	t['-'] = true
	t['_'] = true
	t['='] = true
	t['.'] = true

	return
}()

var base64Encoder = base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-")

// TODO: to complete
func isComplexChar(c byte) bool {
	return !simpleCharTable[c]
	// return !(('a' <= c && c <= 'z') ||
	// 	('A' <= c && c <= 'Z') ||
	// 	('0' <= c && c <= '9') ||
	// 	(c == '=') ||
	// 	(c == '-') ||
	// 	(c == '_'))
}

// my.series;tag1=value1;tag2=value2
func FilePath(root string, s string, hashOnly bool) string {
	sum := sha256.Sum256([]byte(s))
	hash := fmt.Sprintf("%x", sum)
	if hashOnly {
		return filepath.Join(root, Directory, hash[:3], hash[3:6], hash)
	}

	if !UsingEncoding {
		return filepath.Join(root, Directory, hash[:3], hash[3:6], strings.ReplaceAll(s, ".", "_DOT_"))
	}

	var name = make([]byte, 0, len(s))
	var pi int
	var hasComplexChar bool
	var tagSection bool
	log.Printf("s = %+v\n", s)
	for i := 0; i < len(s); i++ {
		// log.Printf("s[i] = %s\n", []byte{s[i]})
		// log.Printf("s[i]==';' = %+v\n", s[i] == ';')
		if s[i] != ';' && i < len(s)-1 {
			if isComplexChar(s[i]) {
				hasComplexChar = true
			}

			continue
		}

		part := s[pi:i]
		if i == len(s)-1 {
			part = s[pi:]
		}

		// if UsingDirMode && !tagSection {
		// 	name = append(name, []byte(part)...)
		// 	tagSection = true
		// 	continue
		// }

		log.Printf("part = %+v\n", part)
		if hasComplexChar {
			part = base64Encoder.EncodeToString([]byte(part))
		}
		if tagSection {
			name = append(name, ';')
		}
		name = append(name, []byte(part)...)

		tagSection = true
		hasComplexChar = false
		pi = i + 1
	}

	// THOUGHTS: another idea of scalabe file naming scheme is: metric/path/$tag_marker/tag1=val1/tag2=val2.wsp
	// could be implemented if there is requirement to have metric path longer than 255 bytes with tags.

	if !UsingDirMode {
		return filepath.Join(root, Directory, hash[:3], hash[3:6], string(name))
	}

	// from: metric.path;tag1=val1;tag2=val2
	// to:   metric.path/tag1=val1/tag2=val2

	for i := 0; i < len(name); i++ {
		if name[i] != ';' {
			continue
		}

		name[i] = '/'

		if i+1 < len(name) && name[i+1] == ';' {
			i++
		}
	}

	return filepath.Join(root, Directory, string(name))
}

// my_DOT_series;tag1=value1;tag2=value2
// my.series;;tag1=value1;tag2=value2
// my.series/;tag1=value1/tag2=value2

// type Pair struct {
// 	Name  string
// 	Value string
// }

func MetricPath(name string) (string, error) {
	if !UsingEncoding {
		return strings.ReplaceAll(name, "_DOT_", "."), nil
	}

	if !UsingDirMode {
		var parts = strings.Split(name, ";")
		var nameb = make([]byte, 0, len(name))

		nameb = append(nameb, []byte(parts[0])...)

		for i := 1; i < len(parts); i++ {
			if parts[i] == "" {
				i++
				if i > len(parts)-1 {
					break
				}

				data, err := base64Encoder.DecodeString(parts[i])
				if err != nil {
					return "", err
				}
				parts[i] = string(data)
			}

			nameb = append(nameb, ';')
			nameb = append(nameb, []byte(parts[i])...)
		}

		return string(nameb), nil
	}

	var parts = strings.Split(name, "/")
	var nameb = make([]byte, 0, len(name))

	nameb = append(nameb, []byte(parts[0])...)

	for i := 1; i < len(parts); i++ {
		if strings.HasPrefix(parts[i], ";") {
			data, err := base64Encoder.DecodeString(parts[i])
			if err != nil {
				return "", err
			}
			parts[i] = string(data)
		}

		nameb = append(nameb, ';')
		nameb = append(nameb, []byte(parts[i])...)
	}

	return string(nameb), nil
}
