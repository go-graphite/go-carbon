package tags

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

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

func FilePath(root string, s string) string {
	sum := sha256.Sum256([]byte(s))
	prefix := fmt.Sprintf("%x", sum[:3])
	return filepath.Join(root, "_tagged", prefix[:3], prefix[3:], strings.Replace(s, ".", "_DOT_", -1))
}
