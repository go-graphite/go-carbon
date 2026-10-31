package carbon

import (
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"
)

func unknownConfigKeys(metadata toml.MetaData) []toml.Key {
	var keys []toml.Key
	seen := make(map[string]bool)
	for _, key := range metadata.Undecoded() {
		// Receiver options are opaque maps, decoded by receiver.New using
		// the registered protocol's options type. Nested interface values
		// remain Undecoded even though they are passed to that decoder.
		if strings.EqualFold(key[0], "receiver") {
			continue
		}
		path := key.String()
		// Array-of-table entries can repeat the same dotted key.
		if !seen[path] {
			keys = append(keys, key)
			seen[path] = true
		}
	}
	return keys
}

// suggestConfigKey compares siblings in the TOML schema, including settings
// absent from the file. Derive names from the decoder's types to avoid a second
// list of supported options. Dynamic map keys (labels, receiver names, etc.)
// are not spelling candidates.
func suggestConfigKey(key toml.Key) string {
	t := reflect.TypeOf(Config{})
	for _, part := range key[:len(key)-1] {
		t = configTableType(t)
		switch t.Kind() {
		case reflect.Struct:
			var next reflect.Type
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				if name := configFieldName(field); name != "" && strings.EqualFold(name, part) {
					next = field.Type
					break
				}
			}
			if next == nil {
				return ""
			}
			t = next
		case reflect.Map:
			t = t.Elem()
		default:
			return ""
		}
	}
	t = configTableType(t)
	if t.Kind() != reflect.Struct {
		return ""
	}

	name := key[len(key)-1]
	best, bestDistance, bestPrefix := "", int(^uint(0)>>1), 0
	for i := 0; i < t.NumField(); i++ {
		candidate := configFieldName(t.Field(i))
		if candidate == "" {
			continue
		}
		distance := configKeyDistance(name, candidate)
		prefix := 0
		for prefix < len(name) && prefix < len(candidate) && name[prefix] == candidate[prefix] {
			prefix++
		}
		// At most two edits, or a substantial common prefix covering at
		// least half of both names (e.g. a renamed suffix).
		if distance > 2 && (prefix < 8 || prefix*2 < max(len(name), len(candidate))) {
			continue
		}
		if distance < bestDistance || distance == bestDistance && (prefix > bestPrefix || prefix == bestPrefix && candidate < best) {
			best, bestDistance, bestPrefix = candidate, distance, prefix
		}
	}
	if best == "" {
		return ""
	}
	suggestion := append(toml.Key(nil), key...)
	suggestion[len(suggestion)-1] = best
	return suggestion.String()
}

func configTableType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	return t
}

func configFieldName(field reflect.StructField) string {
	if !field.IsExported() {
		return ""
	}
	name := strings.Split(field.Tag.Get("toml"), ",")[0]
	if name == "-" {
		return ""
	}
	if name == "" {
		name = field.Name
	}
	return name
}

func configKeyDistance(a, b string) int {
	left, right := []rune(a), []rune(b)
	row := make([]int, len(right)+1)
	for i := range row {
		row[i] = i
	}
	for i, x := range left {
		diagonal := row[0]
		row[0] = i + 1
		for j, y := range right {
			previous := row[j+1]
			cost := 0
			if x != y {
				cost = 1
			}
			row[j+1] = min(row[j]+1, previous+1, diagonal+cost)
			diagonal = previous
		}
	}
	return row[len(right)]
}
