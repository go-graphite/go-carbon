package persister

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
)

func parseIniFile(filename string) ([]map[string]string, error) {
	body, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(body, []byte{'\n'})

	config := make([]map[string]string, 0)
	var section map[string]string

lineLoop:
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(string(lines[i]))

		if len(line) == 0 {
			continue lineLoop
		}

		if line[0] == ';' || line[0] == '#' {
			continue lineLoop
		}

		if line[0] == '[' {
			if line[len(line)-1] != ']' {
				return nil, fmt.Errorf("line %d: unfinished section name", i+1)
			}
			name := strings.TrimSpace(line[1 : len(line)-1])

			if len(name) == 0 {
				return nil, fmt.Errorf("line %d: empty section name", i+1)
			}

			// start new section
			if section != nil {
				config = append(config, section)
			}

			section = map[string]string{"name": name}
			continue lineLoop
		}

		if section == nil {
			return nil, fmt.Errorf("line %d: config section not found", i+1)
		}

		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("line %d: key = value not found", i+1)
		}

		key := strings.ToLower(strings.TrimSpace(kv[0]))
		value := strings.Trim(strings.TrimSpace(kv[1]), "\"'")

		if key == "" {
			return nil, fmt.Errorf("line %d: key is empty", i+1)
		}

		section[key] = value
	}

	if section != nil {
		config = append(config, section)
	}

	return config, nil
}
