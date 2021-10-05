package persister

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Quota represents one quota setting.
type Quota struct {
	Pattern          string
	Namespaces       int64
	Metrics          int64
	LogicalSize      int64
	PhysicalSize     int64
	DataPoints       int64
	Throughput       int64
	DroppingPolicy   string
	StatMetricPrefix string
}

type WhisperQuotas []Quota

// ReadWhisperQuotas reads and parses a storage-quotas.conf file and returns the
// defined quotas.
func ReadWhisperQuotas(filename string) (WhisperQuotas, error) {
	config, err := parseIniFile(filename)
	if err != nil {
		return nil, err
	}

	var quotas WhisperQuotas

	parseInt := func(section map[string]string, name string) (int64, error) {
		str := section[name]
		switch str {
		case "":
			return 0, nil
		case "maximum", "max":
			return math.MaxInt64, nil
		}

		v, err := strconv.ParseInt(strings.ReplaceAll(str, ",", ""), 10, 64)
		if err != nil {
			err = fmt.Errorf("[persister] Failed to parse %s for [%s]: %s", name, section["name"], err)
		}
		return v, err
	}

	for _, section := range config {
		var quota Quota
		quota.Pattern = section["name"]

		if quota.Namespaces, err = parseInt(section, "namespaces"); err != nil {
			return nil, err
		}
		if quota.Metrics, err = parseInt(section, "metrics"); err != nil {
			return nil, err
		}
		if quota.LogicalSize, err = parseInt(section, "logical-size"); err != nil {
			return nil, err
		}
		if quota.PhysicalSize, err = parseInt(section, "physical-size"); err != nil {
			return nil, err
		}
		if quota.DataPoints, err = parseInt(section, "data-points"); err != nil {
			return nil, err
		}
		if quota.Throughput, err = parseInt(section, "throughput"); err != nil {
			return nil, err
		}

		switch v := section["dropping-policy"]; v {
		case "new", "none", "":
			quota.DroppingPolicy = v
		default:
			return nil, fmt.Errorf("[persister] Unknown dropping-policy %q for [%s]", v, section["name"])
		}

		quota.StatMetricPrefix = strings.TrimPrefix(section["stat-metric-prefix"], ".")

		quotas = append(quotas, quota)
	}

	return quotas, nil
}
