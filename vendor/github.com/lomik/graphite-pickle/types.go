package pickle

import (
	"math/big"
	"strconv"

	"github.com/lomik/og-rek"
)

func ToList(value interface{}) ([]interface{}, bool) {
	if l, ok := value.([]interface{}); ok {
		return l, ok
	}

	if l, ok := value.(ogórek.Tuple); ok {
		return []interface{}(l), ok
	}

	return nil, false
}

func ToMap(value interface{}) (map[interface{}]interface{}, bool) {
	if l, ok := value.(map[interface{}]interface{}); ok {
		return l, ok
	}
	return nil, false
}

func ToString(value interface{}) (string, bool) {
	if l, ok := value.(string); ok {
		return l, ok
	}
	return "", false
}

func ToInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case float32:
		return int64(value), true
	case float64:
		return int64(value), true
	case int:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return int64(value), true
	case int8:
		return int64(value), true
	case uint:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		return int64(value), true
	case uint8:
		return int64(value), true
	case string:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	case *big.Int:
		return value.Int64(), true
	case *big.Float:
		v, _ := value.Int64()
		return v, true
	case big.Int:
		return value.Int64(), true
	case big.Float:
		v, _ := value.Int64()
		return v, true
	default:
		return 0, false
	}
}

func ToFloat64(value interface{}) (float64, bool) {
	switch value := value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return float64(value), true
	case int:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case int8:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case uint8:
		return float64(value), true
	case string:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	case *big.Int:
		return float64(value.Int64()), true
	case *big.Float:
		v, _ := value.Float64()
		return v, true
	case big.Int:
		return float64(value.Int64()), true
	case big.Float:
		v, _ := value.Float64()
		return v, true
	default:
		return 0, false
	}
}
