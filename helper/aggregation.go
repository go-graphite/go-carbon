package helper

import (
	"errors"
	"strconv"
	"strings"

	whisper "github.com/go-graphite/go-whisper"
)

func ParseAggregationMethod(str string) (method whisper.AggregationMethod, percentile float32, err error) {
	switch str {
	case "average", "avg":
		method = whisper.Average
	case "sum":
		method = whisper.Sum
	case "last":
		method = whisper.Last
	case "max":
		method = whisper.Max
	case "min":
		method = whisper.Min
	case "median":
		str = "p50"
		fallthrough
	default:
		if strings.HasPrefix(str, "p") {
			method = whisper.Percentile
			var percentile64 float64
			percentile64, err = strconv.ParseFloat(strings.TrimLeft(str, "p"), 32)
			percentile = float32(percentile64)
		} else {
			err = errors.New("unknown aggregation method")
		}
	}
	return
}
