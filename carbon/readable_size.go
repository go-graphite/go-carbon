package carbon

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// readableSize decodes a count or byte size into an existing integer field.
// Keeping only the numeric value makes equivalent spellings equal on reload.
type readableSize struct {
	target any
	bytes  bool
}

func (s *readableSize) UnmarshalTOML(value any) error {
	target := reflect.ValueOf(s.target).Elem()
	var n int64
	switch v := value.(type) {
	case int64:
		n = v // Preserve signed sentinel values and the decoder's integer semantics.
	case string:
		multiplier := int64(1)
		for _, unit := range []struct {
			suffix     string
			multiplier int64
			bytes      bool
		}{
			{"KiB", 1 << 10, true}, {"MiB", 1 << 20, true}, {"GiB", 1 << 30, true},
			{"K", 1_000, false}, {"M", 1_000_000, false}, {"G", 1_000_000_000, false}, {"T", 1_000_000_000_000, false},
		} {
			if strings.HasSuffix(v, unit.suffix) {
				if unit.bytes && !s.bytes {
					return fmt.Errorf("binary suffix %q is only valid for byte sizes", unit.suffix)
				}
				v, multiplier = strings.TrimSuffix(v, unit.suffix), unit.multiplier
				break
			}
		}
		if v == "" {
			return fmt.Errorf("invalid size %q", value)
		}
		for _, c := range v {
			if c < '0' || c > '9' {
				return fmt.Errorf("invalid size %q: expected an integer with K/M/G/T or byte-only KiB/MiB/GiB", value)
			}
		}
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil || parsed > math.MaxInt64/multiplier {
			return fmt.Errorf("size %q overflows an integer", value)
		}
		n = parsed * multiplier
	default:
		return fmt.Errorf("invalid size %v: expected an integer or readable string", value)
	}
	switch target.Kind() {
	case reflect.Int, reflect.Int64:
		if target.OverflowInt(n) {
			return fmt.Errorf("size %v overflows %s", value, target.Type())
		}
		target.SetInt(n)
	case reflect.Uint64:
		if n < 0 {
			return fmt.Errorf("size %v must be nonnegative", value)
		}
		target.SetUint(uint64(n))
	default:
		return fmt.Errorf("unsupported size target %s", target.Type())
	}
	return nil
}
