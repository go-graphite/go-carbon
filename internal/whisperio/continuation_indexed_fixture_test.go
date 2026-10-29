package whisperio

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// Keep the long integrity trace's prefix reads from repeatedly sorting the
// whole fixture map. Values and write counters still use the original fixture.
type indexedIntegrityIO struct {
	*countingIO
	keys []string
}

func (c *indexedIntegrityIO) Set(key, value []byte) error {
	if _, ok := c.mapContinuationIO[string(key)]; !ok {
		c.keys = nil
	}
	return c.countingIO.Set(key, value)
}

func (c *indexedIntegrityIO) Delete(key []byte) error {
	c.keys = nil
	return c.countingIO.Delete(key)
}

func (c *indexedIntegrityIO) DeletePrefix(prefix []byte) error {
	c.keys = nil
	return c.countingIO.DeletePrefix(prefix)
}

func (c *indexedIntegrityIO) Visit(prefix []byte, fn func([]byte, []byte) error) error {
	if c.keys == nil {
		c.keys = make([]string, 0, len(c.mapContinuationIO))
		for key := range c.mapContinuationIO {
			c.keys = append(c.keys, key)
		}
		slices.Sort(c.keys)
	}
	start, _ := slices.BinarySearch(c.keys, string(prefix))
	end := start
	for end < len(c.keys) && strings.HasPrefix(c.keys[end], string(prefix)) {
		end++
	}
	// Like mapContinuationIO, snapshot the matching keys before callbacks can
	// mutate the fixture, and read each value when its callback runs.
	keys := slices.Clone(c.keys[start:end])
	for _, key := range keys {
		if err := fn([]byte(key), c.mapContinuationIO[key]); err != nil {
			return err
		}
	}
	return nil
}

func TestIndexedIntegrityIOMatchesMap(t *testing.T) {
	indexed := &indexedIntegrityIO{countingIO: &countingIO{mapContinuationIO: mapContinuationIO{}}}
	oracle := mapContinuationIO{}
	rng := rand.New(rand.NewSource(20261028))
	stopped := errors.New("stop")
	for step := range 256 {
		key := []byte(fmt.Sprintf("%c/%02d", []byte{0, 'a', 'b', 255}[rng.Intn(4)], rng.Intn(16)))
		switch rng.Intn(4) {
		case 0:
			indexed.Delete(key)
			oracle.Delete(key)
		case 1:
			indexed.DeletePrefix(key[:2])
			oracle.DeletePrefix(key[:2])
		default:
			value := []byte(fmt.Sprint(step))
			indexed.Set(key, value)
			oracle.Set(key, value)
			key[0], value[0] = 'x', 'x'
		}
		if !reflect.DeepEqual(indexed.mapContinuationIO, oracle) {
			t.Fatalf("step %d: map differs", step)
		}
		for _, prefix := range [][]byte{nil, {0}, {'a'}, {'b'}, {255}, []byte("absent")} {
			for _, stop := range []int{0, 2} {
				read := func(visit func([]byte, func([]byte, []byte) error) error) ([]byte, error) {
					var out []byte
					count := 0
					err := visit(prefix, func(key, value []byte) error {
						out = append(out, key...)
						out = append(out, value...)
						count++
						if count == stop {
							return stopped
						}
						return nil
					})
					return out, err
				}
				got, ge := read(indexed.Visit)
				want, we := read(oracle.Visit)
				if !bytes.Equal(got, want) || !errors.Is(ge, we) {
					t.Fatalf("step %d prefix %x: visit differs", step, prefix)
				}
			}
		}
	}
}
