package carbon

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/qa"
)

func TestRestore(t *testing.T) {
	qa.Root(t, func(root string) {
		w := func(fn, body string) {
			err := ioutil.WriteFile(path.Join(root, fn), []byte(body), 0644)
			if err != nil {
				t.Fatal(err)
			}
		}

		w("input.42.1470686967790091088", "m1 1 1470687039\n")
		w("cache.42.1470686967790091088", "m2 2 1470687039\n")

		w("input.15.1470687188677488571", "bad_message\nm3 3 1470687217\n")
		w("cache.15.1470687188677488571", "m4 4 1470687217\n")

		w("input.33.1470687188677488570", "m5 5 1470687217")
		w("cache.33.1470687188677488570", "")

		expected := []*points.Points{
			&points.Points{
				Metric: "m2",
				Data: []points.Point{
					points.Point{
						Value:     2.000000,
						Timestamp: 1470687039,
					},
				},
			},
			&points.Points{
				Metric: "m1",
				Data: []points.Point{
					points.Point{
						Value:     1.000000,
						Timestamp: 1470687039,
					},
				},
			},
			&points.Points{
				Metric: "m5",
				Data: []points.Point{
					points.Point{
						Value:     5.000000,
						Timestamp: 1470687217,
					},
				},
			},
			&points.Points{
				Metric: "m4",
				Data: []points.Point{
					points.Point{
						Value:     4.000000,
						Timestamp: 1470687217,
					},
				},
			},
			&points.Points{
				Metric: "m3",
				Data: []points.Point{
					points.Point{
						Value:     3.000000,
						Timestamp: 1470687217,
					},
				},
			},
		}

		cache := cache.New()
		RestoreFromDir(root, cache, 0)

		if cache.Size() != int32(len(expected)) {
			t.FailNow()
		}

		for idx, p := range expected {
			var m *points.Points
			var ok bool
			if m, ok = cache.GetMetric(p.Metric); !ok {
				t.Fatalf("metric %s wasn't found in cache after restore", p.Metric)
			}
			if m != expected[idx] {
				t.Logf("Strange, pointers are different. Map: %v, []expected slice: %v", &m, &expected[idx])
			}

			if !m.Eq(expected[idx]) {
				t.Fatalf("Found %v, expected %v", m, expected[idx])
			}
		}
	})
}
