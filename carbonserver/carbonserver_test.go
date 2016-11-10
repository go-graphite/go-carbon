package carbonserver

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	pb "github.com/dgryski/carbonzipper/carbonzipperpb"
	"github.com/dgryski/go-trigram"
	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/points"
	whisper "github.com/lomik/go-whisper"
)

type FetchTest struct {
	path             string
	name             string
	now              int
	from             int
	until            int
	createWhisper    bool
	fillWhisper      bool
	fillCache        bool
	errIsNil         bool
	dataIsNil        bool
	expectedStep     int32
	expectedErr      string
	expectedValues   []float64
	expectedIsAbsent []bool
}

func TestExtractTrigrams(t *testing.T) {

	tests := []struct {
		query string
		want  []string
	}{
		{"foo.bar.baz", []string{"foo", "oo.", "o.b", ".ba", "bar", "ar.", "r.b", "baz"}},
		{"foo.*.baz", []string{"foo", "oo.", ".ba", "baz"}},
		{"foo.bar[12]qux.*", []string{"foo", "oo.", "o.b", ".ba", "bar", "qux", "ux."}},
		{"foo.bar*.5*.qux.*", []string{"foo", "oo.", "o.b", ".ba", "bar", ".qu", "qux", "ux."}},
		{"foob[arzf", []string{"foo", "oob"}},
	}

	for _, tt := range tests {
		got := extractTrigrams(tt.query)
		var w []trigram.T
		for _, v := range tt.want {
			tri := trigram.T(v[0])<<16 | trigram.T(v[1])<<8 | trigram.T(v[2])
			w = append(w, tri)
		}
		if !reflect.DeepEqual(got, w) {
			t.Errorf("extractTrigrams(%q)=%q, want %#v\n", tt.query, got, tt.want)
		}
	}
}

func generalFetchSingleMetricInit(testData FetchTest, cache *cache.Cache) (error) {
	var wsp *whisper.Whisper
	var p []*whisper.TimeSeriesPoint
	retentions, err := whisper.ParseRetentionDefs("1m:10m,2m:30m")

	if testData.createWhisper {
		wsp, err = whisper.Create(testData.path+testData.name+".wsp", retentions, whisper.Last, 0.0)
		if err != nil {
			return err
		}

		val := float64(0.0)
		if testData.fillWhisper {
			for i := testData.from; i < testData.now-120; i += 60 {
				p = append(p, &whisper.TimeSeriesPoint{i, val})
				val += 0.1
			}
			err := wsp.UpdateMany(p)
			if err != nil {
				return err
			}
		}
		wsp.Close()
		if testData.fillCache {
			cache.Add(points.OnePoint(testData.name, 7.0, int64(testData.now-123)))
			cache.Add(points.OnePoint(testData.name, 7.1, int64(testData.now-119)))
			cache.Add(points.OnePoint(testData.name, 7.2, int64(testData.now-67)))
			cache.Add(points.OnePoint(testData.name, 7.3, int64(testData.now-45)))
		}
	}
	return nil
}

func generalFetchSingleMetricRemove(testData FetchTest) {
	os.Remove(testData.path + testData.name + ".wsp")
}

func generalFetchSingleMetricHelper(testData FetchTest, cache *cache.Cache, carbonserver *CarbonserverListener) (*pb.FetchResponse, error) {
	data, err := carbonserver.fetchSingleMetric(testData.name, int32(testData.from), int32(testData.until))
	return data, err
}


func testFetchSingleMetricHelper(testData FetchTest, cache *cache.Cache, carbonserver *CarbonserverListener) (*pb.FetchResponse, error) {
	err := generalFetchSingleMetricInit(testData, cache)
	if err != nil {
		return nil, err
	}
	defer generalFetchSingleMetricRemove(testData)
	data, err := generalFetchSingleMetricHelper(testData, cache, carbonserver)
	return data, err
}

func TestFetchSingleMetric(t *testing.T) {
	cache := cache.New()
	path, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	path += "/"
	carbonserver := CarbonserverListener{
		whisperData: path,
		cache:       cache,
	}
	now := int(time.Now().Unix())
	now = now - now%120
	precision := 0.000001
	tests := []FetchTest{
		{
			path:          path,
			name:          "non-existing",
			createWhisper: false,
			fillWhisper:   false,
			fillCache:     true,
			errIsNil:      false,
			dataIsNil:     true,
			expectedErr:   "Can't open metric",
		},
		{
			path:          path,
			name:          "no-proper-archive",
			createWhisper: true,
			fillWhisper:   true,
			fillCache:     false,
			from:          1,
			until:         10,
			now:           now,
			errIsNil:      false,
			dataIsNil:     true,
			expectedErr:   "Can't find proper archive",
		},
		/*		{
					path: path,
					name: "cross-retention",
					createWhisper: true,
					fillWhisper: true,
					fillCache: true,
					from: now - 1200,
					until: now,
					now: now,
					errIsNil: true,
					dataIsNil: false,
					expectedStep: 120,
					expectedValues: []float64{0.3, 0.5, 0.7, 0.9, 1.1, 1.3, 1.5, 1.7, 0.0, 0.0},
					expectedIsAbsent: []bool{false, false, false, false, false, false, false, false, true, true},
				},
		*/
		{
			path:             path,
			name:             "data-file",
			createWhisper:    true,
			fillWhisper:      true,
			fillCache:        false,
			from:             now - 300,
			until:            now,
			now:              now,
			errIsNil:         true,
			dataIsNil:        false,
			expectedStep:     60,
			expectedValues:   []float64{0.1, 0.2, 0.0, 0.0, 0.0},
			expectedIsAbsent: []bool{false, false, true, true, true},
		},
		{
			path:             path,
			name:             "data-file-cache",
			createWhisper:    true,
			fillWhisper:      true,
			fillCache:        true,
			from:             now - 300,
			until:            now,
			now:              now,
			errIsNil:         true,
			dataIsNil:        false,
			expectedStep:     60,
			expectedValues:   []float64{0.1, 0.2, 7.0, 7.1, 7.3},
			expectedIsAbsent: []bool{false, false, false, false, false},
		},
		{
			path:             path,
			name:             "data-cache",
			createWhisper:    true,
			fillWhisper:      false,
			fillCache:        true,
			from:             now - 300,
			until:            now,
			now:              now,
			errIsNil:         true,
			dataIsNil:        false,
			expectedStep:     60,
			expectedValues:   []float64{0.0, 0.0, 7.0, 7.1, 7.3},
			expectedIsAbsent: []bool{true, true, false, false, false},
		},
	}
	// common

	// Non-existing metric
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fmt.Println("Performing test ", test.name)
			data, err := testFetchSingleMetricHelper(test, cache, &carbonserver)
			if !test.errIsNil {
				if err == nil || err.Error() != test.expectedErr || (data == nil) != test.dataIsNil {
					t.Errorf("err: '%v', expected: '%v'", err, test.expectedErr)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}
				if data == nil {
					t.Errorf("Unexpected empty data")
					return
				}
				fmt.Printf("%+v\n\n", data)
				if *(data.StepTime) != test.expectedStep {
					t.Errorf("Unepxected step: '%v', expected: '%v'\n", *(data.StepTime), test.expectedStep)
					return
				}
				if len(test.expectedValues) != len(data.Values) {
					t.Errorf("Unexpected amount of data in return. Got %v, expected %v", len(data.Values), len(test.expectedValues))
					return
				}
				if len(data.Values) != len(data.IsAbsent) {
					t.Errorf("len of Values should match len of IsAbsent! Expected: (%v, %v), got (%v, %v)", len(test.expectedValues), len(test.expectedIsAbsent), len(data.Values), len(data.IsAbsent))
					return
				}
				for i := range test.expectedValues {
					if math.Abs(test.expectedValues[i]-data.Values[i]) > precision {
						t.Errorf("test=%v, position %v, got %v, expected %v", test.name, i, data.Values[i], test.expectedValues[i])
					}
					if test.expectedIsAbsent[i] != data.IsAbsent[i] {
						t.Errorf("test=%v, position %v, got isAbsent=%v, expected %v", test.name, i, data.IsAbsent[i], test.expectedIsAbsent[i])
					}
				}
			}
		})
	}
}

func BenchmarkFetchSingleMetric(t *testing.B) {
	cache := cache.New()
	path, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	path += "/"
	carbonserver := CarbonserverListener{
		whisperData: path,
		cache:       cache,
	}
	now := int(time.Now().Unix())
	now = now - now%120
	tests := []FetchTest{
		{
			path:             path,
			name:             "data-file",
			createWhisper:    true,
			fillWhisper:      true,
			fillCache:        false,
			from:             now - 300,
			until:            now,
			now:              now,
			errIsNil:         true,
			dataIsNil:        false,
			expectedStep:     60,
			expectedValues:   []float64{0.1, 0.2, 0.0, 0.0, 0.0},
			expectedIsAbsent: []bool{false, false, true, true, true},
		},
		{
			path:             path,
			name:             "data-file-cache",
			createWhisper:    true,
			fillWhisper:      true,
			fillCache:        true,
			from:             now - 300,
			until:            now,
			now:              now,
			errIsNil:         true,
			dataIsNil:        false,
			expectedStep:     60,
			expectedValues:   []float64{0.1, 0.2, 7.0, 7.1, 7.3},
			expectedIsAbsent: []bool{false, false, false, false, false},
		},
		{
			path:             path,
			name:             "data-cache",
			createWhisper:    true,
			fillWhisper:      false,
			fillCache:        true,
			from:             now - 300,
			until:            now,
			now:              now,
			errIsNil:         true,
			dataIsNil:        false,
			expectedStep:     60,
			expectedValues:   []float64{0.0, 0.0, 7.0, 7.1, 7.3},
			expectedIsAbsent: []bool{true, true, false, false, false},
		},
	}
	// common

	// Non-existing metric
	for _, test := range tests {

		err := generalFetchSingleMetricInit(test, cache)
		if err != nil {
			t.Fatalf("Unexpected error %v\n", err)
		}

		t.Run(test.name, func(t *testing.B) {
			for runs := 0; runs < t.N; runs++ {
				data, err := generalFetchSingleMetricHelper(test, cache, &carbonserver)
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}
				if data == nil {
					t.Errorf("Unexpected empty data")
					return
				}
			}
		})
		generalFetchSingleMetricRemove(test)
	}
}
