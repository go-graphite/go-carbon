package carbonserver

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	trigram "github.com/dgryski/go-trigram"
	"github.com/lomik/go-carbon/cache"
	pb "github.com/lomik/go-carbon/carbonzipperpb"
	"github.com/lomik/go-carbon/points"
	whisper "github.com/lomik/go-whisper"
)

type point struct {
	Timestamp int
	Value     float64
}

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
	cachePoints      []point
	expectedStep     int32
	expectedErr      string
	expectedValues   []float64
	expectedIsAbsent []bool
	retention        string
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

func generalFetchSingleMetricInit(testData *FetchTest, cache *cache.Cache) error {
	var wsp *whisper.Whisper
	var p []*whisper.TimeSeriesPoint
	if testData.retention == "" {
		testData.retention = "1m:10m,2m:30m"
	}
	retentions, err := whisper.ParseRetentionDefs(testData.retention)

	if testData.createWhisper {
		wsp, err = whisper.Create(filepath.Join(testData.path, testData.name+".wsp"), retentions, whisper.Last, 0.0)
		if err != nil {
			return err
		}

		val := float64(0.0)
		if testData.fillWhisper {
			until := testData.until - 120
			if until < 0 {
				until = testData.until
			}
			for i := testData.from; i < until; i += 60 {
				p = append(p, &whisper.TimeSeriesPoint{Time: i, Value: val})
				val += 0.1
			}
			err := wsp.UpdateMany(p)
			if err != nil {
				return err
			}
		}
		wsp.Close()
		if testData.fillCache {
			for _, p := range testData.cachePoints {
				cache.Add(points.OnePoint(testData.name, p.Value, int64(p.Timestamp)))
			}
		}
	}
	return nil
}

func generalFetchSingleMetricRemove(testData *FetchTest) {
	os.Remove(filepath.Join(testData.path, testData.name+".wsp"))
}

func generalFetchSingleMetricHelper(testData *FetchTest, cache *cache.Cache, carbonserver *CarbonserverListener) (*pb.FetchResponse, error) {
	data, err := carbonserver.fetchSingleMetric(testData.name, int32(testData.from), int32(testData.until))
	return data, err
}

func testFetchSingleMetricHelper(testData *FetchTest, cache *cache.Cache, carbonserver *CarbonserverListener) (*pb.FetchResponse, error) {
	err := generalFetchSingleMetricInit(testData, cache)
	if err != nil {
		return nil, err
	}
	defer generalFetchSingleMetricRemove(testData)
	data, err := generalFetchSingleMetricHelper(testData, cache, carbonserver)
	return data, err
}

var day = 60 * 60 * 24
var now = (int(time.Now().Unix()) / 120) * 120
var singleMetricTests = []FetchTest{
	{
		name:          "non-existing",
		createWhisper: false,
		fillWhisper:   false,
		fillCache:     true,
		errIsNil:      false,
		dataIsNil:     true,
		expectedErr:   "Can't open metric",
	},
	{
		name:          "no-proper-archive",
		createWhisper: true,
		fillWhisper:   true,
		fillCache:     false,
		from:          1,
		until:         10,
		errIsNil:      false,
		dataIsNil:     true,
		expectedErr:   "Can't find proper archive",
	},
	{
		name:             "cross-retention",
		createWhisper:    true,
		fillWhisper:      true,
		fillCache:        true,
		from:             now - 1200,
		until:            now,
		now:              now,
		errIsNil:         true,
		dataIsNil:        false,
		cachePoints:      []point{{now - 123, 7.0}, {now - 119, 7.1}, {now - 45, 7.3}, {now - 243, 6.9}, {now - 67, 7.2}},
		expectedStep:     120,
		expectedValues:   []float64{0.3, 0.5, 0.7, 0.9, 1.1, 1.3, 1.5, 1.7, 0.0, 0.0},
		expectedIsAbsent: []bool{false, false, false, false, false, false, false, false, true, true},
	},
	{
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
		name:             "data-file-cache",
		createWhisper:    true,
		fillWhisper:      true,
		fillCache:        true,
		from:             now - 420,
		until:            now,
		now:              now,
		errIsNil:         true,
		dataIsNil:        false,
		cachePoints:      []point{{now - 123, 7.0}, {now - 119, 7.1}, {now - 45, 7.3}, {now - 243, 6.9}, {now - 67, 7.2}, {now + 3, 7.4}, {now + 67, 7.5}},
		expectedStep:     60,
		expectedValues:   []float64{0.1, 6.9, 0.3, 7.0, 7.2, 7.3, 7.4},
		expectedIsAbsent: []bool{false, false, false, false, false, false, false},
	},
	{
		name:             "data-cache",
		createWhisper:    true,
		fillWhisper:      false,
		fillCache:        true,
		from:             now - 420,
		until:            now,
		now:              now,
		errIsNil:         true,
		dataIsNil:        false,
		cachePoints:      []point{{now - 123, 7.0}, {now - 119, 7.1}, {now - 45, 7.3}, {now - 243, 6.9}, {now - 67, 7.2}},
		expectedStep:     60,
		expectedValues:   []float64{0.0, 6.9, 0.0, 7.0, 7.2, 7.3, 0.0},
		expectedIsAbsent: []bool{true, false, true, false, false, false, true},
	},
	{
		name:          "data-file-cache-long",
		createWhisper: true,
		fillWhisper:   true,
		fillCache:     true,
		from:          now - 6*day,
		until:         now,
		now:           now,
		errIsNil:      true,
		dataIsNil:     false,
		expectedStep:  60,
		retention:     "1m:7d",
	},
}

func getSingleMetricTest(name string) *FetchTest {
	for _, test := range singleMetricTests {
		if test.name == name {
			return &test
		}
	}
	return nil
}

func testFetchSingleMetricCommon(t *testing.T, test *FetchTest) {
	cache := cache.New()
	path, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	carbonserver := CarbonserverListener{
		whisperData: path,
		cacheGet:    cache.Get,
	}
	precision := 0.000001

	test.path = path
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

}

func TestFetchSingleMetricNonExisting(t *testing.T) {
	test := getSingleMetricTest("non-existing")
	testFetchSingleMetricCommon(t, test)
}

func TestFetchSingleMetricNonProperArchive(t *testing.T) {
	test := getSingleMetricTest("no-proper-archive")
	testFetchSingleMetricCommon(t, test)
}

/*
func TestFetchSingleMetricCrossRetention(t *testing.T) {
	test := getSingleMetricTest("cross-retention")
	testFetchSingleMetricCommon(t, test)
}
*/

func TestFetchSingleMetricDataFile(t *testing.T) {
	test := getSingleMetricTest("data-file")
	testFetchSingleMetricCommon(t, test)
}

func TestFetchSingleMetricDataFileCache(t *testing.T) {
	test := getSingleMetricTest("data-file-cache")
	testFetchSingleMetricCommon(t, test)
}

func TestFetchSingleMetricDataCache(t *testing.T) {
	test := getSingleMetricTest("data-cache")
	testFetchSingleMetricCommon(t, test)
}

func benchmarkFetchSingleMetricCommon(b *testing.B, test *FetchTest) {
	path, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(path)
	test.path = path
	cache := cache.New()

	carbonserver := CarbonserverListener{
		whisperData: test.path,
		cacheGet:    cache.Get,
	}
	// common

	// Non-existing metric
	err = generalFetchSingleMetricInit(test, cache)
	if err != nil {
		b.Fatalf("Unexpected error %v\n", err)
	}
	defer os.RemoveAll(test.path)

	b.ResetTimer()
	for runs := 0; runs < b.N; runs++ {
		data, err := generalFetchSingleMetricHelper(test, cache, &carbonserver)
		if err != nil {
			b.Errorf("Unexpected error: %v", err)
			return
		}
		if data == nil {
			b.Errorf("Unexpected empty data")
			return
		}
	}
	b.StopTimer()
	generalFetchSingleMetricRemove(test)
}

func BenchmarkFetchSingleMetricDataFile(b *testing.B) {
	test := getSingleMetricTest("data-file")
	benchmarkFetchSingleMetricCommon(b, test)
}

func BenchmarkFetchSingleMetricDataFileCache(b *testing.B) {
	test := getSingleMetricTest("data-file-cache")
	benchmarkFetchSingleMetricCommon(b, test)
}

func BenchmarkFetchSingleMetricDataCache(b *testing.B) {
	test := getSingleMetricTest("data-cache")
	benchmarkFetchSingleMetricCommon(b, test)
}

func BenchmarkFetchSingleMetricDataCacheLong(b *testing.B) {
	// Fetch and fill 6 days
	// Cache contains one day
	test := getSingleMetricTest("data-file-cache-long")

	l := 1 * day / int(test.expectedStep)
	test.cachePoints = make([]point, 0, l)
	val := float64(70)
	for t := now - l*int(test.expectedStep); t < now; t += int(test.expectedStep) {
		test.cachePoints = append(test.cachePoints, point{t, val})
		val += 10
	}

	benchmarkFetchSingleMetricCommon(b, test)
}
