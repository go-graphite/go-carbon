package v2backend

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/points"
)

func TestBinaryMetricCheckpointExactAndRejectsTruncation(t *testing.T) {
	m := &metricState{Name: []byte("foo.\xff\x00"), Group: []byte("group"), Policy: simplePolicy()}
	m.Policy.XFilesFactor = float32(math.Copysign(0, -1))
	m.Frontier.Order = math.MaxUint64
	m.Frontier.MaxTime = math.MinInt64
	for i := range m.Frontier.Policy {
		m.Frontier.Policy[i] = byte(i)
	}
	b := encodeMetric(m)
	got, err := decodeMetric(b)
	if err != nil || !reflect.DeepEqual(got, m) || math.Float32bits(got.Policy.XFilesFactor) != math.Float32bits(m.Policy.XFilesFactor) {
		t.Fatal("roundtrip", err)
	}
	for i := range b {
		if _, err = decodeMetric(b[:i]); err == nil {
			t.Fatalf("accepted prefix %d", i)
		}
	}
	if _, err = decodeMetric(append(b, 0)); err == nil {
		t.Fatal("accepted trailing byte")
	}
}
func TestLegacyCheckpointMigratesUntouchedMetrics(t *testing.T) {
	clock := int64(1794700800)
	opts := testOptions(t.TempDir(), simplePolicy())
	open := func() *Store {
		t.Helper()
		s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	s := open()
	for _, name := range []string{"one.metric", "untouched.metric"} {
		if e := s.Append(&points.Points{Metric: name, Data: []points.Point{{Timestamp: clock - 10, Value: 1}}}); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.Flush(); e != nil {
		t.Fatal(e)
	}
	o := s.owners[0]
	legacy := o.meta
	legacy.Version = 1
	for _, m := range o.metrics {
		legacy.Metrics = append(legacy.Metrics, m)
	}
	data, e := json.Marshal(legacy)
	if e != nil {
		t.Fatal(e)
	}
	b := o.db.NewBatch()
	p := checkpointPrefix(0, 'M')
	if e = b.DeleteRange(p, partsformat.PrefixEnd(p), nil); e != nil {
		t.Fatal(e)
	}
	p = checkpointPrefix(0, 'R')
	if e = b.DeleteRange(p, partsformat.PrefixEnd(p), nil); e != nil {
		t.Fatal(e)
	}
	if e = b.Set(metaKey(0), data, nil); e != nil {
		t.Fatal(e)
	}
	if e = b.Commit(pebble.Sync); e != nil {
		t.Fatal(e)
	}
	if e = b.Close(); e != nil {
		t.Fatal(e)
	}
	requireClose(t, s)
	s = open()
	if s.owners[0].meta.Version != 1 {
		t.Fatal("legacy path not exercised")
	}
	if e = s.Append(&points.Points{Metric: "one.metric", Data: []points.Point{{Timestamp: clock - 9, Value: 2}}}); e != nil {
		t.Fatal(e)
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	requireClose(t, s)
	s = open()
	defer requireClose(t, s)
	if len(s.owners[0].metrics) != 2 || s.owners[0].meta.Version != 2 {
		t.Fatal("migration lost untouched metrics")
	}
	if s.Stats().IngestedPoints != 3 {
		t.Fatal("migration replayed old receipt")
	}
}
