package chunkstore

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func catalogFixture() Metadata {
	return Metadata{MetricConfig: MetricConfig{
		Name:              "aggregations.secondly.example.by_region.eu.requests.count",
		Retentions:        []Retention{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}, {Step: 3600, Count: 8760}},
		AggregationMethod: Average, XFilesFactor: .5,
	}, ID: math.MaxUint64, Generation: 123456789}
}

func TestCatalogEncodingCompatibility(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, method := range []AggregationMethod{Average, Sum, First, Last, Min, Max} {
			m := catalogFixture()
			m.AggregationMethod = method
			data, err := (&Store{exactBlocks: exact}).encodeCatalog(m)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeCatalog(m.Name, data)
			if err != nil || !reflect.DeepEqual(got, m) {
				t.Fatalf("exact=%v: got=%+v want=%+v err=%v", exact, got, m, err)
			}
			if !exact {
				legacy, err := json.Marshal(m)
				if err != nil || string(data) != string(legacy) {
					t.Fatal("legacy encoding changed")
				}
			}
		}
	}
}

func TestBinaryCatalogRejectsMalformedValues(t *testing.T) {
	m := catalogFixture()
	data, err := (&Store{exactBlocks: true}).encodeCatalog(m)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(data); i++ {
		if _, err := decodeCatalog(m.Name, data[:i]); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 4 },
		func(b []byte) { clear(b[1:9]) },
		func(b []byte) { clear(b[9:17]) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[17:], 100) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[21:], math.Float32bits(float32(math.NaN()))) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[25:], math.MaxUint32) },
		func(b []byte) { clear(b[catalogHeaderSize : catalogHeaderSize+4]) },
	} {
		b := append([]byte(nil), data...)
		mutate(b)
		if _, err := decodeCatalog(m.Name, b); err == nil {
			t.Fatal("accepted malformed catalog value")
		}
	}
	if _, err := decodeCatalog(m.Name, append(data, 0)); err == nil {
		t.Fatal("accepted trailing byte")
	}
}

func BenchmarkDecodeCatalog(b *testing.B) {
	m := catalogFixture()
	for _, exact := range []bool{false, true} {
		name := "json"
		if exact {
			name = "binary"
		}
		data, err := (&Store{exactBlocks: exact}).encodeCatalog(m)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				if _, err := decodeCatalog(m.Name, data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
