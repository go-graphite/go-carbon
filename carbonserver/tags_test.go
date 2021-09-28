package carbonserver

import (
	"log"
	"testing"
)

func TestTagIndex(t *testing.T) {
	ti := newTagsIndex()

	ti.addMetric("sys.cpu;host=app-00;dc=ams")
	ti.addMetric("sys.cpu;host=app-01;dc=ams")
	ti.addMetric("sys.cpu;host=app-02;dc=ams")
	ti.addMetric("sys.cpu;host=app-03;dc=ams")

	// metrics, err := ti.seriesByTag([]string{"sys.cpu;host=app-02"})
	metrics, err := ti.seriesByTag([]string{"host=app-02"})
	if err != nil {
		t.Error(err)
	}
	log.Printf("metrics = %+v\n", metrics)
}
