package carbonserver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFileListCacheV1(t *testing.T) {
	defer func() {
		os.Remove("flc_v1_test.txt")
		os.Remove("flc_v1_test.txt.tmp")
	}()

	flcv1w, err := NewFileListCache("flc_v1_test.txt", FLCVersion1, 'w')
	if err != nil {
		t.Fatal(err)
	}

	if err := flcv1w.Write(&FLCEntry{"sys/app/cpu/core0/system.wsp", 1024, 4096, 2048, 1652453112}); err != nil {
		t.Fatal(err)
	}
	if err := flcv1w.Write(&FLCEntry{"sys/app/cpu/core1/system.wsp", 1024, 4096, 2048, 1652453112}); err != nil {
		t.Fatal(err)
	}
	if err := flcv1w.Write(&FLCEntry{"sys/app/cpu/core2/system.wsp", 1024, 4096, 2048, 1652453112}); err != nil {
		t.Fatal(err)
	}
	flcv1w.Close()

	t.Run("v1_reader_with_v1_data", func(t *testing.T) {
		flcv1r, err := NewFileListCache("flc_v1_test.txt", FLCVersion1, 'r')
		if err != nil {
			t.Fatal(err)
		}
		var v1data []string
		for {
			entry, eof := flcv1r.Read()
			if eof != nil {
				break
			}

			v1data = append(v1data, entry.Path)
		}
		flcv1r.Close()

		if got, want := v1data, []string{
			"sys/app/cpu/core0/system.wsp",
			"sys/app/cpu/core1/system.wsp",
			"sys/app/cpu/core2/system.wsp",
		}; !reflect.DeepEqual(got, want) {
			t.Errorf("v1data = %s; want %s", got, want)
		}
	})

	t.Run("v2_reader_with_v1_data", func(t *testing.T) {
		flcv2r, err := NewFileListCache("flc_v1_test.txt", FLCVersion2, 'r')
		if err != nil {
			t.Fatal(err)
		}
		var v2data []string
		for {
			entry, eof := flcv2r.Read()
			if eof != nil {
				break
			}

			v2data = append(v2data, entry.Path)
		}
		flcv2r.Close()

		if got, want := v2data, []string{
			"sys/app/cpu/core0/system.wsp",
			"sys/app/cpu/core1/system.wsp",
			"sys/app/cpu/core2/system.wsp",
		}; !reflect.DeepEqual(got, want) {
			t.Errorf("v2data = %s; want %s", got, want)
		}
	})
}

func TestFileListCacheV2(t *testing.T) {
	defer func() {
		os.Remove("flc_v2_test.txt")
		os.Remove("flc_v2_test.txt.tmp")
	}()

	flcv2w, err := NewFileListCache("flc_v2_test.txt", FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}

	var testData []*FLCEntry
	for i := int64(0); i < 409600; i++ {
		entry := &FLCEntry{
			Path:         fmt.Sprintf("sys/app/cpu/core-%d/system.wsp", i),
			DataPoints:   10240 + i,
			LogicalSize:  40960 + i,
			PhysicalSize: 20480 + i,
			FirstSeenAt:  1652453112,
		}
		testData = append(testData, entry)

		if err := flcv2w.Write(entry); err != nil {
			t.Fatal(err)
		}
	}
	flcv2w.Close()

	flcv2r, err := NewFileListCache("flc_v2_test.txt", FLCVersion2, 'r')
	if err != nil {
		t.Fatal(err)
	}
	var v2data []*FLCEntry
	for {
		entry, err := flcv2r.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Error(err)
			}
			break
		}

		v2data = append(v2data, entry)
	}
	flcv2r.Close()

	if got, want := v2data, testData; !cmp.Equal(got, want) {
		t.Errorf("%s", cmp.Diff(got, want))
	}
}
