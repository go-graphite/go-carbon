package carbonserver

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

var addMetrics = [...]string{
	"new.data.point1",
	"same.data.new.point1",
	"same.data.new.point2",
	"totally.new.point",
	"fresh.add",
}

var addFiles = [...]string{
	"path/to/file/name1.wsp",
	"path/to/file/name2.wsp",
	"path/to/file1/name1.wsp",
	"file/name1.wsp",
	"justname.wsp",
}

var removeFiles = [...]string{
	"path/to/file/name2.wsp",
	"path/to/file1/name1.wsp",
	"justname.wsp",
}

type testInfo struct {
	forceChan     chan struct{}
	exitChan      chan struct{}
	testCache     *cache.Cache
	csListener    *CarbonserverListener
	scanFrequency <-chan time.Time
	whisperDir    string
}

func addFileToSys(file string, tmpDir string) error {
	path := filepath.Dir(file)
	if err := addFilePathToDir(path, tmpDir); err != nil {
		return err
	}
	if nfile, err := os.OpenFile(filepath.Join(tmpDir, file), os.O_RDONLY|os.O_CREATE, 0644); err == nil {
		nfile.Close()
		return nil
	} else {
		return nil
	}
}

func addFilePathToDir(filePath string, tmpDir string) error {
	err := os.MkdirAll(filepath.Join(tmpDir, filePath), 0755)
	if err != nil {
		os.RemoveAll(tmpDir)
	}
	return err
}

func removeFileFromDir(filePath string, tmpDir string) error {
	return os.Remove(filepath.Join(tmpDir, filePath))
}

func getTestInfo(t *testing.T) *testInfo {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		fmt.Printf("unable to create test dir tree: %v\n", err)
		t.Fatal(err)
	}

	c := cache.New()
	c.InitCacheScanAdds()
	carbonserver := NewCarbonserverListener(c.Get)
	carbonserver.whisperData = tmpDir
	carbonserver.logger = zap.NewNop()
	carbonserver.cacheGetRecentMetrics = c.GetRecentNewMetrics
	carbonserver.metrics = &metricStruct{}
	carbonserver.exitChan = make(chan struct{})

	return &testInfo{
		forceChan:     make(chan struct{}),
		exitChan:      make(chan struct{}),
		scanFrequency: time.Tick(3 * time.Second),
		testCache:     c,
		csListener:    carbonserver,
		whisperDir:    tmpDir,
	}
}

func (f *testInfo) setTrigramOnly() {
	f.csListener.trigramIndex = true
	f.csListener.trieIndex = false
}

func (f *testInfo) setTrieOnly() {
	f.csListener.trigramIndex = false
	f.csListener.trieIndex = true
}

func (f *testInfo) checkExpandGlobs(t *testing.T, query string, shdExist bool) {
	fmt.Println("the query is - ", query)
	expandedGlobs, err := f.csListener.getExpandedGlobs(context.TODO(), zap.NewNop(), time.Now(), []string{query})
	if err != nil {
		t.Errorf("Unexpected err: '%v', expected: 'nil'", err)
		return
	}

	if expandedGlobs == nil {
		t.Errorf("No globs returned")
		return
	}

	fmt.Println("************* the expanded globs - ", expandedGlobs)

	if shdExist {
		file := expandedGlobs[0].Files[0]
		if file != query {
			t.Errorf("files: '%v', epxected: '%s'\n", file, query)
			return
		}
	} else if len(expandedGlobs[0].Files) != 0 {
		t.Errorf("expected no files but found - '%v'\n", expandedGlobs[0].Files)
		return
	}
}

func (f *testInfo) commonCacheIdxTestHelper(t *testing.T) {
	defer os.RemoveAll(f.whisperDir)

	for _, filePath := range addFiles {
		if err := addFileToSys(filePath, f.whisperDir); err != nil {
			t.Fatal(err)
		}
	}

	// trigger filescan
	go f.csListener.fileListUpdater(f.whisperDir, f.scanFrequency, f.forceChan, f.exitChan)
	f.forceChan <- struct{}{}
	time.Sleep(2 * time.Second)

	// add metrics to cache
	for i, metricName := range addMetrics {
		f.testCache.Add(points.OnePoint(metricName, float64(i), 10))
	}
	// check expandblobs for new metrics
	f.checkExpandGlobs(t, addMetrics[2], false)
	f.checkExpandGlobs(t, addMetrics[0], false)

	// pop metric from cache
	m1 := f.testCache.WriteoutQueue().Get(nil)
	p1, _ := f.testCache.PopNotConfirmed(m1)
	f.testCache.Confirm(p1)

	if !p1.Eq(points.OnePoint(addMetrics[4], 4, 10)) {
		fmt.Printf("error - recived wrong point - %v\n", p1)
		t.FailNow()
	}

	for _, filePath := range removeFiles {
		if err := removeFileFromDir(filePath, f.whisperDir); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(5 * time.Second)
	f.checkExpandGlobs(t, "path.to.file.name1", true)
	f.checkExpandGlobs(t, addMetrics[3], true)
	f.checkExpandGlobs(t, addMetrics[0], true)
	// f.checkExpandGlobs(t, addMetrics[4], false)

	// pop metric from cache
	m2 := f.testCache.WriteoutQueue().Get(nil)
	p2, _ := f.testCache.PopNotConfirmed(m2)
	f.testCache.Confirm(p2)

	// queue within cache is sorted by length of metric name
	if !p2.Eq(points.OnePoint(addMetrics[0], 0, 10)) {
		fmt.Printf("error - recived wrong point - %v\n", p2)
		t.FailNow()
	}
	f.checkExpandGlobs(t, addMetrics[0], true)

	// wait for next filewalk and check the metric again
	fmt.Println("wait for next filewalk and check the metric again")
	time.Sleep(4 * time.Second)
	// f.checkExpandGlobs(t, addMetrics[0], false)

	close(f.forceChan)
	close(f.exitChan)
}

func TestCacheIdxTrie(t *testing.T) {
	// get test info
	f := getTestInfo(t)
	f.setTrieOnly()
	f.commonCacheIdxTestHelper(t)
}

func TestCacheIdxTrigram(t *testing.T) {
	// get test info
	f := getTestInfo(t)
	f.setTrigramOnly()
	f.commonCacheIdxTestHelper(t)
}
