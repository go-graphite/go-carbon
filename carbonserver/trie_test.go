package carbonserver

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OneOfOne/go-utils/memory"
	"github.com/dgryski/go-trigram"
	"go.uber.org/zap"
)

var filesForIndex []string

func getTestFileList() []string {
	if len(filesForIndex) > 0 {
		return filesForIndex
	}

	rand.Seed(time.Now().Unix())
	// 10/100/50000
	limit0 := 10
	limit1 := 10
	limit2 := 500
	limit3 := 1
	filesForIndex = make([]string, 0, limit0*limit1*limit2*limit3)
	for i := 0; i < limit0; i++ {
		for j := 0; j < limit1; j++ {
			for k := 0; k < limit2; k++ {
				for l := 0; l < limit3; l++ {
					filesForIndex = append(filesForIndex, fmt.Sprintf("/service-%02d/server-%03d/metric-namespace-%03d/%x.wsp", i, j, k, rand.Int()))
				}
				filesForIndex = append(filesForIndex, fmt.Sprintf("/service-%02d/server-%03d/metric-namespace-%03d/cpu.wsp", i, j, k))
			}
		}
	}

	return filesForIndex
}

var trieServer *CarbonserverListener
var trigramServer *CarbonserverListener

func init() {
	getTestFileList()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		var listener CarbonserverListener
		listener.logger, _ = zap.NewDevelopment()
		listener.accessLogger, _ = zap.NewDevelopment()
		listener.trieIndex = true
		listener.whisperData = "./testdata"
		listener.maxGlobs = 100
		listener.failOnMaxGlobs = true

		start := time.Now()
		trieIndex := newTrie(".wsp")
		for _, file := range getTestFileList() {
			trieIndex.insert(file)
		}
		fmt.Printf("trie index took %s\n", time.Now().Sub(start))

		listener.UpdateFileIndex(&fileIndex{
			trieIdx: trieIndex,
		})
		trieServer = &listener

		log.Printf("memory.Sizeof(trieServer) = %+v\n", memory.Sizeof(trieServer))
		wg.Done()
	}()
	go func() {
		var listener CarbonserverListener
		listener.logger, _ = zap.NewDevelopment()
		listener.accessLogger, _ = zap.NewDevelopment()
		listener.trigramIndex = true
		listener.whisperData = "./testdata"
		listener.maxGlobs = 100
		listener.failOnMaxGlobs = true

		start := time.Now()
		files := getTestFileList()
		idx := trigram.NewIndex(files)
		fmt.Printf("trigram index took %s\n", time.Now().Sub(start))

		listener.UpdateFileIndex(&fileIndex{
			idx:   idx,
			files: files,
		})
		trigramServer = &listener
		log.Printf("memory.Sizeof(trigramServer) = %+v\n", memory.Sizeof(trigramServer))
		wg.Done()
	}()

	wg.Wait()
}

func TestTrieIndex(t *testing.T) {
	var cases = []string{
		// trigram index falls back to file system globbing for these cases
		// "*",
		// "*.*.*",
		// "service-01.*",

		"*.*.metric-namespace-100.*",
		"*.*.{metric-namespace-100,metric-namespace-300}.*",
	}
	for _, target := range cases {
		t.Logf("case: %s", target)

		trieFiles, _, err := trieServer.expandGlobs(target)
		if err != nil {
			t.Errorf("failed to trie.expandGlobs: %s", err)
		}

		trigramFiles, _, err := trigramServer.expandGlobs(target)
		if err != nil {
			t.Errorf("failed to trigram.expandGlobs: %s", err)
		}
		for i, f := range trigramFiles {
			f = f[len(trigramServer.whisperData+"/"):]
			f = f[:len(f)-4]
			trigramFiles[i] = strings.Replace(f, "/", ".", -1)
		}

		sort.Strings(trieFiles)
		sort.Strings(trigramFiles)

		if len(trieFiles) == 0 {
			t.Errorf("trie.expandGlobs fetched 0 files")
		}
		if len(trigramFiles) == 0 {
			t.Errorf("trigram.expandGlobs fetched 0 files")
		}

		if !reflect.DeepEqual(trieFiles, trigramFiles) {
			t.Errorf("trie.expandGlobs returns:\t%s\ntrigram.expandGlobs returns:\t%s\n", trieFiles, trigramFiles)
		}
	}
}

// func BenchmarkTrieIndexExact(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		target := filesForIndex[rand.Intn(len(filesForIndex))]
// 		trieServer.expandGlobs(target[1 : len(target)-4])
// 	}
// }
//
// func BenchmarkTrigramIndexExact(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		target := filesForIndex[rand.Intn(len(filesForIndex))]
// 		trigramServer.expandGlobs(target[1 : len(target)-4])
// 	}
// }

// /service-00/server-000/metric-namespace-000/355bf71b128f1749.wsp
func BenchmarkTrieIndexGlob1(b *testing.B) {
	// start := time.Now()
	// f, _, _ := trieServer.expandGlobs("service-*.server-*.metric-namespace-40*.*")
	// b.Logf("len(f) = %+v\n", len(f))
	// b.Logf("search took: %s", time.Now().Sub(start))
	for i := 0; i < b.N; i++ {
		trieServer.expandGlobs("service-*.server-*.metric-namespace-40*.*")
	}
}

func BenchmarkTrigramIndexGlob1(b *testing.B) {
	// f, _, _ := trigramServer.expandGlobs("service-*.server-*.metric-namespace-40*.*")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		trigramServer.expandGlobs("service-*.server-*.metric-namespace-40*.*")
	}
}

func BenchmarkTrieIndexGlob2(b *testing.B) {
	// f, _, _ := trieServer.expandGlobs("service-*.server-*.metric-namespace-[4]0*.*")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		trieServer.expandGlobs("service-*.server-*.metric-namespace-[4]0*.*")
	}
}

func BenchmarkTrigramIndexGlob2(b *testing.B) {
	// f, _, _ := trigramServer.expandGlobs("service-*.server-*.metric-namespace-[4]0*.*")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		trigramServer.expandGlobs("service-*.server-*.metric-namespace-[4]0*.*")
	}
}

func BenchmarkTrieIndexGlob3(b *testing.B) {
	// f, _, _ := trieServer.expandGlobs("service-*.server-*.*.cpu")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		trieServer.expandGlobs("service-*.server-*.*.cpu")
	}
}

func BenchmarkTrigramIndexGlob3(b *testing.B) {
	// f, _, _ := trigramServer.expandGlobs("service-*.server-*.*.cpu")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		trigramServer.expandGlobs("service-*.server-*.*.cpu")
	}
}
