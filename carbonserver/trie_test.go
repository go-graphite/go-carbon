package carbonserver

import (
	"flag"
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
	"github.com/kr/pretty"
	"go.uber.org/zap"
)

var filesForIndex []string

func getTestFileList() []string {
	if len(filesForIndex) > 0 {
		return filesForIndex
	}

	rand.Seed(time.Now().Unix())
	// 10/100/50000
	limit0 := 1 // 0
	limit1 := 1 // 0
	limit2 := 5 // 00
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

	pretty.Println(filesForIndex)

	return filesForIndex
}

var trieServer *CarbonserverListener
var trigramServer *CarbonserverListener

func initServersForGlob(files []string) {
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
		for _, file := range files {
			trieIndex.insert(file)
		}
		fmt.Printf("trie index took %s\n", time.Now().Sub(start))

		listener.UpdateFileIndex(&fileIndex{
			trieIdx: trieIndex,
		})
		trieServer = &listener

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
		idx := trigram.NewIndex(files)
		fmt.Printf("trigram index took %s\n", time.Now().Sub(start))

		listener.UpdateFileIndex(&fileIndex{
			idx:   idx,
			files: files,
		})
		trigramServer = &listener
		wg.Done()
	}()

	wg.Wait()
	log.Printf("memory.Sizeof(trieServer)    = %+v\n", memory.Sizeof(trieServer))
	log.Printf("memory.Sizeof(trigramServer) = %+v\n", memory.Sizeof(trigramServer))
}

func init() {
	flag.Parse()
	if strings.Contains(strings.ToLower(flag.Lookup("test.bench").Value.String()), "index") {
		getTestFileList()
		initServersForGlob(filesForIndex)
	}
}

func TestTrieIndex(t *testing.T) {
	initServersForGlob([]string{
		"/something/else/server.wsp",
		"/service-00/server-000/metric-namespace-000/43081e003a315b88.wsp",
		"/service-00/server-000/metric-namespace-000/cpu.wsp",
		"/service-00/server-000/metric-namespace-001/d218bc1539f2cf8.wsp",
		"/service-00/server-000/metric-namespace-001/cpu.wsp",
		"/service-00/server-000/metric-namespace-005/cpu.wsp",
		"/service-00/server-000/metric-namespace-002/29370bc791c0fccb.wsp",
		"/service-00/server-000/metric-namespace-002/cpu.wsp",
		"/service-00/server-001/metric-namespace-002/cpu.wsp",
		"/service-00/server-001/metric-namespace-005/cpu.wsp",
		"/service-00/server-002/metric-namespace-003/64cd3228c99afc54.wsp",
		"/service-00/server-002/metric-namespace-003/cpu.wsp",
		"/service-00/server-002/metric-namespace-005/cpu.wsp",
		"/service-00/server-002/metric-namespace-004/6f31f9305c67895c.wsp",
		"/service-00/server-002/metric-namespace-004/cpu.wsp",
		"/service-01/server-000/metric-namespace-004/6f31f9305c67895c.wsp",
		"/service-01/server-000/metric-namespace-004/cpu.wsp",
		"/service-01/server-000/metric-namespace-005/cpu.wsp",
	})
	var cases = []struct {
		query  string
		expect []string
	}{
		// trigram index falls back to file system globbing for these cases
		// "*",
		// "*.*.*",
		// "service-01.*",

		{
			query:  "service-00",
			expect: []string{"service-00"},
		},
		{
			query:  "service-00.server-000.metric-namespace-000.cpu",
			expect: []string{"service-00.server-000.metric-namespace-000.cpu"},
		},
		{
			query: "service-00.server-000.metric-namespace-00[0-2].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-000.cpu",
				"service-00.server-000.metric-namespace-001.cpu",
				"service-00.server-000.metric-namespace-002.cpu",
			},
		},
		{
			query: "service-00.server-00[0-2].metric-namespace-00[0-2].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-000.cpu",
				"service-00.server-000.metric-namespace-001.cpu",
				"service-00.server-000.metric-namespace-002.cpu",
				"service-00.server-001.metric-namespace-002.cpu",
			},
		},
		{
			query: "service-00.*.metric-namespace-005.cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
			},
		},
		{
			query: "service-0*.*.metric-namespace-005.cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
				"service-01.server-000.metric-namespace-005.cpu",
			},
		},
		{
			query:  "*.*.metric-namespace-001.*",
			expect: []string{
				// "service-00.server-000.metric-namespace-005.cpu",
				// "service-00.server-001.metric-namespace-005.cpu",
				// "service-00.server-002.metric-namespace-005.cpu",
				// "service-01.server-000.metric-namespace-005.cpu",
			},
		},

		// "*.*.metric-namespace-001.*",
		// "*.*.metric-namespace-*.*",
		// "*.*.metric-namespace-*1.*",
		// "service-00.server-000.metric-*-0-*-xxx.cpu",
		// "service-00.server-000.metric-*-0-*-0-*-xxx.cpu",
		// "service-00.server-000.metric-*-0-*-[a-z]-*-xxx.cpu",

		// "service-00.server-000.metric-1-0-2-A-3-0-4-xxx.cpu",
		// "*.*.*-001.*",
		// "service-*.*.*-001.*",
		// "service-[0-2].*.*-001.*",
		// "service-[0-2].***.*-001.*",
		// "*.*.{metric-namespace-1,metric-namespace-3}.*",
	}
	for _, c := range cases {
		t.Logf("case: %s", c.query)

		trieFiles, _, err := trieServer.expandGlobs(c.query)
		if err != nil {
			t.Errorf("failed to trie.expandGlobs: %s", err)
		}

		// trigramFiles, _, err := trigramServer.expandGlobs(target)
		// if err != nil {
		// 	t.Errorf("failed to trigram.expandGlobs: %s", err)
		// }
		// for i, f := range trigramFiles {
		// 	f = f[len(trigramServer.whisperData+"/"):]
		// 	f = f[:len(f)-4]
		// 	trigramFiles[i] = strings.Replace(f, "/", ".", -1)
		// }

		sort.Strings(trieFiles)
		// sort.Strings(trigramFiles)

		if len(trieFiles) == 0 {
			t.Errorf("trie.expandGlobs fetched 0 files")
		}
		// if len(trigramFiles) == 0 {
		// 	t.Errorf("trigram.expandGlobs fetched 0 files")
		// }

		// if !reflect.DeepEqual(trieFiles, trigramFiles) {
		// 	t.Errorf("trie.expandGlobs returns:\t%s\ntrigram.expandGlobs returns:\t%s\n", trieFiles, trigramFiles)
		// }
		if !reflect.DeepEqual(trieFiles, c.expect) {
			t.Errorf("incorrect files retrieved\nreturns:\t%s\nexpect:\t%s\n", trieFiles, c.expect)
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

func TestGlobState(t *testing.T) {
	gs, err := newGlobState("a.[a-z0-9]bcd.*.d[0-9]")
	if err != nil {
		t.Fatal(err)
	}
	pretty.Println(gs)
}
