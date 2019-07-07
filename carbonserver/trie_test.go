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
	"go.uber.org/zap"
)

var filesForIndex []string

func getTestFileList() []string {
	if len(filesForIndex) > 0 {
		return filesForIndex
	}

	rand.Seed(time.Now().Unix())
	// 10/100/50000
	limit0 := 50
	limit1 := 50
	limit2 := 100
	limit3 := 1
	filesForIndex = make([]string, 0, limit0*limit1*limit2*limit3)
	for i := 0; i < limit0; i++ {
		for j := 0; j < limit1; j++ {
			for k := 0; k < limit2; k++ {
				// for l := 0; l < limit3; l++ {
				// 	filesForIndex = append(filesForIndex, fmt.Sprintf("/service-%02d/server-%03d/metric-namespace-%03d/%x.wsp", i, j, k, rand.Int()))
				// }
				filesForIndex = append(filesForIndex, fmt.Sprintf("/service-%02d/server-%03d/metric-namespace-%03d/cpu.wsp", i, j, k))
			}
		}
	}

	// fmt.Println(filesForIndex[len(filesForIndex)-30:])

	return filesForIndex
}

var btrieServer *CarbonserverListener
var btrigramServer *CarbonserverListener

func newTrieServer(files []string) *CarbonserverListener {
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

	return &listener
}

func newTrigramServer(files []string) *CarbonserverListener {
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

	return &listener
}

func initServersForGlob(files []string) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		btrieServer = newTrieServer(files)
		wg.Done()
	}()
	go func() {
		btrigramServer = newTrigramServer(files)
		wg.Done()
	}()

	wg.Wait()
	log.Printf("memory.Sizeof(btrieServer)    = %+v\n", memory.Sizeof(btrieServer))
	log.Printf("memory.Sizeof(btrigramServer) = %+v\n", memory.Sizeof(btrigramServer))
}

func init() {
	flag.Parse()
	if strings.Contains(strings.ToLower(flag.Lookup("test.bench").Value.String()), "index") {
		getTestFileList()
		initServersForGlob(filesForIndex)
	}
}

func TestTrieIndex(t *testing.T) {
	commonFiles := []string{
		// service-0*.*.metric-*-00[5-7].cpu
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

		"/service-01/server-110/metric-namespace-007/cpu.wsp",
		"/service-01/server-120/metric-namespace-007/cpu.wsp",
		"/service-01/server-170/metric-namespace-007/cpu.wsp",

		"/service-01/server-114/metric-namespace-007/cpu.wsp",
		"/service-01/server-125/metric-namespace-007/cpu.wsp",
		"/service-01/server-12a/metric-namespace-007/cpu.wsp",
		"/service-01/server-149/metric-namespace-007/cpu.wsp",

		"/service-01/server-125/metric-namespzce-007/cpu.wsp",

		// service-0*.*.metric-*-00[5-7]-xdp.cpu
		"/service-01/server-170/metric-namespace-004-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-005-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-008-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-006-xdp/cpu.wsp",
	}
	var cases = []struct {
		input  []string
		query  string
		expect []string
	}{
		// trigram index falls back to file system globbing for these cases
		// "*",
		// "*.*.*",
		// "service-01.*",

		{
			input:  commonFiles,
			query:  "service-00",
			expect: []string{"service-00"},
		},
		{
			input:  commonFiles,
			query:  "service-00.server-000.metric-namespace-000.cpu",
			expect: []string{"service-00.server-000.metric-namespace-000.cpu"},
		},

		{
			input: commonFiles,
			query: "service-00.server-000.metric-namespace-00[0-2].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-000.cpu",
				"service-00.server-000.metric-namespace-001.cpu",
				"service-00.server-000.metric-namespace-002.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-00.server-00[0-2].metric-namespace-00[0-2].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-000.cpu",
				"service-00.server-000.metric-namespace-001.cpu",
				"service-00.server-000.metric-namespace-002.cpu",
				"service-00.server-001.metric-namespace-002.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2]0.metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-110.metric-namespace-007.cpu",
				"service-01.server-120.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-5][4-5a-z].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
				"service-01.server-12a.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[1]4.metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2][4-5].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
			},
		},
		{
			input: []string{
				"/service-01/server-114/metric-namespace-007/cpu.wsp",
				"/service-01/server-125/metric-namespace-007/cpu.wsp",
				"/service-01/server-125/metric-namespace-006/cpu.wsp",
				"/service-01/server-111/metric-namespace-007/cpu.wsp",
				"/service-01/server-11a/metric-namespace-007/cpu.wsp",
				"/service-01/something-125/metric-namespace-007/cpu.wsp",
			},
			query: "service-01.server-1[0-2][^4-5].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-111.metric-namespace-007.cpu",
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-11a.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2][4-5].metric-n[a-z]mesp[a-z1-9]ce-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespzce-007.cpu",
			},
		},

		{
			input: commonFiles,
			query: "service-00.*.metric-namespace-005.cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
			},
		},

		{
			input: commonFiles,
			// query: "*.*.metric-namespace-001.*",
			query: "*",
			expect: []string{
				"service-00",
				"service-01",
				"something",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-namespace-005.cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
				"service-01.server-000.metric-namespace-005.cpu",
			},
		},
		// {
		//  input: commonFiles,
		// 	query: "service-0*.*.metric-*-00[5-7].cpu",
		// 	expect: []string{
		// 		"service-00.server-000.metric-namespace-005.cpu",
		// 		"service-00.server-001.metric-namespace-005.cpu",
		// 		"service-00.server-002.metric-namespace-005.cpu",
		// 		"service-01.server-000.metric-namespace-005.cpu",
		// 	},
		// },
		{
			input: commonFiles,
			query: "service-0*.*.metric-*-00[5-7]-xdp.cpu",
			expect: []string{
				"service-01.server-170.metric-namespace-004-007-xdp.cpu",
				"service-01.server-170.metric-namespace-006-xdp.cpu",
				"service-01.server-170.metric-namespace-007-005-xdp.cpu",
				"service-01.server-170.metric-namespace-007-007-xdp.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.{metric-namespace-004-007-xdp,metric-namespace-007-007-xdp}.cpu",
			expect: []string{
				"service-01.server-170.metric-namespace-004-007-xdp.cpu",
				"service-01.server-170.metric-namespace-007-007-xdp.cpu",
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
	log.SetFlags(log.Lshortfile)
	for _, c := range cases {
		t.Run(c.query, func(t *testing.T) {
			t.Logf("case: %s", c.query)

			trieServer := newTrieServer(c.input)
			trieFiles, _, err := trieServer.expandGlobsTrie(c.query)
			if err != nil {
				t.Errorf("failed to trie.expandGlobs: %s", err)
			}

			// trigramServer := newTrigramServer(c.input)
			// trigramFiles, _, err := trigramServer.expandGlobs(target)
			// if err != nil {
			// 	t.Errorf("failed to trigram.expandGlobs: %s", err)
			// }
			// for i, f := range trigramFiles {
			// 	f = f[len(btrigramServer.whisperData+"/"):]
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
				t.Errorf("incorrect files retrieved\nreturns: %s\nexpect:  %s\n", trieFiles, c.expect)
			}
		})
	}
}

// func BenchmarkTrieIndexExact(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		target := filesForIndex[rand.Intn(len(filesForIndex))]
// 		btrieServer.expandGlobs(target[1 : len(target)-4])
// 	}
// }
//
// func BenchmarkTrigramIndexExact(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		target := filesForIndex[rand.Intn(len(filesForIndex))]
// 		btrigramServer.expandGlobs(target[1 : len(target)-4])
// 	}
// }

// /service-00/server-000/metric-namespace-000/355bf71b128f1749.wsp
func BenchmarkTrieIndexGlob1(b *testing.B) {
	// start := time.Now()
	// f, _, _ := btrieServer.expandGlobsTrie("service-*.server-*.metric-namespace-40*.*")
	// b.Logf("len(f) = %+v\n", len(f))
	// b.Logf("search took: %s", time.Now().Sub(start))
	for i := 0; i < b.N; i++ {
		btrieServer.expandGlobsTrie("service-*.server-*.metric-namespace-40*.*")
	}
}

func BenchmarkTrigramIndexGlob1(b *testing.B) {
	// f, _, _ := btrigramServer.expandGlobs("service-*.server-*.metric-namespace-40*.*")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		btrigramServer.expandGlobs("service-*.server-*.metric-namespace-40*.*")
	}
}

func BenchmarkTrieIndexGlob2(b *testing.B) {
	// f, _, _ := btrieServer.expandGlobsTrie("service-*.server-*.metric-namespace-[4]0*.*")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		btrieServer.expandGlobsTrie("service-*.server-*.metric-namespace-[4]0*.*")
	}
}

func BenchmarkTrigramIndexGlob2(b *testing.B) {
	// f, _, _ := btrigramServer.expandGlobs("service-*.server-*.metric-namespace-[4]0*.*")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		btrigramServer.expandGlobs("service-*.server-*.metric-namespace-[4]0*.*")
	}
}

func BenchmarkTrieIndexGlob3(b *testing.B) {
	// f, _, _ := btrieServer.expandGlobsTrie("service-*.server-*.*.cpu")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		btrieServer.expandGlobsTrie("service-*.server-*.*.cpu")
	}
}

func BenchmarkTrigramIndexGlob3(b *testing.B) {
	// f, _, _ := btrigramServer.expandGlobs("service-*.server-*.*.cpu")
	// b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		btrigramServer.expandGlobs("service-*.server-*.*.cpu")
	}
}

// /service-49/server-049/metric-namespace-099/cpu.wsp
func BenchmarkTrieIndexGlob4(b *testing.B) {
	f, _, _ := btrieServer.expandGlobsTrie("service-2[3-4].server-02[1-9].metric-namespace-0[2-3]0.cpu")
	b.Logf("len(f) = %+v\n", len(f))
	// fmt.Println(f)
	for i := 0; i < b.N; i++ {
		btrieServer.expandGlobsTrie("service-2[3-4].server-02[1-9].metric-namespace-0[2-3]0.cpu")
	}
}

func BenchmarkTrigramIndexGlob4(b *testing.B) {
	f, _, _ := btrigramServer.expandGlobs("service-2[3-4].server-02[1-9].metric-namespace-0[2-3]0.cpu")
	b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		btrigramServer.expandGlobs("service-2[3-4].server-02[1-9].metric-namespace-0[2-3]0.cpu")
	}
}

// /service-49/server-049/metric-namespace-099/cpu.wsp
func BenchmarkTrieIndexGlob5(b *testing.B) {
	f, _, _ := btrieServer.expandGlobsTrie("service-23.server-029.metric-namespace-030.cpu")
	b.Logf("len(f) = %+v\n", len(f))
	// fmt.Println(f)
	for i := 0; i < b.N; i++ {
		btrieServer.expandGlobsTrie("service-23.server-029.metric-namespace-030.cpu")
	}
}

func BenchmarkTrigramIndexGlob5(b *testing.B) {
	f, _, _ := btrigramServer.expandGlobs("service-23.server-029.metric-namespace-030.cpu")
	b.Logf("len(f) = %+v\n", len(f))
	for i := 0; i < b.N; i++ {
		btrigramServer.expandGlobs("service-23.server-029.metric-namespace-030.cpu")
	}
}
