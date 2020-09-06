// +build real

package carbonserver

import (
	"context"
	"flag"
	"io/ioutil"
	"reflect"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OneOfOne/go-utils/memory"
)

var testDataPath = flag.String("testdata", "files.txt", "path to test file")
var checkMemory = flag.Bool("memory-size", false, "show index memory size")
var targetQueryPath = flag.String("query-data", "queries.txt", "queries for testing")
var carbonPath = flag.String("carbon", "/var/lib/carbon/whisper", "carbon data path")
var noTrigram = flag.Bool("no-trigram", false, "disable trigram search")
var pureTrie = flag.Bool("no-trie-with-trigram", false, "enable trigram in trie")
var localTest = flag.Bool("local-test", false, "trim prefix and replace / with . on trigram result")
var excludeFilesNotInTestData = flag.Bool("exclude-non-test-files", false, "filter out files trigram matched by using filepath.Glob")

func readFile(path string) []string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	// if strings.HasSuffix(path, ".tar.gz") || strings.HasSuffix(path, ".tgz") {
	// 	tar.NewReader()
	// }

	lines := strings.Split(string(data), "\n")
	if lines[len(lines)-1] == "" {
		return lines[:len(lines)-1]
	}
	return lines
}

// TestTrieGlobRealData is built for benchmarking against real data, for two reasons:
// 		* trigram index solution invokes stat syscall
// 		* can't share production data in open source project
func TestTrieGlobRealData(t *testing.T) {
	files := readFile(*testDataPath)
	filesm := make(map[string]bool)
	dirm := make(map[string]bool)
	for _, f := range files {
		f = strings.TrimSuffix(f, ".wsp")
		f = strings.TrimPrefix(f, "/")
		filesm[f] = true
		dirs := strings.Split(f, "/")
		for i := range dirs[:len(dirs)-1] {
			dirm[strings.Join(dirs[:i+1], "/")] = true
		}
	}
	var trieServer, trigramServer *CarbonserverListener

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		trieServer = newTrieServer(files, !*pureTrie)
		trieServer.whisperData = *carbonPath

		if *checkMemory {
			start := time.Now()
			t.Logf("memory.Sizeof(btrieServer)\t\t= %+v took %s\n", memory.Sizeof(trieServer), time.Now().Sub(start)) //nolint:gosimple
		}
		wg.Done()
	}()

	if !*noTrigram {
		wg.Add(1)
		go func() {
			trigramServer = newTrigramServer(files)
			trigramServer.whisperData = *carbonPath

			if *checkMemory {
				start := time.Now()
				t.Logf("memory.Sizeof(btrigramServer)\t\t= %+v took %s\n", memory.Sizeof(trigramServer), time.Now().Sub(start)) //nolint:gosimple
			}
			wg.Done()
		}()
	}
	wg.Wait()

	queries := readFile(*targetQueryPath)
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%q: %s", query, r)
					debug.PrintStack()
				}
			}()
			start1 := time.Now()
			trierc := make(chan *ExpandedGlobResponse, 1)
			trieServer.expandGlobs(context.TODO(), query, trierc)
			trier := <-trierc
			trieFiles, trieLeafs, err := trier.Files, trier.Leafs, trier.Err
			if err != nil {
				t.Errorf("trie search error: %s", err)
			}
			trieTime := time.Now().Sub(start1) //nolint:gosimple
			t.Logf("trie took %s", trieTime)

			// for i, str := range trieFiles {
			// 	log.Printf("%d: %s %t\n", i, str, trieLeafs[i])
			// }
			// log.Printf("len(trieFiles) = %+v\n", len(trieFiles))

			if *noTrigram {
				return
			}

			start2 := time.Now()
			trigramrc := make(chan *ExpandedGlobResponse, 1)
			trigramServer.expandGlobs(context.TODO(), query, trigramrc)
			trigramr := <-trigramrc
			trigramFiles, trigramLeafs, err := trigramr.Files, trigramr.Leafs, trigramr.Err
			// log.Printf("trigramLeafs = %+v\n", trigramLeafs)
			if err != nil {
				t.Errorf("trigram search error: %s", err)
			}
			trigramTime := time.Now().Sub(start2) //nolint:gosimple
			t.Logf("trigram took %s", trigramTime)

			if *localTest {
				for i := range trigramFiles {
					trigramFiles[i] = strings.TrimPrefix(trigramFiles[i], *carbonPath+"/")
					trigramFiles[i] = strings.TrimSuffix(trigramFiles[i], ".wsp")
					trigramFiles[i] = strings.Replace(trigramFiles[i], "/", ".", -1)
				}
			}
			if *excludeFilesNotInTestData {
				var index int
				for i := range trigramFiles {
					name := strings.Replace(trigramFiles[i], ".", "/", -1)
					if (trigramLeafs[i] && filesm[name]) || (!trigramLeafs[i] && dirm[name]) {
						trigramFiles[index] = trigramFiles[i]
						trigramLeafs[index] = trigramLeafs[i]
						index++
					}
				}
				// if index == 0 {
				// 	index = 1
				// }
				trigramFiles = trigramFiles[:index]
				trigramLeafs = trigramLeafs[:index]
			}

			if trieTime < trigramTime {
				t.Logf("trie is %f times faster", float64(trigramTime)/float64(trieTime))
			} else {
				t.Logf("trie is %f times slower", float64(trieTime)/float64(trigramTime))
			}

			t.Logf("trie %d trigram %d (raw)", len(trieFiles), len(trigramFiles))

			sortFilesLeafs(trieFiles, trieLeafs)
			sortFilesLeafs(trigramFiles, trigramLeafs)

			// trigram index is returning duplicated result, might be due to my test
			// environment, adding a simple de-duplication here to reduce noise.
			trigramFiles, trigramLeafs = uniqFilesLeafs(trigramFiles, trigramLeafs)

			t.Logf("trie %d trigram %d (unique)", len(trieFiles), len(trigramFiles))

			if trieFiles == nil {
				trieFiles = []string{}
			}
			if trigramFiles == nil {
				trigramFiles = []string{}
			}
			if trieLeafs == nil {
				trieLeafs = []bool{}
			}
			if trigramLeafs == nil {
				trigramLeafs = []bool{}
			}

			if !reflect.DeepEqual(trieFiles, trigramFiles) {
				t.Errorf("files diff\n\ttrie:    %v\n\ttrigram: %v\n", trieFiles, trigramFiles)
			}
			if trieLeafs == nil {
				trieLeafs = []bool{}
			}
			if !reflect.DeepEqual(trieLeafs, trigramLeafs[:]) {
				t.Errorf("leafs diff\n\ttrie:    %#v\n\ttrigram: %#v\n", trieLeafs, trigramLeafs)
			}
		})
	}
}

func sortFilesLeafs(files []string, leafs []bool) {
	if len(files) <= 1 {
		return
	}

	oldIndex := map[string]int{}
	for i, f := range files {
		oldIndex[f] = i
	}
	sort.Strings(files)
	oldLeafs := make([]bool, len(leafs))
	copy(oldLeafs, leafs)
	for i, f := range files {
		leafs[i] = oldLeafs[oldIndex[f]]
	}
}

func uniqFilesLeafs(files []string, leafs []bool) ([]string, []bool) {
	if len(files) <= 1 {
		return files, leafs
	}
	var index = 0
	for i := 1; i < len(files); i++ {
		if files[index] == files[i] {
			continue
		}
		index++
		files[index] = files[i]
		leafs[index] = leafs[i]
	}
	return files[:index+1], leafs[:index+1]
}
