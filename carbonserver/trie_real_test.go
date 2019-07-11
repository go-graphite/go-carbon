// +build real

package carbonserver

import (
	"flag"
	"io/ioutil"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/OneOfOne/go-utils/memory"
)

var testDataPath = flag.String("testdata", "files.txt", "path to test file")
var checkMemory = flag.Bool("memory-size", false, "show index memory size")
var targetQueryPath = flag.String("query-data", "queries.txt", "queries for testing")
var carbonPath = flag.String("carbon", "/var/lib/carbon/whisper", "carbon data path")

func readFile(path string) []string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
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
	var trieServer, trigramServer *CarbonserverListener
	trieServer = newTrieServer(files)
	trigramServer = newTrigramServer(files)
	trieServer.whisperData = *carbonPath
	trigramServer.whisperData = *carbonPath

	if *checkMemory {
		t.Logf("memory.Sizeof(btrieServer)    = %+v\n", memory.Sizeof(trieServer))
		t.Logf("memory.Sizeof(btrigramServer) = %+v\n", memory.Sizeof(trigramServer))
	}

	queries := readFile(*targetQueryPath)
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			start1 := time.Now()
			trieFiles, trieLeafs, err := trieServer.expandGlobsTrie(query)
			if err != nil {
				t.Errorf("trie search errro: %s", err)
			}
			t.Logf("trie took %s", time.Now().Sub(start1))

			start2 := time.Now()
			trigramFiles, trigramLeafs, err := trigramServer.expandGlobs(query)
			if err != nil {
				t.Errorf("trigram search errro: %s", err)
			}
			t.Logf("trigram took %s", time.Now().Sub(start2))

			t.Logf("trie %d trigram %d", len(trieFiles), len(trigramFiles))

			sortFilesLeafs(trieFiles, trieLeafs)
			sortFilesLeafs(trigramFiles, trigramLeafs)

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
	oldIndex := map[string]int{}
	for i, f := range files {
		oldIndex[f] = i
	}
	sort.Strings(files)
	oldLeafs := make([]bool, len(leafs))
	copy(leafs, oldLeafs)
	for i, f := range files {
		leafs[i] = oldLeafs[oldIndex[f]]
	}
}
