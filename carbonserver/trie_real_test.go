package carbonserver

import (
	"flag"
	"io/ioutil"
	"log"
	"reflect"
	"strings"
	"testing"

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

func TestTrieGlobRealData(t *testing.T) {
	files := readFile(*testDataPath)
	var trieServer, trigramServer *CarbonserverListener
	trieServer = newTrieServer(files)
	trigramServer = newTrigramServer(files)
	trieServer.whisperData = *carbonPath
	trigramServer.whisperData = *carbonPath

	if *checkMemory {
		log.Printf("memory.Sizeof(btrieServer)    = %+v\n", memory.Sizeof(btrieServer))
		log.Printf("memory.Sizeof(btrigramServer) = %+v\n", memory.Sizeof(btrigramServer))
	}

	queries := readFile(*targetQueryPath)
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			trieFiles, trieLeafs, err := trieServer.expandGlobsTrie(query)
			if err != nil {
				t.Errorf("trie search errro: %s", err)
			}

			trigramFiles, trigramLeafs, err := trigramServer.expandGlobs(query)
			if err != nil {
				t.Errorf("trigram search errro: %s", err)
			}

			if !reflect.DeepEqual(trieFiles, trigramFiles) {
				t.Errorf("files diff\n\ttrie: %v\n\ttrigram: %v\n", trieFiles, trigramFiles)
			}
			if !reflect.DeepEqual(trieLeafs, trigramLeafs) {
				t.Errorf("leafs diff\n\ttrie: %v\n\ttrigram: %v\n", trieLeafs, trigramLeafs)
			}
		})
	}
}
