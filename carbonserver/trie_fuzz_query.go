//go:build fuzz_trie_query
// +build fuzz_trie_query

package carbonserver

import (
	"math/rand"
	"strings"
	"time"
)

// run instructions:
// 	mkdir -p fuzz/trie_query
// 	go-fuzz-build -tags fuzz_trie_query
// 	go-fuzz -workdir fuzz/trie_query

var trie = func() *trieIndex {
	trie := newTrie(".wsp")
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 1000; i++ {
		var nodes []string
		var numj = rand.Intn(20)
		for j := 0; j < numj+1; j++ {
			var node []byte
			var numz = rand.Intn(256)
			for z := 0; z < numz+1; z++ {
				node = append(node, byte(rand.Intn(256)))
			}

			nodes = append(nodes, string(node))
		}
		err := trie.insert(strings.Join(nodes, "/") + ".wsp")
		if err != nil {
			_, ok := err.(nilFilenameError)
			if !ok {
				panic(err)
			}
		}
	}

	return trie
}()

func Fuzz(data []byte) int {
	_, _, err := trie.query(string(data), 1000, func([]string) ([]string, error) { return nil, nil })
	if err != nil {
		// panic(err)
		return 0
	}
	return 0
}
