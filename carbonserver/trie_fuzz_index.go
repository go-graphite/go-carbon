//go:build fuzz_trie_index
// +build fuzz_trie_index

package carbonserver

// run instructions:
// 	mkdir -p fuzz/trie_index
// 	go-fuzz-build -tags fuzz_trie_index
// 	go-fuzz -workdir fuzz/trie_index

var trie = newTrie(".wsp")

func Fuzz(data []byte) int {
	err := trie.insert(string(data))
	if err != nil {
		_, ok := err.(nilFilenameError)
		if !ok {
			panic(err)
		}
	}

	return 1
}
