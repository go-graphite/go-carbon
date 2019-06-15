package carbonserver

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
	"time"
)

func TestTrieIndex(t *testing.T) {
	trieIndex := newTrie(".wsp")
	data, err := ioutil.ReadFile("files.txt")
	if err != nil {
		panic(err)
	}

	start := time.Now()
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		trieIndex.insert(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	fmt.Printf("index took %s\n", time.Now().Sub(start))

	// input := bufio.NewScanner(os.Stdin)
	// for input.Scan() {
	// 	start := time.Now()
	// 	files, leaf := trieIndex.search(input.Text(), 100)
	// 	for i, file := range files {
	// 		fmt.Printf("  %s %t\n", file, leaf[i])
	// 	}
	// 	fmt.Printf("index took %s\n", time.Now().Sub(start))
	// }

}
