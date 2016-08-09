package carbon

import (
	"fmt"
	"io/ioutil"
	"path"
	"testing"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/qa"
)

func TestRecover(t *testing.T) {
	qa.Root(t, func(root string) {
		w := func(fn, body string) {
			err := ioutil.WriteFile(path.Join(root, fn), []byte(body), 0644)
			if err != nil {
				t.Fatal(err)
			}
		}

		w("input.42.1470686967790091088", "m1 1 1470687039\n")
		w("cache.42.1470686967790091088", "m2 2 1470687039\n")

		w("input.15.1470687188677488571", "m3 3 1470687217\n")
		w("cache.15.1470687188677488571", "m4 4 1470687217\n")

		w("input.33.1470687188677488570", "m5 5 1470687217")
		w("cache.33.1470687188677488570", "")

		ch := make(chan *points.Points, 1024)

		Recover(root, ch)

		fmt.Println(len(ch))
	})
}
