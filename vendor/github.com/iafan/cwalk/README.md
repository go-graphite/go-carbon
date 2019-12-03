### cwalk = Concurrent filepath.Walk

A concurrent version of https://golang.org/pkg/path/filepath/#Walk function
that scans files in a directory tree and runs a callback for each file.

Since scanning (and callback execution) is done from within goroutines,
this may result in a significant performance boost on multicore systems
in cases when the bottleneck is the CPU, not the I/O.

My tests showed ~3.5x average speed increase on an 8-core CPU and 8 workers.
For measurements, I used the provided `bin/traversaltime.go` utility that measures
directory traversal time for both concurrent (`cwalk.Walk()`) and standard
(`filepath.Walk()`) functions.

Here are two common use cases when `cwalk` might be useful:

  1. You're doing subsequent scans of the same directory
     (e.g. monitoring it for changes), which means that the directory structure
     is likely cached in memory by OS;

  2. You're doing some CPU-heavy processing for each file in the callback.

### Installation

```shell
$ go get github.com/iafan/cwalk
```

### Usage

```go
import "github.com/iafan/cwalk"

...

func walkFunc(path string, info os.FileInfo, err error) error {
    ...
}

...

err := cwalk.Walk("/path/to/dir", walkFunc)
```

### Errors
An error such as a file limit being exceeded will be reported as `too many open files` for a particular file.  Each occurance of this is available in the returned error via the `type WalkerError struct`.  When errors are encountered the file walk will be completed prematurley, not all paths/files shall be walked.  You can check and access for errors like this:

```
if err != nil {
	fmt.Printf("Error : %s\n", err.Error())
	for _, errors := range err.(cwalk.WalkerError).ErrorList {
		fmt.Println(errors)
	}
}
```

### Differences from filepath.Walk

`filepath.Walk` sorts directory results while traversing the tree, which makes processing repeatable between runs. `cwalk.Walk()` processes files concurrentrly, sp there's no way to guarantee the order in which files or even folders are processed. If needed, you can sort the results once the entire tree is processed.
