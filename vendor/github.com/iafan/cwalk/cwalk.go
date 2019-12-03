package cwalk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// NumWorkers defines how many workers to run
// on each Walk() function invocation
var NumWorkers = runtime.GOMAXPROCS(0)

// BufferSize defines the size of the job buffer
var BufferSize = NumWorkers

// ErrNotDir indicates that the path, which is being passed
// to a walker function, does not point to a directory
var ErrNotDir = errors.New("Not a directory")

// WalkerError struct stores individual errors reported from each worker routine
type WalkerError struct {
	error error
	path  string
}

// WalkerErrorList struct store a list of errors reported from all worker routines
type WalkerErrorList struct {
	ErrorList []WalkerError
}

// Implement the error interface for WalkerError
func (we WalkerError) Error() string {
	return we.error.Error()
}

// Implement the error interface fo WalkerErrorList
func (wel WalkerErrorList) Error() string {
	if len(wel.ErrorList) > 0 {
		out := make([]string, len(wel.ErrorList))
		for i, err := range wel.ErrorList {
			out[i] = err.Error()
		}
		return strings.Join(out, "\n")
	}
	return ""
}

// Walker is constructed for each Walk() function invocation
type Walker struct {
	wg             sync.WaitGroup
	ewg            sync.WaitGroup // a separate wg for error collection
	jobs           chan string
	root           string
	followSymlinks bool
	walkFunc       filepath.WalkFunc
	errors         chan WalkerError
	errorList      WalkerErrorList // this is where we store the errors as we go
}

// the readDirNames function below was taken from the original
// implementation (see https://golang.org/src/path/filepath/path.go)
// but has sorting removed (sorting doesn't make sense
// in concurrent execution, anyway)

// readDirNames reads the directory named by dirname and returns
// a list of directory entries.
func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	return names, nil
}

// lstat is a wrapper for os.Lstat which accepts a path
// relative to Walker.root and also follows symlinks
func (w *Walker) lstat(relpath string) (info os.FileInfo, err error) {
	path := filepath.Join(w.root, relpath)
	info, err = os.Lstat(path)
	if err != nil {
		return nil, err
	}
	// check if this is a symlink
	if w.followSymlinks && info.Mode()&os.ModeSymlink > 0 {
		path, err = filepath.EvalSymlinks(path)
		if err != nil {
			return nil, err
		}
		info, err = os.Lstat(path)
		if err != nil {
			return nil, err
		}
	}
	return
}

// collectErrors processes any any errors passed via the error channel
// and stores them in the errorList
func (w *Walker) collectErrors() {
	defer w.ewg.Done()
	for err := range w.errors {
		w.errorList.ErrorList = append(w.errorList.ErrorList, err)
	}
}

// processPath processes one directory and adds
// its subdirectories to the queue for further processing
func (w *Walker) processPath(relpath string) error {
	defer w.wg.Done()

	path := filepath.Join(w.root, relpath)
	names, err := readDirNames(path)
	if err != nil {
		return err
	}

	for _, name := range names {
		subpath := filepath.Join(relpath, name)
		info, err := w.lstat(subpath)

		err = w.walkFunc(subpath, info, err)

		if err == filepath.SkipDir {
			return nil
		}

		if err != nil {
			w.errors <- WalkerError{
				error: err,
				path:  subpath,
			}
			continue
		}

		if info == nil {
			w.errors <- WalkerError{
				error: fmt.Errorf("Broken symlink: %s", subpath),
				path:  subpath,
			}
			continue
		}

		if info.IsDir() {
			w.addJob(subpath)
		}
	}
	return nil
}

// addJob increments the job counter
// and pushes the path to the jobs channel
func (w *Walker) addJob(path string) {
	w.wg.Add(1)
	select {
	// try to push the job to the channel
	case w.jobs <- path: // ok
	default: // buffer overflow
		// process job synchronously
		err := w.processPath(path)
		if err != nil {
			w.errors <- WalkerError{
				error: err,
				path:  path,
			}
		}
	}
}

// worker processes all the jobs
// until the jobs channel is explicitly closed
func (w *Walker) worker() {
	for path := range w.jobs {
		err := w.processPath(path)
		if err != nil {
			w.errors <- WalkerError{
				error: err,
				path:  path,
			}
		}
	}

}

// Walk recursively descends into subdirectories,
// calling walkFn for each file or directory
// in the tree, including the root directory.
func (w *Walker) Walk(relpath string, walkFn filepath.WalkFunc) error {
	w.errors = make(chan WalkerError, BufferSize)
	w.jobs = make(chan string, BufferSize)
	w.walkFunc = walkFn

	w.ewg.Add(1) // a separate error waitgroup so we wait until all errors are reported before exiting
	go w.collectErrors()

	info, err := w.lstat(relpath)
	err = w.walkFunc(relpath, info, err)
	if err == filepath.SkipDir {
		return nil
	}
	if err != nil {
		return err
	}

	if info == nil {
		return fmt.Errorf("Broken symlink: %s", relpath)
	}

	if !info.IsDir() {
		return ErrNotDir
	}

	// spawn workers
	for n := 1; n <= NumWorkers; n++ {
		go w.worker()
	}
	w.addJob(relpath) // add this path as a first job
	w.wg.Wait()       // wait till all paths are processed
	close(w.jobs)     // signal workers to close
	close(w.errors)   // signal errors to close
	w.ewg.Wait()      // wait for all errors to be collected

	if len(w.errorList.ErrorList) > 0 {
		return w.errorList
	}
	return nil
}

// Walk is a wrapper function for the Walker object
// that mimics the behavior of filepath.Walk,
// and doesn't follow symlinks.
func Walk(root string, walkFn filepath.WalkFunc) error {
	w := Walker{
		root: root,
	}
	return w.Walk("", walkFn)
}

// WalkWithSymlinks is a wrapper function for the Walker object
// that mimics the behavior of filepath.Walk, but follows
// directory symlinks.
func WalkWithSymlinks(root string, walkFn filepath.WalkFunc) error {
	w := Walker{
		root:           root,
		followSymlinks: true,
	}
	return w.Walk("", walkFn)
}
