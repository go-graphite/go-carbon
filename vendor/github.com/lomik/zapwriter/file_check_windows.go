// +build windows

package zapwriter

func (r *FileOutput) doWithCheck(f func()) {
	f()
}
