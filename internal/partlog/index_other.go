//go:build !linux && !darwin

package partlog

import "os"

func fileStamp(os.FileInfo) (segmentStamp, bool) { return segmentStamp{}, false }
