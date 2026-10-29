//go:build !unix

package chunkstore

import whisper "github.com/go-graphite/go-whisper"

var continuationPhysicalReadFS whisper.StorageFileSystem
