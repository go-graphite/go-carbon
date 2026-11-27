//go:build !cgo

package v2part

// Pebble's pure-Go Zstd compressor emits different valid physical bytes.
const denseGoldenSuffix = "-nocgo"
