package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// This is a pipe protocol inside the existing audit runner, without a listener
// or any import capability. The production worker also requires a stopped,
// disabled go-carbon unit; the ordinary exclusive Pebble lock rejects live use.
func offlineNative(directory string, input io.Reader, output io.Writer) (err error) {
	if !filepath.IsAbs(directory) {
		return errors.New("absolute offline store directory required")
	}
	lock, err := os.Lstat(filepath.Join(directory, "LOCK"))
	if err != nil || !lock.Mode().IsRegular() {
		return errors.New("existing regular database LOCK required; never create a store")
	}
	marker, err := os.ReadFile(filepath.Join(directory, "CHUNKSTORE"))
	if err != nil {
		return err
	}
	var codec store.ContinuationCodec
	if strings.TrimSpace(string(marker)) != "go-carbon-chunks-v4" {
		codec = &whisperio.ContinuationCodec{}
	}
	source, err := store.OpenTransferSource(directory, store.TransferSourceOptions{Continuation: codec, CacheBytes: 128 << 20})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	pin, err := source.PinNativeMergeSource("full-verify-offline")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, pin.Close()) }()
	return offlineRequests(source, pin, input, output)
}

type offlineRequest struct {
	Op    string   `json:"op"`
	After string   `json:"after"`
	Names []string `json:"names"`
}

type boundedNativeWire struct{ bytes.Buffer }

func (w *boundedNativeWire) Write(p []byte) (int, error) {
	if len(p) > (64<<20)-w.Len() {
		return 0, store.ErrTransferLimit
	}
	return w.Buffer.Write(p)
}

func offlineRequests(source *store.TransferSource, pin *store.NativeSource, input io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var request offlineRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			return err
		}
		switch request.Op {
		case "list":
			rows, err := source.ListCatalogPage(context.Background(), "", request.After, 10000)
			if err != nil {
				return err
			}
			names := make([]string, len(rows))
			for i, row := range rows {
				names[i] = row.Name
			}
			if err := encoder.Encode(map[string]any{"names": names}); err != nil {
				return err
			}
		case "export":
			var wire boundedNativeWire
			err := pin.Write(context.Background(), request.Names, &wire)
			if errors.Is(err, store.ErrTransferLimit) || wire.Len() > 64<<20 {
				if err := encoder.Encode(map[string]any{"status": 413, "bytes": 0}); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if err := encoder.Encode(map[string]any{"status": 200, "bytes": wire.Len()}); err != nil {
				return err
			}
			if _, err := io.Copy(output, &wire.Buffer); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown read-only offline request %q", request.Op)
		}
	}
	return scanner.Err()
}
