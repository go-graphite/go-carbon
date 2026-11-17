package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func copiedMoveParts(dir string) ([]v2core.MovePart, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	manifest, err := readPartSnapshot(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		return nil, err
	}
	var parts []v2core.MovePart
	for _, file := range manifest.Parts {
		// SSTs can be much larger than the bounded descriptor JSON.
		if file.Bytes > v2part.MaxDescriptorBytes {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, file.ID+".part"))
		if err != nil {
			return nil, err
		}
		descriptor, err := v2part.ParseDescriptor(b)
		if err != nil {
			continue
		}
		parts = append(parts, v2core.MovePart{Path: filepath.Join(dir, fmt.Sprintf("%x.part", descriptor.SHA256)), Descriptor: descriptor})
	}
	if len(parts) == 0 {
		return nil, errors.New("no copied descriptors")
	}
	return parts, nil
}
func moveAdmin(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("move", flag.ContinueOnError)
	endpoint := fs.String("url", "", "daemon HTTP URL")
	action := fs.String("action", "status", "prepare, adopt or status")
	id := fs.String("id", "", "prepared move ID")
	dir := fs.String("parts-dir", "", "received whole-file snapshot directory")
	now := fs.Int64("now", 0, "adoption Unix clock (default daemon clock)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *endpoint == "" {
		return errors.New("move requires --url")
	}
	method := http.MethodPost
	var body io.Reader
	switch *action {
	case "prepare":
		if *dir == "" {
			return errors.New("prepare requires --parts-dir")
		}
		parts, err := copiedMoveParts(*dir)
		if err != nil {
			return err
		}
		b, err := json.Marshal(v2daemon.MoveRequest{Parts: parts, Now: *now})
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	case "status":
		method = http.MethodGet
	case "adopt":
	default:
		return errors.New("unknown move action")
	}
	target := strings.TrimRight(*endpoint, "/") + "/admin/v2/move/" + *action + "?id=" + url.QueryEscape(*id)
	request, err := http.NewRequest(method, target, body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	client := http.Client{Timeout: 2 * time.Minute}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("move %s: %s", *action, b)
	}
	_, err = io.Copy(out, response.Body)
	return err
}
