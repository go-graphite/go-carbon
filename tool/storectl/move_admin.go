package main

import (
	"bytes"
	"context"
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
	action := fs.String("action", "status", "copy, prepare, adopt or status")
	id := fs.String("id", "", "prepared move ID")
	dir := fs.String("parts-dir", "", "received whole-file snapshot directory")
	source := fs.String("from", "", "retained whole-file snapshot HTTP URL")
	tokenPath := fs.String("part-token-file", "", "donor bearer token file")
	now := fs.Int64("now", 0, "adoption Unix clock (default daemon clock)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *endpoint == "" {
		return errors.New("move requires --url")
	}
	if *action == "copy" {
		token := ""
		if *tokenPath != "" {
			b, e := os.ReadFile(*tokenPath)
			if e != nil {
				return e
			}
			token = strings.TrimSpace(string(b))
			if len(token) < 32 {
				return errors.New("part token must contain at least 32 characters")
			}
		}
		return copyMoveWithToken(*endpoint, *source, *id, token, *now, out)
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

// copyMove bridges the existing immutable whole-file HTTP protocol to the
// receiving daemon's durable move inventory. The client has no progress store.
func copyMove(endpoint, source, id string, now int64, out io.Writer) error {
	return copyMoveWithToken(endpoint, source, id, "", now, out)
}
func copyMoveWithToken(endpoint, source, id, token string, now int64, out io.Writer) error {
	client := &http.Client{Timeout: 2 * time.Minute}
	call := func(method, action string, body io.Reader) (v2daemon.MoveStatus, error) {
		var status v2daemon.MoveStatus
		r, e := http.NewRequest(method, strings.TrimRight(endpoint, "/")+"/admin/v2/move/"+action, body)
		if e != nil {
			return status, e
		}
		response, e := client.Do(r)
		if e != nil {
			return status, e
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			return status, fmt.Errorf("move: %s", b)
		}
		e = json.NewDecoder(response.Body).Decode(&status)
		return status, e
	}
	var status v2daemon.MoveStatus
	var err error
	if id != "" {
		status, err = call(http.MethodGet, "status?id="+url.QueryEscape(id), nil)
	} else {
		if source == "" {
			return errors.New("copy requires --from")
		}
		response, e := partGet(context.Background(), client, source, "/snapshot.json", token)
		if e != nil {
			return e
		}
		var snapshot v2core.PartSnapshot
		e = json.NewDecoder(io.LimitReader(response.Body, partManifestLimit+1)).Decode(&snapshot)
		response.Body.Close()
		if e != nil {
			return e
		}
		if e = snapshot.Validate(); e != nil {
			return e
		}
		b, e := json.Marshal(v2daemon.MoveCopyRequest{Snapshot: snapshot, Now: now})
		if e != nil {
			return e
		}
		status, err = call(http.MethodPost, "begin", bytes.NewReader(b))
	}
	if err != nil {
		return err
	}
	// Expose the stable move id before any large request, including to a client
	// killed mid-file. A replacement client needs only this id and donor URL.
	if err = json.NewEncoder(out).Encode(status); err != nil {
		return err
	}
	if status.Done {
		return nil
	}
	for _, p := range status.Parts {
		if p.State == "verified" || p.State == "staged" || p.State == "active" {
			continue
		}
		var body io.Reader
		var response *http.Response
		if p.State != "received" {
			if source == "" {
				return errors.New("unfinished copy requires --from")
			}
			response, err = partGet(context.Background(), client, source, "/parts/"+p.Name, token)
			if err != nil {
				return err
			}
			body = response.Body
		}
		_, err = call(http.MethodPut, "part?id="+url.QueryEscape(status.ID)+"&part="+p.Name, body)
		if response != nil {
			response.Body.Close()
		}
		if err != nil {
			return err
		}
	}
	status, err = call(http.MethodPost, "adopt?id="+url.QueryEscape(status.ID), nil)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(status)
}
