package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// A native capture is private proof input, never a replacement checkpoint.
type nativeCapture struct {
	Source, WireFile, SHA256 string
	Names                    []string
	StoreFormat              int
	Bytes                    int64
	Captured                 time.Time
	Targets                  map[string]string `json:",omitempty"`
	TargetSHA256             map[string]string `json:",omitempty"`
	TargetNative             *nativeCapture    `json:",omitempty"`
}

func NativeCaptureMain(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("storectl capture-native", flag.ContinueOnError)
	source := flags.String("source", "", "donor Buckyd origin")
	namesPath := flags.String("names", "", "JSON array of planned names")
	out := flags.String("out", "", "new private capture metadata path; raw bytes use .transfer suffix")
	tokenEnv := flags.String("token-env", "BUCKYD_TOKEN", "root read token environment variable")
	if err := flags.Parse(args); err != nil {
		return err
	}
	endpoint, err := origin(*source)
	if err != nil {
		return err
	}
	if *namesPath == "" || *out == "" {
		return errors.New("capture requires --source --names --out")
	}
	data, err := os.ReadFile(*namesPath)
	if err != nil {
		return err
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return err
	}
	if len(names) == 0 {
		return errors.New("empty native capture")
	}
	client := &http.Client{Transport: &http.Transport{}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	m := &mover{options: options{tokenEnv: *tokenEnv}, client: client}
	var pin struct {
		ID          string
		StoreFormat int
	}
	if _, err := m.requestOnce(ctx, endpoint, "/v1/native/source", struct{}{}, &pin); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		req, e := http.NewRequestWithContext(cleanup, http.MethodDelete, endpoint+"/v1/native/source/"+pin.ID, nil)
		if e != nil {
			return
		}
		if token := os.Getenv(*tokenEnv); token != "" {
			req.Header.Set("X-Buckyd-Authorization", token)
		}
		res, e := client.Do(req)
		if e == nil {
			res.Body.Close()
		}
	}()
	body, _ := json.Marshal(struct {
		Session string
		Names   []string
	}{pin.ID, names})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/native/export", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := os.Getenv(*tokenEnv); token != "" {
		req.Header.Set("X-Buckyd-Authorization", token)
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return errors.New(string(raw))
	}
	wirePath := *out + ".transfer"
	wire, err := os.OpenFile(wirePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.Remove(wirePath)
		}
	}()
	hash := sha256.New()
	count, copyErr := io.Copy(io.MultiWriter(wire, hash), response.Body)
	if err := errors.Join(copyErr, wire.Sync(), wire.Close()); err != nil {
		return err
	}
	capture := nativeCapture{Source: endpoint, WireFile: filepath.Base(wirePath), SHA256: hex.EncodeToString(hash.Sum(nil)), Names: names, StoreFormat: pin.StoreFormat, Bytes: count, Captured: time.Now().UTC()}
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if err := errors.Join(json.NewEncoder(file).Encode(capture), file.Sync(), file.Close()); err != nil {
		return err
	}
	complete = true
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"capture": *out, "names": len(names), "native_bytes": count, "sha256": capture.SHA256, "store_format": pin.StoreFormat, "pin_released_on_exit": true})
}
