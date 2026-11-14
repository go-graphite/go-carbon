// Package remote provides bounded authenticated maintenance requests.
package remote

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Client struct {
	Endpoint string
	Secret   []byte
	HTTP     *http.Client
	Apply    bool
}

func New(endpoint, secretFile string, apply bool) (*Client, error) {
	secret, err := os.ReadFile(secretFile)
	if err != nil {
		return nil, err
	}
	secret = bytes.TrimSpace(secret)
	if len(secret) == 0 {
		return nil, fmt.Errorf("empty auth secret")
	}
	return &Client{Endpoint: strings.TrimRight(endpoint, "/"), Secret: secret, Apply: apply, HTTP: &http.Client{Timeout: 120 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Do(method, path string, body, result any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, c.Endpoint+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	ops := []string{"read"}
	if c.Apply {
		ops = append(ops, "update")
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(5 * time.Minute).Unix(), "namespaces": []string{"*"}, "ops": ops}).SignedString(c.Secret)
	if err != nil {
		return err
	}
	req.Header.Set("X-Buckyd-Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	const bound = 8 << 20
	b, err := io.ReadAll(io.LimitReader(resp.Body, bound+1))
	if err != nil {
		return err
	}
	if len(b) > bound {
		return fmt.Errorf("maintenance response exceeds 8MiB")
	}
	return json.Unmarshal(b, result)
}
