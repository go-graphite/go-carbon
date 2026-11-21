package downsize

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

// defaultBuckydPort is buckyd's default port when an endpoint names none.
const defaultBuckydPort = "4242"

// normalizeEndpoint turns a bare host, host:port or full origin into a
// scheme://host:port origin: no scheme means http, no port means 4242. It does
// no name resolution, so saved state keeps what the operator typed.
func normalizeEndpoint(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("invalid endpoint origin %q", raw)
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("invalid endpoint origin %q", raw)
	}
	if u.Port() == "" {
		u.Host = net.JoinHostPort(u.Hostname(), defaultBuckydPort)
	}
	return origin(strings.TrimSuffix(u.String(), "/"))
}

// lookupHost is replaced by tests.
var lookupHost = func(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

// resolveEndpoint replaces an http origin's host name with its address (IPv4
// first) because buckyd matches a pull source by the literal host against its
// ring nodes and transfer-sources, which list addresses. Addresses and https
// origins (certificates name hosts) pass unchanged.
func resolveEndpoint(ctx context.Context, raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	host := u.Hostname()
	if u.Scheme != "http" || net.ParseIP(host) != nil {
		return raw, nil
	}
	addrs, err := lookupHost(ctx, host)
	if err != nil || len(addrs) == 0 {
		return "", fmt.Errorf("cannot resolve host %q: %v", host, err)
	}
	pick := addrs[0]
	for _, a := range addrs {
		if ip := net.ParseIP(a); ip != nil && ip.To4() != nil {
			pick = a
			break
		}
	}
	u.Host = net.JoinHostPort(pick, u.Port())
	if pick != host {
		event("endpoint_resolved", map[string]any{"host": host, "address": u.Host})
	}
	return u.String(), nil
}

// endpointList is a repeatable, comma-separable --source/--from that stores
// normalized origins, so saved configs hold full URLs.
type endpointList []string

func (l *endpointList) String() string { return strings.Join(*l, ",") }

func (l *endpointList) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		o, err := normalizeEndpoint(part)
		if err != nil {
			return err
		}
		*l = append(*l, o)
	}
	return nil
}

// endpointValue is a single normalized --to.
type endpointValue struct{ p *string }

func (v endpointValue) String() string {
	if v.p == nil {
		return ""
	}
	return *v.p
}

func (v endpointValue) Set(value string) error {
	o, err := normalizeEndpoint(value)
	if err != nil {
		return err
	}
	*v.p = o
	return nil
}

// localBuckydEndpoint returns this host's own buckyd origin from the [buckyd]
// bind of the first candidate go-carbon config that has one (same candidate
// order as the default JWT secret), or "" with the config it came from.
func localBuckydEndpoint(configPath, procRoot string) (endpoint, config string) {
	candidates := []string{configPath}
	if configPath == "" {
		candidates = append(runningGoCarbonConfigs(procRoot), defaultGoCarbonConfig)
	}
	for _, path := range candidates {
		var cfg struct {
			Buckyd struct {
				Bind string `toml:"bind"`
			} `toml:"buckyd"`
		}
		data, err := os.ReadFile(path)
		if err != nil || toml.Unmarshal(data, &cfg) != nil || cfg.Buckyd.Bind == "" {
			continue
		}
		host, port, err := net.SplitHostPort(cfg.Buckyd.Bind)
		if err != nil {
			continue
		}
		if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
			host = "127.0.0.1"
		}
		return "http://" + net.JoinHostPort(host, port), path
	}
	return "", ""
}
