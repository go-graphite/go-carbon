package buckyd

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// transferSourceOrigin constructs a destination from the configured allowlist.
// The caller selects a trusted host and a numeric API port; its URL is never
// forwarded as a destination, including when a signed read token is attached.
func (s *Service) transferSourceOrigin(source string) (*url.URL, error) {
	u, err := url.Parse(source)
	if err != nil || u.Host == "" || u.User != nil || u.Opaque != "" || strings.Trim(u.Path, "/") != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("invalid transfer source origin")
	}
	var scheme string
	var port uint64
	switch u.Scheme {
	case "http":
		scheme, port = "http", 80
	case "https":
		scheme, port = "https", 443
	default:
		return nil, errors.New("invalid transfer source scheme")
	}
	if text := u.Port(); text != "" {
		port, err = strconv.ParseUint(text, 10, 16)
		if err != nil || port == 0 {
			return nil, errors.New("invalid transfer source port")
		}
	}
	origin := func(host string) *url.URL {
		return &url.URL{Scheme: scheme, Host: net.JoinHostPort(host, strconv.FormatUint(port, 10))}
	}
	for _, node := range s.nodes {
		if strings.EqualFold(node.Server, u.Hostname()) {
			return origin(node.Server), nil
		}
	}
	for _, configured := range s.config.TransferSources {
		host := configured
		if name, configuredPort, err := net.SplitHostPort(configured); err == nil {
			host = name
			n, err := strconv.ParseUint(configuredPort, 10, 16)
			if err != nil || n != port {
				continue
			}
		}
		if strings.EqualFold(host, u.Hostname()) {
			return origin(host), nil
		}
	}
	return nil, &transferUpstreamError{403, "source is neither a ring node nor a configured transfer source"}
}
