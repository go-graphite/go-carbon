package buckyd

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// transferSourceOrigin constructs a pull origin from a caller-supplied source.
// Any graphite-store host is accepted; callers are authenticated by the buckyd
// JWT. Only the scheme, host and numeric port are kept, so no path, query or
// credentials from the caller's URL are forwarded with the signed read token.
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
	return origin(u.Hostname()), nil
}
