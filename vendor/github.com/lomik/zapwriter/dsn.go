package zapwriter

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type DsnObj struct {
	url.Values
}

func DSN(values url.Values) *DsnObj {
	return &DsnObj{values}
}

func (dsn *DsnObj) Duration(key string, initial string) (time.Duration, error) {
	s := dsn.Get(key)
	if s == "" {
		return time.ParseDuration(initial)
	}
	return time.ParseDuration(s)
}

func (dsn *DsnObj) Int(key string, initial int) (int, error) {
	s := dsn.Get(key)
	if s == "" {
		return initial, nil
	}
	return strconv.Atoi(s)
}

func (dsn *DsnObj) Bool(key string, initial bool) (bool, error) {
	s := dsn.Get(key)
	if s == "" {
		return initial, nil
	}

	return map[string]bool{
		"true": true,
		"1":    true,
	}[strings.ToLower(s)], nil
}

func (dsn *DsnObj) String(key string, initial string) (string, error) {
	s := dsn.Get(key)
	if s == "" {
		return initial, nil
	}
	return s, nil
}

func (dsn *DsnObj) StringRequired(key string) (string, error) {
	s := dsn.Get(key)
	if s == "" {
		return "", fmt.Errorf("%#v is required", key)
	}
	return s, nil
}

func (dsn *DsnObj) SetInt(p *int, key string) error {
	var err error
	*p, err = dsn.Int(key, *p)
	return err
}

func (dsn *DsnObj) SetDuration(p *time.Duration, key string) error {
	var err error
	*p, err = dsn.Duration(key, p.String())
	return err
}

func (dsn *DsnObj) SetString(p *string, key string) error {
	var err error
	*p, err = dsn.String(key, *p)
	return err
}

func AnyError(e ...error) error {
	for i := 0; i < len(e); i++ {
		if e[i] != nil {
			return e[i]
		}
	}
	return nil
}
