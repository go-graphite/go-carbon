package pickle

import (
	"bytes"

	ogórek "github.com/lomik/og-rek"
)

// NewCarbonlinkRequest creates instance of CarbonlinkRequest
func NewCarbonlinkRequest() *CarbonlinkRequest {
	return &CarbonlinkRequest{}
}

// CarbonlinkRequest ...
type CarbonlinkRequest struct {
	Type   string `pickle:"type"`
	Metric string `pickle:"metric"`
	Key    string `pickle:"key"`
	Value  string `pickle:"value"`
}

func ParseCarbonlinkRequest(msg []byte) (*CarbonlinkRequest, error) {
	d := ogórek.NewDecoder(bytes.NewReader(msg))

	v, err := d.Decode()
	if err != nil {
		return nil, err
	}

	m, ok := ToMap(v)
	if !ok {
		return nil, BadPickleError
	}

	r := NewCarbonlinkRequest()

	for k, v := range m {
		kk, ok := ToString(k)
		if !ok {
			return nil, BadPickleError
		}
		vv, ok := ToString(v)
		if !ok {
			return nil, BadPickleError
		}
		switch kk {
		case "type":
			r.Type = vv
		case "metric":
			r.Metric = vv
		case "key":
			r.Key = vv
		case "value":
			r.Value = vv
		default:
			return nil, BadPickleError
		}
	}

	return r, nil
}

func MarshalCarbonlinkRequest(req *CarbonlinkRequest) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := NewWriter(buf)
	w.Dict()
	if req.Type != "" {
		w.String("type")
		w.String(req.Type)
		w.SetItem()
	}

	if req.Metric != "" {
		w.String("metric")
		w.String(req.Metric)
		w.SetItem()
	}

	if req.Key != "" {
		w.String("key")
		w.String(req.Key)
		w.SetItem()
	}

	if req.Value != "" {
		w.String("value")
		w.String(req.Value)
		w.SetItem()
	}

	w.Stop()

	return buf.Bytes(), nil
}

func UnmarshalCarbonlinkRequest(data []byte) (*CarbonlinkRequest, error) {
	// try fast
	req, err := ParseCarbonlinkRequestFast(data)
	if err != nil {
		// fallback to slow method
		req, err = ParseCarbonlinkRequest(data)
	}

	return req, err
}
