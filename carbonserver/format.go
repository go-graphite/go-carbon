package carbonserver

type responseFormat int

func (r responseFormat) String() string {
	switch r {
	case jsonFormat:
		return "json"
	case pickleFormat:
		return "pickle"
	case protoV2Format:
		return "carbonapi_v2_pb"
	case protoV3Format:
		return "carbonapi_v3_pb"
	default:
		return "unknown"
	}
}

const (
	jsonFormat responseFormat = iota
	pickleFormat
	protoV2Format
	protoV3Format
)

var knownFormats = map[string]responseFormat{
	"json":            jsonFormat,
	"pickle":          pickleFormat,
	"protobuf":        protoV2Format,
	"protobuf3":       protoV2Format,
	"carbonapi_v2_pb": protoV2Format,
	"carbonapi_v3_pb": protoV3Format,
}
