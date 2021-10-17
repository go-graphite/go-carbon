package carbonapi_v2_pb

//go:generate protoc --go_out=. --go-vtproto_out=. --go-vtproto_opt=features=all carbonapi_v2_pb.proto --proto_path=. --proto_path=../
//go:generate mv github.com/go-graphite/protocol/carbonapi_v2_pb/carbonapi_v2_pb.pb.go ./
//go:generate mv github.com/go-graphite/protocol/carbonapi_v2_pb/carbonapi_v2_pb_vtproto.pb.go ./
//go:generate rm -r github.com/go-graphite/protocol/carbonapi_v2_pb
//go:generate rm -r github.com/go-graphite/protocol
//go:generate rm -r github.com/go-graphite
//go:generate rm -r github.com/
// protoc --go_out=. --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size --go_opt="Mprotocol/carbonapi_v2_pb/carbonapi_v2_pb.proto=github.com/go-graphite/carbonapi_v2_pb/carbonapi_v2_pb;carbonapi_v2_pb" carbonapi_v2_pb.proto --proto_path=../vendor/ --proto_path=. --proto_path=../
