package carbonapi_v3_pb

//go:generate protoc --go_out=. --go-vtproto_out=. --go-vtproto_opt=features=all carbonapi_v3_pb.proto --proto_path=. --proto_path=../
//go:generate mv github.com/go-graphite/protocol/carbonapi_v3_pb/carbonapi_v3_pb.pb.go ./
//go:generate mv github.com/go-graphite/protocol/carbonapi_v3_pb/carbonapi_v3_pb_vtproto.pb.go ./
//go:generate rm -r github.com/go-graphite/protocol/carbonapi_v3_pb
//go:generate rm -r github.com/go-graphite/protocol
//go:generate rm -r github.com/go-graphite
//go:generate rm -r github.com
