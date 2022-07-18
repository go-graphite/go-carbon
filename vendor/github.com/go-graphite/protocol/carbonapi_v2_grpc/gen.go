package carbonapi_v2_grpc

//go:generate protoc --go_out=. --go-vtproto_out=. --go-vtproto_opt=features=all carbonapi_v2_grpc.proto --proto_path=. --proto_path=../
//go:generate mv github.com/go-graphite/protocol/carbonapi_v2_grpc/carbonapi_v2_grpc.pb.go ./
//go:generate mv github.com/go-graphite/protocol/carbonapi_v2_grpc/carbonapi_v2_grpc_vtproto.pb.go ./
//go:generate rm -r github.com/go-graphite/protocol/carbonapi_v2_grpc
//go:generate rm -r github.com/go-graphite/protocol
//go:generate rm -r github.com/go-graphite
//go:generate rm -r github.com/
// protoc --go_out=. --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size --go_opt="Mprotocol/carbonapi_v2_grpc/carbonapi_v2_grpc.proto=github.com/go-graphite/carbonapi_v2_grpc/carbonapi_v2_grpc;carbonapi_v2_grpc" carbonapi_v2_grpc.proto --proto_path=../vendor/ --proto_path=. --proto_path=../
