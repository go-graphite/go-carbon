module github.com/go-graphite/go-carbon

go 1.21

toolchain go1.23.0

require (
	cloud.google.com/go/pubsub v1.41.0
	github.com/BurntSushi/toml v1.4.0
	github.com/IBM/sarama v1.43.3
	github.com/NYTimes/gziphandler v1.1.1
	github.com/OneOfOne/go-utils v0.0.0-20180319162427-6019ff89a94e
	github.com/dgryski/go-expirecache v0.0.0-20170314133854-743ef98b2adb
	github.com/dgryski/go-trigram v0.0.0-20160407183937-79ec494e1ad0
	github.com/dgryski/httputil v0.0.0-20160116060654-189c2918cd08
	github.com/go-graphite/carbonzipper v0.0.0-20191211140943-b9a9d1881aed
	github.com/go-graphite/go-whisper v0.0.0-20230526115116-e3110f57c01c
	github.com/go-graphite/protocol v1.0.1-0.20220718132526-4b842ba389ee
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.6.0
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/klauspost/compress v1.17.9
	github.com/lomik/graphite-pickle v0.0.0-20171221213606-614e8df42119
	github.com/lomik/og-rek v0.0.0-20170411191824-628eefeb8d80
	github.com/lomik/stop v0.0.0-20161127103810-188e98d969bd
	github.com/lomik/zapwriter v0.0.0-20210624082824-c1161d1eb463
	github.com/prometheus/client_golang v1.20.0
	github.com/sevlyar/go-daemon v0.1.6
	github.com/stretchr/testify v1.9.0
	github.com/syndtr/goleveldb v1.0.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
	go.uber.org/zap v1.27.0
	google.golang.org/api v0.192.0
	google.golang.org/grpc v1.65.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/greatroar/blobloom v0.8.0
	golang.org/x/net v0.28.0
	google.golang.org/protobuf v1.34.2
)

require (
	cloud.google.com/go v0.115.0 // indirect
	cloud.google.com/go/auth v0.8.1 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.3 // indirect
	cloud.google.com/go/compute/metadata v0.5.0 // indirect
	cloud.google.com/go/iam v1.1.12 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/s2a-go v0.1.8 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.13.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/onsi/ginkgo v1.14.0 // indirect
	github.com/onsi/gomega v1.10.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.einride.tech/aip v0.67.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/sdk v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/oauth2 v0.22.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.23.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	golang.org/x/time v0.6.0 // indirect
	google.golang.org/genproto v0.0.0-20240730163845-b1a4ccb954bf // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240725223205-93522f1f2a9f // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240730163845-b1a4ccb954bf // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
