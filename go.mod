module github.com/thesues/aspira

go 1.14

require (
	contrib.go.opencensus.io/exporter/jaeger v0.2.0
	github.com/alecthomas/template v0.0.0-20160405071501-a0175ee3bccc
	github.com/coreos/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/gin-contrib/pprof v1.3.0
	github.com/gin-gonic/gin v1.6.3
	github.com/golang/protobuf v1.3.3
	github.com/golang/snappy v0.0.1
	github.com/hashicorp/golang-lru v0.5.0
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.4.0
	github.com/swaggo/files v0.0.0-20190704085106-630677cd5c14
	github.com/swaggo/gin-swagger v1.2.0
	github.com/swaggo/swag v1.5.1
	github.com/thesues/cannyls-go v0.2.1-0.20200714020537-b5127d67bbe4
	github.com/urfave/cli v1.20.0
	github.com/urfave/cli/v2 v2.2.0
	go.etcd.io/etcd v3.3.22+incompatible
	go.etcd.io/etcd/v3 v3.3.0-rc.0.0.20200708123750-429826b46719 // indirect
	go.opencensus.io v0.22.4
	go.uber.org/zap v1.14.1
	golang.org/x/net v0.0.0-20191014212845-da9a3fd4c582
	google.golang.org/grpc v1.26.0
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.3
