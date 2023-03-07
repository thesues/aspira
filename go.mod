module github.com/thesues/aspira

go 1.14

require (
	contrib.go.opencensus.io/exporter/jaeger v0.2.0
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/coreos/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/gin-contrib/pprof v1.3.0
	github.com/gin-gonic/gin v1.7.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.3.3
	github.com/golang/snappy v0.0.1
	github.com/google/uuid v1.0.0 // indirect
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.9.5 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/olekukonko/tablewriter v0.0.4
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/swaggo/files v0.0.0-20190704085106-630677cd5c14
	github.com/swaggo/gin-swagger v1.2.0
	github.com/swaggo/swag v1.6.7
	github.com/thesues/cannyls-go v0.2.1-0.20200908230251-d434dc4afee7
	github.com/tmc/grpc-websocket-proxy v0.0.0-20170815181823-89b8d40f7ca8 // indirect
	github.com/urfave/cli/v2 v2.2.0
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	go.opencensus.io v0.22.4
	go.uber.org/zap v1.14.1
	golang.org/x/net v0.0.0-20201021035429-f5854403a974
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2
	google.golang.org/grpc v1.26.0
	gopkg.in/go-playground/assert.v1 v1.2.1
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.3
