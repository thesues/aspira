#export GO111MODULE=off
#go get -u github.com/gogo/protobuf/{proto,protoc-gen-gogo,gogoproto}
#
#
#GOGOPROTO_ROOT="${GOPATH}/src/github.com/gogo/protobuf"
#GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
#ETCD_PATH="$GOPATH/src/github.com/coreos/etcd/raft/raftpb"
#protoc  --proto_path=${PROTO_PATH} --gofast_out=plugins=grpc:./aspirapb  aspirapb.proto
protoc -I=.:${ETCD_PATH}:${GOGOPROTO_PATH} --gofast_out=plugins=grpc:./aspirapb  aspirapb.proto
