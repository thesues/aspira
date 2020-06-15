PROTO_PATH=.:${GOPATH}/src
protoc  --proto_path=${PROTO_PATH} --gofast_out=plugins=grpc:./aspirapb  aspirapb.proto
