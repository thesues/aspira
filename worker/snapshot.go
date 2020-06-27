package worker

import (
	"github.com/thesues/aspira/protos/aspirapb"
)

func (as *AspiraServer) StreamSnapshot(in *aspirapb.RaftContext, stream aspirapb.AspiraGRPC_StreamSnapshotServer) error {
	/*
		ctx := stream.Context()
		for i := 0; i < 100; i++ {
			select {
				case <-
			}
			stream.Send(&aspirapb.Payload{Data: []byte("FUCKYOU")})
		}
	*/
	return nil
}
