package main

import (
	"io"

	"github.com/thesues/aspira/protos/aspirapb"
)

//stream snapsho API
func (as *AspiraServer) StreamSnapshot(in *aspirapb.RaftContext, stream aspirapb.AspiraGRPC_StreamSnapshotServer) error {
	reader, err := as.store.GetStreamReader()
	if err != nil {
		return err
	}
	defer as.store.FreeStreamReader()
	buf := make([]byte, 512<<10)
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		if err = stream.Send(&aspirapb.Payload{Data: buf[:n]}); err != nil {
			return err
		}
	}
	return nil
}
