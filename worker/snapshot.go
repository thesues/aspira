package main

import (
	"io"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/xlog"
)

//stream snapshot API
func (as *AspiraServer) StreamSnapshot(in *aspirapb.RaftContext, stream aspirapb.AspiraGRPC_StreamSnapshotServer) error {

	if !as.AmLeader() {
		return errors.Errorf("I am not leader, try next...")
	}

	defer xlog.Logger.Infof("StreamSnasphot done")

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
			as.node.Raft().ReportSnapshot(in.Id, raft.SnapshotFailure)
			return err
		}
		time.Sleep(1 * time.Millisecond)
	}
	return nil
}
