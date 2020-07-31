package main

import (
	"io"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/conn"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/xlog"
)

//stream snapshot API

func (w *RaftServer) StreamSnapshot(in *aspirapb.RaftContext, stream aspirapb.Raft_StreamSnapshotServer) error {

	xlog.Logger.Infof("snapshot request for gid :%d ", in.Gid)
	node := w.store.GetNode(in.Gid)
	if node == nil {
		return conn.ErrNoNode
	}
	if !node.AmLeader() {
		return errors.Errorf("I am not leader, try next...")
	}

	defer xlog.Logger.Infof("StreamSnasphot done")
	worker := w.store.GetWorker(in.Gid)
	reader, err := worker.store.GetStreamReader()
	if err != nil {
		return err
	}
	defer worker.store.FreeStreamReader()
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
			node.Raft().ReportSnapshot(in.Id, raft.SnapshotFailure)
			return err
		}
		time.Sleep(1 * time.Millisecond)
	}
	return nil
}
