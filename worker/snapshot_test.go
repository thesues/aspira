package main

import (
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/aspira/conn"
)

func TestStreamSnapshot(t *testing.T) {
	addr := "127.0.0.1:3301"
	var id uint64 = 1
	x, _ := NewAspiraServer(id, addr, "local.lusf")
	x.ServeGRPC()
	conn.GetPools().Connect(addr)
	var snap raftpb.Snapshot
	p, err := conn.GetPools().Get(addr)
	assert.Nil(t, err)
	err = x.populateSnapshot(snap, p)
	assert.Nil(t, err)
}
