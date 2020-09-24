package main

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/conn"
	"github.com/thesues/aspira/protos/aspirapb"
	pb "github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/xlog"

	otrace "go.opencensus.io/trace"
)

// RaftServer is a wrapper around node that implements the Raft service.
type RaftServer struct {
	m     sync.RWMutex
	store *AspiraStore
	//node *Node
}

// UpdateNode safely updates the node.
/*
func (w *RaftServer) UpdateNode(n *Node) {
	w.m.Lock()
	defer w.m.Unlock()
	w.node = n
}

// GetNode safely retrieves the node.
func (w *RaftServer) GetNode() *Node {
	w.m.RLock()
	defer w.m.RUnlock()
	return w.node
}
*/

// NewRaftServer returns a pointer to a new RaftServer instance.
func NewRaftServer(store *AspiraStore) *RaftServer {
	return &RaftServer{store: store}
}

// IsPeer checks whether this node is a peer of the node sending the request.
/*
func (w *RaftServer) IsPeer(ctx context.Context, rc *pb.RaftContext) (
	*pb.PeerResponse, error) {
	node := w.GetNode()
	if node == nil || node.Raft() == nil {
		return &pb.PeerResponse{}, ErrNoNode
	}

	confState := node.ConfState()

	if confState == nil {
		return &pb.PeerResponse{}, nil
	}

	for _, raftIdx := range confState.Nodes {
		if rc.Id == raftIdx {
			return &pb.PeerResponse{Status: true}, nil
		}
	}
	return &pb.PeerResponse{}, nil
}
*/

// JoinCluster handles requests to join the cluster.
func (w *RaftServer) JoinCluster(ctx context.Context,
	rc *pb.RaftContext) (*pb.Payload, error) {
	if ctx.Err() != nil {
		return &pb.Payload{}, ctx.Err()
	}

	node := w.store.GetNode(rc.Gid)

	if node == nil || node.Raft() == nil {
		return &pb.Payload{}, conn.ErrNoNode
	}

	return node.JoinCluster(ctx, rc)
}

func (w *RaftServer) BlobRaftMessage(ctx context.Context, request *aspirapb.BlobRaftMessageRequest) (*aspirapb.BlobRaftMessageResponse, error) {

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	span := otrace.FromContext(ctx)

	node := w.store.GetNode(request.Context.Gid)
	if node == nil || node.Raft() == nil {
		return nil, conn.ErrNoNode
	}
	span.Annotatef(nil, "BlobStream server is node %#x", node.Id)
	rc := request.Context
	span.Annotatef(nil, "Stream from %#x", request.Context.GetId())
	if rc != nil {
		node.Connect(rc.Id, rc.Addr)
	}
	msg := raftpb.Message{}
	msg.Unmarshal(request.Payload.Data)
	if err := node.Raft().Step(ctx, msg); err != nil {
		xlog.Logger.Warnf("Error while raft.Step from %#x: %v. Closing BloBRaftMessage stream.",
			rc.GetId(), err)
		return nil, errors.Wrapf(err, "error while raft.Step from %#x", rc.GetId())
	}

	return &aspirapb.BlobRaftMessageResponse{}, nil
}

// RaftMessage handles RAFT messages.
func (w *RaftServer) RaftMessage(server pb.Raft_RaftMessageServer) error {

	ctx := server.Context()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	span := otrace.FromContext(ctx)

	/*
		node := w.node
		if node == nil || node.Raft() == nil {
			return ErrNoNode
		}
		span.Annotatef(nil, "Stream server is node %#x", node.Id)
		raft := node.Raft()
	*/

	var rc *pb.RaftContext

	var node *conn.Node
	//var raft raft.Node

	step := func(data []byte, node *conn.Node) error {

		if node == nil {
			panic("node is nil")
		}
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		for idx := 0; idx < len(data); {
			/*
				x.AssertTruef(len(data[idx:]) >= 4,
					"Slice left of size: %v. Expected at least 4.", len(data[idx:]))
			*/

			sz := int(binary.LittleEndian.Uint32(data[idx : idx+4]))
			idx += 4
			msg := raftpb.Message{}
			if idx+sz > len(data) {
				return errors.Errorf(
					"Invalid query. Specified size %v overflows slice [%v,%v)\n",
					sz, idx, len(data))
			}
			if err := msg.Unmarshal(data[idx : idx+sz]); err != nil {
				panic("unmarshal")
			}
			// This should be done in order, and not via a goroutine.
			// Step can block forever. See: https://github.com/etcd-io/etcd/issues/10585
			// So, add a context with timeout to allow it to get out of the blockage.
			switch msg.Type {
			case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
				atomic.AddInt64(&node.HeartbeatsIn, 1)
			case raftpb.MsgReadIndex, raftpb.MsgReadIndexResp:
				/*
					case raftpb.MsgApp, raftpb.MsgAppResp:
					case raftpb.MsgProp:*/
			default:
				xlog.Logger.Debugf("RaftComm: [%#x] Received msg of type: %s from %#x",
					msg.To, msg.Type, msg.From)
			}

			if err := node.Raft().Step(ctx, msg); err != nil {
				xlog.Logger.Warnf("Error while raft.Step from %#x: %v. Closing RaftMessage stream.",
					rc.GetId(), err)
				return errors.Wrapf(err, "error while raft.Step from %#x", rc.GetId())
			}
			idx += sz
		}
		return nil
	}

	for loop := 1; ; loop++ {
		batch, err := server.Recv()
		if err != nil {
			return err
		}
		if loop%1e6 == 0 {
			xlog.Logger.Infof("%d messages received by %#x from %#x", loop, node.Id, rc.GetId())
		}
		if loop == 1 {
			rc = batch.GetContext()
			span.Annotatef(nil, "Stream from %#x", rc.GetId())

			node = w.store.GetNode(rc.Gid)
			if node == nil || node.Raft() == nil {
				return conn.ErrNoNode
			}
			span.Annotatef(nil, "Stream server is node %#x", node.Id)
			//raft = node.Raft()
			if rc != nil {
				node.Connect(rc.Id, rc.Addr)
			}
		}
		if batch.Payload == nil {
			continue
		}
		data := batch.Payload.Data
		if err := step(data, node); err != nil {
			return err
		}
	}
}

// Heartbeat rpc call is used to check connection with other workers after worker
// tcp server for this instance starts.
func (w *RaftServer) Heartbeat(in *pb.Payload, stream pb.Raft_HeartbeatServer) error {
	ticker := time.NewTicker(conn.EchoDuration)
	defer ticker.Stop()

	ctx := stream.Context()
	out := &pb.Payload{Data: []byte("beat")}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := stream.Send(out); err != nil {
				return err
			}
		}
	}
}
