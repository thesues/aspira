/*
 * Copyright 2017-2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"

	"github.com/golang/glog"
	"github.com/thesues/aspira/conn"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/raftwal"
	"github.com/thesues/aspira/utils"
	cannyls "github.com/thesues/cannyls-go/storage"
	otrace "go.opencensus.io/trace"
	"google.golang.org/grpc"
)

const (
	SmallKeySize = 32 << 10
)

type AspiraServer struct {
	node          *conn.Node
	raftServer    *conn.RaftServer //internal comms
	grpcServer    *grpc.Server     //internal comms, raftServer is registered on grpcServer
	store         *raftwal.WAL
	addr          string
	stopper       *utils.Stopper
	state         *aspirapb.MembershipState
	allowSnapshot bool
}

func NewAspiraServer(id uint64, addr string, path string) (as *AspiraServer, err error) {

	var db *cannyls.Storage

	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		if db, err = cannyls.CreateCannylsStorage(path, 8<<30, 0.05); err != nil {
			return
		}
	} else {
		if db, err = cannyls.OpenCannylsStorage(path); err != nil {
			return
		}
	}

	store := raftwal.Init(db)
	node := conn.NewNode(&aspirapb.RaftContext{Id: id, Addr: addr}, store)
	raftServer := conn.NewRaftServer(node)

	as = &AspiraServer{
		node:       node,
		raftServer: raftServer,
		addr:       addr,
		stopper:    utils.NewStopper(),
		store:      store,
		state:      &aspirapb.MembershipState{Nodes: make(map[uint64]string)},
	}
	return as, nil
}

func (as *AspiraServer) InitAndStart(id uint64, clusterAddr string) {
	restart := as.store.PastLife()
	if restart {
		snap, err := as.store.Snapshot()
		utils.Check(err)
		fmt.Printf("RESTART\n")

		if !raft.IsEmptySnap(snap) {
			as.node.SetConfState(&snap.Metadata.ConfState) //for future snapshot

			var state aspirapb.MembershipState
			utils.Check(state.Unmarshal(snap.Data))
			as.state = &state
			for _, id := range snap.Metadata.ConfState.Nodes {
				as.node.Connect(id, state.Nodes[id])
			}
		}
		as.node.SetRaft(raft.RestartNode(as.node.Cfg))
	} else if len(clusterAddr) == 0 {
		fmt.Printf("START\n")
		rpeers := make([]raft.Peer, 1)
		data, err := as.node.RaftContext.Marshal()
		utils.Check(err)
		rpeers[0] = raft.Peer{ID: as.node.Id, Context: data}
		as.node.SetRaft(raft.StartNode(as.node.Cfg, rpeers))
	} else {
		//join remote cluster
		p := conn.GetPools().Connect(clusterAddr)
		if p == nil {
			panic(fmt.Sprintf("Unhealthy connection to %v", clusterAddr))
		}
		c := aspirapb.NewRaftClient(p.Get())
		for {
			timeout := 8 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			// JoinCluster can block indefinitely, raft ignores conf change proposal
			// if it has pending configuration.
			_, err := c.JoinCluster(ctx, as.node.RaftContext)
			if err == nil {
				cancel()
				break
			}
			if utils.ShouldCrash(err) {
				cancel()
				log.Fatalf("Error while joining cluster: %v", err)
			}
			glog.Errorf("Error while joining cluster: %v\n", err)
			timeout *= 2
			if timeout > 32*time.Second {
				timeout = 32 * time.Second
			}
			time.Sleep(timeout) // This is useful because JoinCluster can exit immediately.
			cancel()
		}
		as.node.SetRaft(raft.StartNode(as.node.Cfg, nil))
	}

	as.stopper.RunWorker(func() {
		as.node.BatchAndSendMessages(as.stopper)
	})
	as.stopper.RunWorker(func() {
		as.node.ReportRaftComms(as.stopper)
	})
	as.stopper.RunWorker(func() {
		as.snapshotPeriodically()
	})
	as.Run()
}

func (as *AspiraServer) ServeGRPC() (err error) {
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1<<25),
		grpc.MaxSendMsgSize(1<<25),
		grpc.MaxConcurrentStreams(1000),
	)

	aspirapb.RegisterRaftServer(s, as.raftServer)
	aspirapb.RegisterAspiraGRPCServer(s, as)
	listener, err := net.Listen("tcp", as.addr)
	if err != nil {
		return err
	}
	go func() {
		defer func() {
			glog.Infof("GRPC server return")
		}()
		s.Serve(listener)
	}()
	as.grpcServer = s
	return nil
}

var errInvalidProposal = errors.New("Invalid group proposal")

func (as *AspiraServer) applyProposal(e raftpb.Entry) (string, error) {
	var p aspirapb.AspiraProposal
	glog.Infof("apply commit %d: data is %d", e.Index, e.Size())
	//leader's first commit
	if len(e.Data) == 0 {
		return p.AssociateKey, nil
	}
	utils.Check(p.Unmarshal(e.Data))
	if len(p.AssociateKey) == 0 {
		return p.AssociateKey, errInvalidProposal
	}
	var err error
	switch p.ProposalType {
	case aspirapb.AspiraProposal_Put:
		err = as.store.ApplyPut(e.Index)
	case aspirapb.AspiraProposal_Delete:
		err = as.store.Delete(p.Key)
	case aspirapb.AspiraProposal_PutWithOffset:
		panic("to be implemented")
	default:
		glog.Fatalf("unkonw type %+v", p.ProposalType)
	}
	return p.AssociateKey, err
}

func (as *AspiraServer) Run() {
	var leader bool
	ctx := context.Background()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	n := as.node
	for {
		select {
		case <-ticker.C:
			n.Raft().Tick()
		case rd := <-n.Raft().Ready():

			_, span := otrace.StartSpan(ctx, "Ready.Loop", otrace.WithSampler(otrace.ProbabilitySampler(0.001)))

			span.Annotatef(nil, "Pushed %d readstates", len(rd.ReadStates))
			if rd.SoftState != nil {
				leader = rd.RaftState == raft.StateLeader
			}

			if leader {
				for i := range rd.Messages {
					if rd.Messages[i].To == 0 {
						glog.Warningf("THE MESSAGE IS %+V\n", rd.Messages[i])
					}
					as.node.Send(&rd.Messages[i])
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {

				//drain the applied messages

				glog.Warningf("I Got snapshot %+v", rd.Snapshot.Metadata)
				err := as.receiveSnapshot(rd.Snapshot)
				if err != nil {
					glog.Fatalf("can not receive remote snapshot %+v", err)
				}

				glog.Infof("---> SNAPSHOT: %+v. DONE.\n", rd.Snapshot)

				snapOnDisk, _ := as.store.Snapshot()
				if snapOnDisk.Metadata.Index != rd.Snapshot.Metadata.Index || snapOnDisk.Metadata.Term != rd.Snapshot.Metadata.Term {
					panic("for loop, try again")
				}
				as.store.DeleteFrom(rd.Snapshot.Metadata.Index + 1)
			}

			n.SaveToStorage(rd.HardState, rd.Entries, rd.Snapshot)

			span.Annotatef(nil, "Saved to storage")

			if rd.MustSync {
				n.Store.Flush()
			}

			span.Annotatef(nil, "Sync files done")

			for _, entry := range rd.CommittedEntries {
				fmt.Printf("try to commit index:%d, size:%d\n", entry.Index, entry.Size())
				n.Applied.Begin(entry.Index)
				switch {
				case entry.Type == raftpb.EntryConfChange:
					as.applyConfChange(entry)
				case entry.Type == raftpb.EntryNormal:
					uniqKey, err := as.applyProposal(entry)
					if err != nil {
						glog.Errorf("While applying proposal: %v\n", err)
					}
					n.Proposals.Done(uniqKey, entry.Index, err)
				default:
					glog.Warningf("Unhandled entry: %+v\n", entry)
				}
				n.Applied.Done(entry.Index)
			}
			span.Annotatef(nil, "Applied %d CommittedEntries", len(rd.CommittedEntries))

			if !leader {
				for i := range rd.Messages {
					as.node.Send(&rd.Messages[i])
				}
			}
			span.Annotate(nil, "Sent messages")

			n.Raft().Advance()
			span.Annotate(nil, "Advanced Raft")
			span.End()
		}
	}
}

func (as *AspiraServer) snapshotPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer func() {
		glog.Infof("snapshot period return")
	}()
	for {
		select {
		case <-ticker.C:
			as.trySnapshot(10)
		case <-as.stopper.ShouldStop():
			return
		}
	}
}

func (as *AspiraServer) trySnapshot(skip uint64) {
	existing, err := as.node.Store.Snapshot()
	utils.Check(err)
	si := existing.Metadata.Index
	doneUntil := as.node.Applied.DoneUntil()

	if doneUntil < si+skip {
		return
	}
	data, err := as.state.Marshal()
	utils.Check(err)
	glog.Infof("Writing snapshot at index:%d\n", doneUntil-skip/2)
	as.store.CreateSnapshot(doneUntil-skip/2, as.node.ConfState(), data)
}

func (as *AspiraServer) AmLeader() bool {
	if as.node.Raft() == nil {
		return false
	}
	r := as.node.Raft()
	if r.Status().Lead != r.Status().ID {
		return false
	}
	return true
}

var errInternalRetry = errors.New("Retry Raft proposal internally")

func (as *AspiraServer) getAndWait(ctx context.Context, index uint64) ([]byte, error) {
	n := as.node
	switch {
	case n.Raft() == nil:
		return nil, errors.Errorf("Raft isn't initialized yet.")
	case ctx.Err() != nil:
		return nil, ctx.Err()
	}
	type result struct {
		data []byte
		err  error
	}

	ch := make(chan result, 1)
	go func() {
		data, err := as.store.GetData(index)
		ch <- result{data: data, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.Errorf("TIMEOUT")
	case result := <-ch:
		return result.data, result.err
	}
}

func (as *AspiraServer) proposeAndWait(ctx context.Context, proposal *aspirapb.AspiraProposal) (uint64, error) {
	n := as.node
	switch {
	case n.Raft() == nil:
		return 0, errors.Errorf("Raft isn't initialized yet.")
	case ctx.Err() != nil:
		return 0, ctx.Err()
	case !as.AmLeader():
		// Do this check upfront. Don't do this inside propose for reasons explained below.
		return 0, errors.Errorf("Not a leader. Aborting proposal: %+v", len(proposal.Data))
	}

	span := otrace.FromContext(ctx)
	// Overwrite ctx, so we no longer enforce the timeouts or cancels from ctx.
	ctx = otrace.NewContext(context.Background(), span)

	propose := func(timeout time.Duration) (uint64, error) {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		ch := make(chan conn.ProposalResult, 1)
		pctx := &conn.ProposalCtx{
			ResultCh: ch,
			// Don't use the original context, because that's not what we're passing to Raft.
			Ctx: cctx,
		}
		n.Rand.Uint64()
		uniqKey := n.UniqueKey()
		utils.AssertTruef(n.Proposals.Store(uniqKey, pctx), "Found existing proposal with key: [%v]", uniqKey)
		defer n.Proposals.Delete(uniqKey)
		proposal.AssociateKey = uniqKey
		span.Annotatef(nil, "Proposing with key: %s. Timeout: %v", uniqKey, timeout)

		data, err := proposal.Marshal()
		if err != nil {
			return 0, err
		}
		// Propose the change.
		if err := n.Raft().Propose(cctx, data); err != nil {
			span.Annotatef(nil, "Error while proposing via Raft: %v", err)
			return 0, errors.Wrapf(err, "While proposing")
		}

		// Wait for proposal to be applied or timeout.
		select {
		case result := <-ch:
			// We arrived here by a call to n.props.Done().
			return result.Index, result.Err
		case <-cctx.Done():
			span.Annotatef(nil, "Internal context timeout %s. Will retry...", timeout)
			return 0, errInternalRetry
		}
	}
	var index uint64
	err := errInternalRetry
	timeout := 4 * time.Second
	for err == errInternalRetry {
		index, err = propose(timeout)
		timeout *= 2 // Exponential backoff
		if timeout > time.Minute {
			timeout = 32 * time.Second
		}
	}
	return index, err
}

func (as *AspiraServer) applyConfChange(e raftpb.Entry) {

	var cc raftpb.ConfChange
	utils.Check(cc.Unmarshal(e.Data))
	//fmt.Printf("applyConfChange: %+v\n", e.String())
	fmt.Printf("applyConfChange: %+v\n", cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if len(cc.Context) > 0 {
			var ctx aspirapb.RaftContext
			utils.Check(ctx.Unmarshal(cc.Context))
			as.node.Connect(ctx.Id, ctx.Addr)
			//update state
			as.state.Nodes[ctx.Id] = ctx.Addr
		}

		xx := as.node.Raft().ApplyConfChange(cc)
		fmt.Printf("FUCKApply cc %+v\n", xx)
		as.node.SetConfState(xx)
		as.node.DoneConfChange(cc.ID, nil)
	case raftpb.ConfChangeRemoveNode:
	case raftpb.ConfChangeUpdateNode:
	}

}

var (
	id    = flag.Uint64("id", 0, "id")
	join  = flag.String("join", "", "remote addr")
	debug = flag.Bool("debug", false, "debug")
)

func (as *AspiraServer) Stop() {
	as.node.Raft().Stop()
	as.stopper.Close() //HTTP, createSnapshot
	as.grpcServer.Stop()
}

func main() {
	//1 => 127.0.0.1:3301
	//2 => 127.0.0.1:3302

	flag.Parse()

	/*
		je, _ := jaeger.NewExporter(jaeger.Options{
			Endpoint:    "http://localhost:14268",
			ServiceName: "aspira",
		})
		otrace.RegisterExporter(je)
		otrace.ApplyConfig(otrace.Config{DefaultSampler: trace.AlwaysSample()})
	*/

	stringId := fmt.Sprintf("%d", *id)
	var x *AspiraServer
	x, _ = NewAspiraServer(*id, "127.0.0.1:330"+stringId, stringId+".lusf")

	x.ServeGRPC()
	go func() {
		x.ServeHTTP()
	}()

	x.InitAndStart(*id, *join)
}

func (as *AspiraServer) receiveSnapshot(snap raftpb.Snapshot) (err error) {
	//close and delete current store
	var memberStat aspirapb.MembershipState
	utils.Check(memberStat.Unmarshal(snap.Data))
	for _, remotePeer := range snap.Metadata.ConfState.Nodes {
		p := as.getPeerPool(remotePeer)
		if p == nil {
			continue
		}
		glog.V(2).Infof("Snapshot.RaftContext.Addr: %+v", p.Addr)
		err = as.populateSnapshot(snap, p)
		if err == nil {
			break
		}
	}
	return
}

func (as *AspiraServer) getPeerPool(peer uint64) *conn.Pool {
	if peer == as.node.RaftContext.Id {
		return nil
	}
	addr, ok := as.node.Peer(peer)
	if !ok {
		return nil
	}
	p, err := conn.GetPools().Get(addr)
	if err != nil {
		return nil
	}
	return p
}

func (as *AspiraServer) populateSnapshot(snap raftpb.Snapshot, pl *conn.Pool) (err error) {
	conn := pl.Get()
	c := aspirapb.NewAspiraGRPCClient(conn)
	stream, err := c.StreamSnapshot(context.Background(), as.node.RaftContext)
	if err != nil {
		return
	}

	backupName := fmt.Sprintf("backup-%d", as.node.RaftContext.Id)
	file, err := os.OpenFile(backupName, os.O_CREATE|os.O_RDWR, 0644)

	defer file.Close()
	glog.Infof("Start to receive data")
	var payload *aspirapb.Payload
	for {
		payload, err = stream.Recv()
		if err != nil && err != io.EOF {
			return
		}
		if payload != nil {
			file.Write(payload.Data)
		} else {
			break
		}
	}
	glog.Infof("End to receive data")

	as.store.CloseDB()
	name := fmt.Sprintf("%d.lusf", as.node.RaftContext.Id)
	os.Remove(name)

	os.Rename(backupName, name)

	db, err := cannyls.OpenCannylsStorage(name)
	if err != nil {
		panic("can not open downloaded cannylsdb")
	}
	as.store.SetDB(db)
	return nil
}
