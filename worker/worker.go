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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/thesues/aspira/conn"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/raftwal"
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	cannyls "github.com/thesues/cannyls-go/storage"
	otrace "go.opencensus.io/trace"
)

type AspiraWorker struct {
	Node      *conn.Node
	store     *raftwal.WAL
	stopper   *utils.Stopper
	storePath string
	info      unsafe.Pointer // *aspirapb.WorkerInfo
}

func NewAspiraWorker(id uint64, gid uint64, addr, path string) (as *AspiraWorker, err error) {

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
	node := conn.NewNode(&aspirapb.RaftContext{Id: id, Gid: gid, Addr: addr}, store)
	//raftServer := conn.NewRaftServer(node)

	as = &AspiraWorker{
		Node: node,
		//raftServer: raftServer,
		//addr:    addr,
		stopper:   utils.NewStopper(),
		store:     store,
		storePath: path,
		//state:      &aspirapb.MembershipState{Nodes: make(map[uint64]string)},
	}
	return as, nil
}

func (aw *AspiraWorker) WorkerStatus() *aspirapb.WorkerStatus {
	return (*aspirapb.WorkerStatus)(atomic.LoadPointer(&aw.info))
}

func (aw *AspiraWorker) SetWorkerStatus(p map[uint64]aspirapb.WorkerStatus_ProgressType) {
	if p != nil {
		info := &aspirapb.WorkerStatus{
			RaftContext:   proto.Clone(aw.Node.RaftContext).(*aspirapb.RaftContext),
			Progress:      p,
			DataFreeBytes: aw.store.DB().Usage().DataFreeBytes,
		}
		atomic.StorePointer(&aw.info, unsafe.Pointer(info))
		return
	}
	//else
	atomic.StorePointer(&aw.info, unsafe.Pointer(nil))

}

func splitAndTrim(s string, sep string) []string {
	parts := strings.Split(s, sep)
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// InitAndStart
//if has localstore, restart
//if joinClusterAddr == "", initialCluster == "", start worker as leader
//if initialCluster != "",  start worker in the initialCluster, the format of initialCluster is "100;127.0.0.1:3301,  101;127.0.0.1:3302, 104;127.0.0.1:3304" => ID;addr, ID;addr, ID;addr
//if joinClusterAddr != "", join the remote addr(must be leader)
func (aw *AspiraWorker) InitAndStart(joinClusterAddr, initialCluster string) error {
	restart, err := aw.store.PastLife()
	if err != nil {
		return err
	}
	if restart {
		snap, err := aw.store.Snapshot()
		utils.Check(err)
		xlog.Logger.Info("RESTART")

		if !raft.IsEmptySnap(snap) {
			aw.Node.SetConfState(&snap.Metadata.ConfState) //for future snapshot

			var state aspirapb.MembershipState
			utils.Check(state.Unmarshal(snap.Data))
			aw.Node.Lock()
			aw.Node.State = &state
			aw.Node.Unlock()
			for _, id := range snap.Metadata.ConfState.Nodes {
				aw.Node.Connect(id, state.Nodes[id])
			}
		}
		aw.Node.SetRaft(raft.RestartNode(aw.Node.Cfg))
	} else if len(joinClusterAddr) == 0 && len(initialCluster) == 0 {
		xlog.Logger.Info("START")
		rpeers := make([]raft.Peer, 1)
		data, err := aw.Node.RaftContext.Marshal()
		utils.Check(err)
		rpeers[0] = raft.Peer{ID: aw.Node.Id, Context: data}
		aw.Node.SetRaft(raft.StartNode(aw.Node.Cfg, rpeers))
	} else if len(initialCluster) != 0 {
		//initial start
		xlog.Logger.Info("Initial START")
		parts := strings.Split(initialCluster, ",")
		rpeers := make([]raft.Peer, len(parts))
		var valid bool
		for i, part := range parts {

			segs := splitAndTrim(part, ";")
			if len(segs) != 2 {
				return errors.Errorf("failed to parse initialcluster, segs len is not 2")
			}

			id, err := strconv.ParseUint(segs[0], 10, 64)
			if err != nil {
				return errors.Errorf("failed to parse initialcluster, seg[0] is %v, err is %v", segs[0], err)
			}
			xlog.Logger.Infof("worker:%d,  segs: %v, [%d, %s]", aw.Node.Id, segs, aw.Node.Id, aw.Node.MyAddr)
			if id == aw.Node.Id && segs[1] == aw.Node.MyAddr {
				valid = true
			}
			rc := aspirapb.RaftContext{
				Id:   id,
				Gid:  aw.Node.Gid,
				Addr: segs[1],
			}
			data, err := rc.Marshal()
			rpeers[i] = raft.Peer{ID: id, Context: data}
		}

		if !valid {
			return errors.Errorf("initial start: initialCluster doesnot include myself")
		}
		aw.Node.SetRaft(raft.StartNode(aw.Node.Cfg, rpeers))
	} else if len(joinClusterAddr) != 0 {
		//join remote cluster
		xlog.Logger.Info("Join remote cluster")
		p := conn.GetPools().Connect(joinClusterAddr)
		if p == nil {
			panic(fmt.Sprintf("Unhealthy connection to %v", joinClusterAddr))
		}

		//download snapshot first
		retry := 1
		for {
			err := aw.populateSnapshot(raftpb.Snapshot{}, p)
			if err != nil {
				xlog.Logger.Error(err.Error())
			} else {
				break
			}
			time.Sleep(time.Duration(retry) * time.Second)
			retry++
			if retry > 10 {
				return errors.Errorf("can not download remote snapshot from %s, quit", joinClusterAddr)
			}
		}

		restart, err := aw.store.PastLife()
		if !restart || err != nil {
			return errors.Errorf("can not initial and replay backup snapshot")
		}
		c := aspirapb.NewRaftClient(p.Get())
		retry = 1
		for {
			timeout := 8 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			// JoinCluster can block indefinitely, raft ignores conf change proposal
			// if it has pending configuration.
			_, err := c.JoinCluster(ctx, aw.Node.RaftContext)
			cancel()
			if err == nil {
				xlog.Logger.Infof("Success to joining cluster: %v\n")
				cancel()
				break
			}
			if utils.ShouldCrash(err) {
				cancel()
				log.Fatalf("Error while joining cluster: %v", err)
			}
			xlog.Logger.Errorf("Error while joining cluster: %v\n, try again", err)
			retry++
			if retry > 10 {
				return errors.Errorf("Error while join remote cluster %+v, quit...", err)
			}
			timeout *= 2
			if timeout > 32*time.Second {
				timeout = 32 * time.Second
			}
			time.Sleep(timeout) // This is useful because JoinCluster can exit immediately.
			cancel()
		}
		aw.Node.SetRaft(raft.StartNode(aw.Node.Cfg, nil))
	} else { //if len(initialCluster) ! = 0 &&  len(joinAddress) != 0
		xlog.Logger.Fatal("input parameter is %+v, and %+v", joinClusterAddr, initialCluster)
	}

	aw.stopper.RunWorker(func() {
		aw.Node.BatchAndSendMessages(aw.stopper)
	})
	/*
		as.stopper.RunWorker(func() {
			as.node.ReportRaftComms(as.stopper)
		})
	*/
	aw.stopper.RunWorker(aw.Run)
	return nil
}

var errInvalidProposal = errors.New("Invalid group proposal")

func (aw *AspiraWorker) applyProposal(e raftpb.Entry) (string, error) {
	var p aspirapb.AspiraProposal
	xlog.Logger.Infof("apply commit %d: data is %d", e.Index, e.Size())
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
		err = aw.store.ApplyPut(e)
	case aspirapb.AspiraProposal_Delete:
		err = aw.store.Delete(p.Key)
	case aspirapb.AspiraProposal_PutWithOffset:
		panic("to be implemented")
	default:
		xlog.Logger.Fatalf("unkonw type %+v", p.ProposalType)
	}
	return p.AssociateKey, err
}

func (aw *AspiraWorker) Run() {
	var leader bool
	ctx := context.Background()
	var createSnapshot bool = true
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	n := aw.Node
	loop := uint64(0)

	for {
		select {
		case <-aw.stopper.ShouldStop():
			xlog.Logger.Infof("worker %d quit", aw.Node.Id)
			return
		case <-ticker.C:
			n.Raft().Tick()
		case rd := <-n.Raft().Ready():
			_, span := otrace.StartSpan(ctx, "Ready.Loop", otrace.WithSampler(otrace.ProbabilitySampler(0.1)))

			span.Annotatef(nil, "Pushed %d readstates", len(rd.ReadStates))
			if rd.SoftState != nil {
				leader = rd.RaftState == raft.StateLeader
			}

			createSnapshot = true
			loop++
			if leader {
				if loop%1000 == 0 {
					xlog.Logger.Infof("I am leader")
				}
				for i := range rd.Messages {
					aw.Node.Send(&rd.Messages[i])
					if !raft.IsEmptySnap(rd.Messages[i].Snapshot) {
						createSnapshot = true
						//xlog.Logger.Warnf("from %d to %d, snap is %v", rd.Messages[i].From, rd.Messages[i].To, rd.Messages[i].Snapshot)
					}
				}

				p := make(map[uint64]aspirapb.WorkerStatus_ProgressType)

				for id, progress := range n.Raft().Status().Progress {
					switch progress.State {
					case raft.ProgressStateProbe:
						p[id] = aspirapb.WorkerStatus_Probe
					case raft.ProgressStateReplicate:
						p[id] = aspirapb.WorkerStatus_Replicate
					case raft.ProgressStateSnapshot:
						p[id] = aspirapb.WorkerStatus_Snapshot
					default:
						xlog.Logger.Fatal("unkown status type")
					}

					if progress.State == raft.ProgressStateSnapshot {
						createSnapshot = false
					}

				}
				aw.SetWorkerStatus(p)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {

				//drain the applied messages
				xlog.Logger.Infof("I Got snapshot %+v", rd.Snapshot.Metadata)
				err := aw.receiveSnapshot(rd.Snapshot)
				if err != nil {
					xlog.Logger.Fatalf("can not receive remote snapshot %+v", err)
				}

				xlog.Logger.Infof("---> SNAPSHOT: %+v. DONE.\n", rd.Snapshot)

				aw.store.ApplySnapshot(rd.Snapshot)
				/*
					snapOnDisk, _ := aw.store.Snapshot()

					if snapOnDisk.Metadata.Index != rd.Snapshot.Metadata.Index || snapOnDisk.Metadata.Term != rd.Snapshot.Metadata.Term {
						xlog.Logger.Errorf("[] snapshot not match... quit", n.RaftContext.Id)
						return
					}
				*/
				aw.Node.Applied.Done(rd.Snapshot.Metadata.Index)
				//aw.store.DeleteFrom(rd.Snapshot.Metadata.Index + 1)
				aw.Node.SetConfState(&rd.Snapshot.Metadata.ConfState)
			}

			n.SaveToStorage(rd.HardState, rd.Entries)

			span.Annotatef(nil, "Saved to storage")

			synced := false
			if createSnapshot {
				synced = aw.trySnapshot(300)
			}

			if rd.MustSync && !synced {
				if *strict {
					n.Store.Sync()
				} else {
					n.Store.Flush()
				}

			}

			span.Annotatef(nil, "Sync files done")

			for _, entry := range rd.CommittedEntries {
				n.Applied.Begin(entry.Index)
				switch {
				case entry.Type == raftpb.EntryConfChange:
					aw.applyConfChange(entry)
				case entry.Type == raftpb.EntryNormal:
					uniqKey, err := aw.applyProposal(entry)
					if err != nil {
						xlog.Logger.Errorf("While applying proposal: %v\n", err)
					}
					n.Proposals.Done(uniqKey, entry.Index, err)
				default:
					xlog.Logger.Warnf("Unhandled entry: %+v\n", entry)
				}
				xlog.Logger.Infof("commit index:%d, size:%d\n", entry.Index, entry.Size())
				n.Applied.Done(entry.Index)
			}
			span.Annotatef(nil, "Applied %d CommittedEntries", len(rd.CommittedEntries))

			if !leader {
				for i := range rd.Messages {
					aw.Node.Send(&rd.Messages[i])
				}
				aw.SetWorkerStatus(nil)
			}
			span.Annotate(nil, "Sent messages")

			n.Raft().Advance()
			span.Annotate(nil, "Advanced Raft")
			span.End()
		}
	}
}

func (aw *AspiraWorker) trySnapshot(skip uint64) (created bool) {
	existing, err := aw.Node.Store.Snapshot()
	utils.Check(err)
	si := existing.Metadata.Index
	doneUntil := aw.Node.Applied.DoneUntil()

	if doneUntil < si+skip {
		return
	}
	data, err := aw.Node.State.Marshal()
	utils.Check(err)
	xlog.Logger.Infof("Writing snapshot at index:%d\n", doneUntil-skip/2)
	created, err = aw.store.CreateSnapshot(doneUntil-skip/2, aw.Node.ConfState(), data)
	if err != nil {
		xlog.Logger.Warnf("trySnapshot have error %+v", err)
	}
	return created
}

func (aw *AspiraWorker) AmLeader() bool {
	return aw.Node.AmLeader()
}

var errInternalRetry = errors.New("Retry Raft proposal internally")

func (aw *AspiraWorker) getAndWait(ctx context.Context, index uint64) ([]byte, error) {
	n := aw.Node
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
		data, err := aw.store.GetData(index)
		ch <- result{data: data, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.Errorf("TIMEOUT")
	case result := <-ch:
		return result.data, result.err
	}
}

func (aw *AspiraWorker) proposeAndWait(ctx context.Context, proposal *aspirapb.AspiraProposal) (uint64, error) {
	n := aw.Node
	switch {
	case n.Raft() == nil:
		return 0, errors.Errorf("Raft isn't initialized yet.")
	case ctx.Err() != nil:
		return 0, ctx.Err()
		/*
			case !as.AmLeader():
				// Do this check upfront. Don't do this inside propose for reasons explained below.
				return 0, errors.Errorf("Not a leader. Aborting proposal: %+v", len(proposal.Data))
		*/
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

func (aw *AspiraWorker) applyConfChange(e raftpb.Entry) {

	var cc raftpb.ConfChange
	utils.Check(cc.Unmarshal(e.Data))
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if len(cc.Context) > 0 {
			var ctx aspirapb.RaftContext
			utils.Check(ctx.Unmarshal(cc.Context))
			aw.Node.Connect(ctx.Id, ctx.Addr)
			//update state
			aw.Node.Lock()
			aw.Node.State.Nodes[ctx.Id] = ctx.Addr
			aw.Node.Unlock()
		}

		xx := aw.Node.Raft().ApplyConfChange(cc)
		xlog.Logger.Infof("Apply CC %+v at index %d\n", xx, e.Index)
		aw.Node.SetConfState(xx)
		aw.Node.DoneConfChange(cc.ID, nil)
	case raftpb.ConfChangeRemoveNode:
	case raftpb.ConfChangeUpdateNode:
	}

}

func (aw *AspiraWorker) Stop() {
	aw.Node.Raft().Stop()
	aw.stopper.Close()
	//as.grpcServer.Stop()
}

/*
func main() {
	//1 => 127.0.0.1:3301
	//2 => 127.0.0.1:3302

	flag.Parse()
	if *hasJaeger {
		je, _ := jaeger.NewExporter(jaeger.Options{
			Endpoint:    "http://localhost:14268",
			ServiceName: "aspira",
		})
		otrace.RegisterExporter(je)
		otrace.ApplyConfig(otrace.Config{DefaultSampler: trace.AlwaysSample()})
	}

	xlog.InitLog(fmt.Sprintf("%d", *id))

	xlog.Logger.Infof("strict is %+v", *strict)
	stringID := fmt.Sprintf("%d", *id)
	var x *AspiraWorker
	x, err := NewAspiraWorker(*id, *addr, stringID+".lusf")
	if err != nil {
		panic(err.Error())
	}

	utils.Check(x.ServeGRPC())
	go x.ServeHTTP()

	x.InitAndStart(*id, *join)
}
*/

func (as *AspiraWorker) receiveSnapshot(snap raftpb.Snapshot) (err error) {
	//close and delete current store
	var memberStat aspirapb.MembershipState
	utils.Check(memberStat.Unmarshal(snap.Data))
	for _, remotePeer := range snap.Metadata.ConfState.Nodes {
		p := as.getPeerPool(remotePeer)
		if p == nil {
			continue
		}
		xlog.Logger.Infof("Snapshot.RaftContext.Addr: %+v", p.Addr)
		err = as.populateSnapshot(snap, p)
		if err == nil {
			break
		}
	}
	return
}

func (as *AspiraWorker) getPeerPool(peer uint64) *conn.Pool {
	if peer == as.Node.RaftContext.Id {
		return nil
	}
	addr, ok := as.Node.Peer(peer)
	if !ok {
		return nil
	}
	p, err := conn.GetPools().Get(addr)
	if err != nil {
		return nil
	}
	return p
}

func (aw *AspiraWorker) populateSnapshot(snap raftpb.Snapshot, pl *conn.Pool) (err error) {
	conn := pl.Get()
	c := aspirapb.NewRaftClient(conn)
	//c := aspirapb.NewAspiraGRPCClient(conn)
	xlog.Logger.Infof("know snapshot %d", snap.Metadata.Index)
	stream, err := c.StreamSnapshot(context.Background(), aw.Node.RaftContext)
	if err != nil {
		return
	}

	backupName := fmt.Sprintf("%s/backup-%d", filepath.Dir(aw.storePath), aw.Node.RaftContext.Id)
	file, err := os.OpenFile(backupName, os.O_CREATE|os.O_RDWR, 0755)

	defer file.Close()
	xlog.Logger.Infof("Start to receive data")
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
		xlog.Logger.Infof("recevied data %d", len(payload.Data))
	}
	xlog.Logger.Infof("End to receive data")

	aw.store.CloseDB()

	os.Remove(aw.storePath)
	os.Rename(backupName, aw.storePath)

	db, err := cannyls.OpenCannylsStorage(aw.storePath)
	if err != nil {
		return err
	}
	aw.store.SetDB(db)

	/*
		restart, err := aw.store.PastLife()
		if !restart || err != nil {
			return errors.Errorf("can not initial and replay backup snapshot")
		}
	*/
	sa, _ := aw.store.Snapshot()
	xlog.Logger.Infof("get snapshot %d", sa.Metadata.Index)
	return nil
}
