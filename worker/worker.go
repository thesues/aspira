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
	"google.golang.org/grpc"
)

type AspiraServer struct {
	node       *conn.Node
	raftServer *conn.RaftServer //internal comms
	grpcServer *grpc.Server     //internal comms, raftServer is registered on grpcServer
	store      *raftwal.WAL
	addr       string
	stopper    *utils.Stopper
	state      aspirapb.MembershipState
	//applyCh
	//proposeCh
	//getCh
}

func NewAspiraServer(id uint64, addr string, path string) (as *AspiraServer, err error) {

	var db *cannyls.Storage

	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		if db, err = cannyls.CreateCannylsStorage(path, 10<<20, 0.1); err != nil {
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
		state: aspirapb.MembershipState{
			Nodes: make(map[uint64]string),
		},
	}
	return as, nil
}

func (as *AspiraServer) InitAndStart(id uint64, clusterAddr string) (err error) {
	if err = as.serveGRPC(); err != nil {
		return
	}

	/*
		snap, _ := as.node.Store.Snapshot()
		if raft.IsEmptySnap(snap) == false {
			//restore conf state,
			as.node.SetConfState(&snap.Metadata.ConfState)
			for _, id := range snap.Metadata.ConfState.Nodes {
				as.node.Connect(id)
			}
			//restart Node, nil
		} else {

			//start node
		}
	*/
	//static peers
	restart := as.store.PastLife()
	if restart {
		snap, err := as.store.Snapshot()
		utils.Check(err)
		as.node.SetConfState(&snap.Metadata.ConfState) //for future snapshot
		for i := 1; i <= 3; i++ {
			as.node.Connect(uint64(i), "127.0.0.1:330"+fmt.Sprintf("%d", i))
		}
		as.node.SetRaft(raft.RestartNode(as.node.Cfg))
		fmt.Printf("RESTART\n")
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
			return errors.Errorf("Unhealthy connection to %v", clusterAddr)
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
		as.node.BatchAndSendMessages()
	})
	as.Run()
	return
}

func (as *AspiraServer) serveGRPC() (err error) {
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1<<25),
		grpc.MaxSendMsgSize(1<<25),
		grpc.MaxConcurrentStreams(1000),
	)

	aspirapb.RegisterRaftServer(s, as.raftServer)
	listener, err := net.Listen("tcp", as.addr)
	if err != nil {
		return err
	}
	as.stopper.RunWorker(func() {
		s.Serve(listener)
	})
	as.grpcServer = s
	return nil
}

func (as *AspiraServer) Run() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	n := as.node
	for {
		select {
		case <-ticker.C:
			n.Raft().Tick()
		case rd := <-n.Raft().Ready():
			/*
				if rd.SoftState != nil {
					leader := rd.RaftState == raft.StateLeader
					if leader {
						fmt.Printf("I am leader...\n")
					} else {
						fmt.Printf("I am not leader...\n")
					}
				}*/

			n.SaveToStorage(rd.HardState, rd.Entries, rd.Snapshot)

			if rd.MustSync {
				n.Store.Sync()
			}

			for i := range rd.Messages {
				//fmt.Printf("sending %+v", rd.Messages)
				as.node.Send(&rd.Messages[i])
			}

			for _, entry := range rd.CommittedEntries {
				switch {
				case entry.Type == raftpb.EntryConfChange:
					as.applyConfChange(entry)
					/*
						var cc raftpb.ConfChange
						cc.Unmarshal(entry.Data)
						newConf := n.Raft().ApplyConfChange(cc)
						fmt.Printf("%+v\n", newConf)
						n.SetConfState(newConf)
					*/
					//set membership
					//glog.Infof("Done applying conf change at %#x", n.Id)
				case entry.Type == raftpb.EntryNormal:

					fmt.Printf("%+v COMMITED\n", entry)
					/*
						key, err := n.applyProposal(entry)
						if err != nil {
							glog.Errorf("While applying proposal: %v\n", err)
						}
						n.Proposals.Done(key, err)
					*/

				default:
					glog.Warningf("Unhandled entry: %+v\n", entry)
				}

			}

			n.Raft().Advance()
		}
	}
}

/*
func (as *AspiraServer) trySnapshot() {
	data, err := as.state.Marshal()
	utils.Check(err)

	as.store.CreateSnapshot()
}
*/

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
			go as.node.Connect(ctx.Id, ctx.Addr)
			//update state
			as.state.Nodes[ctx.Id] = ctx.Addr
		}
	case raftpb.ConfChangeRemoveNode:
	case raftpb.ConfChangeUpdateNode:
	}

	as.node.SetConfState(as.node.Raft().ApplyConfChange(cc))
	as.node.DoneConfChange(cc.ID, nil)

}

var (
	id   = flag.Uint64("id", 0, "id")
	join = flag.String("join", "", "remote addr")
)

func main() {
	//1 => 127.0.0.1:3301
	//2 => 127.0.0.1:3302

	flag.Parse()

	stringId := fmt.Sprintf("%d", *id)
	x, _ := NewAspiraServer(*id, "127.0.0.1:330"+stringId, stringId+".lusf")

	x.InitAndStart(*id, *join)

}
