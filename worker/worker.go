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
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"

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
	//applyCh
	//proposeCh
	//getCh
}

func NewAspiraServer(id uint64, addr string, path string) (as *AspiraServer, err error) {

	raftCxt := aspirapb.RaftContext{Id: id, Addr: addr}

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
	node := conn.NewNode(&raftCxt, store)
	raftServer := conn.NewRaftServer(node)

	as = &AspiraServer{
		node:       node,
		raftServer: raftServer,
		addr:       addr,
		stopper:    utils.NewStopper(),
		store:      store,
	}
	return as, nil
}

func (as *AspiraServer) InitAndStart(id uint64) (err error) {
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
	peers := as.store.MemberShip()
	if len(peers.Nodes) > 0 {
		/*
			for id, addr := range peers.Nodes {
				fmt.Printf("%d => %s\n", id, addr)
				as.node.Connect(id, addr)
			}
		*/
		as.node.SetRaft(raft.RestartNode(as.node.Cfg))
		fmt.Printf("RESTART")
	} else {
		fmt.Printf("START")
		rpeers := make([]raft.Peer, 3)
		var i uint64
		for i = 1; i <= 3; i++ {
			rpeers[i-1] = raft.Peer{ID: i}
		}
		for i = 1; i <= 3; i++ {
			as.node.Connect(i, "127.0.0.1:330"+fmt.Sprintf("%d", i))
		}
		as.node.SetRaft(raft.StartNode(as.node.Cfg, rpeers))
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

func (as *AspiraServer) applyConfChange(e raftpb.Entry) {
	var cc raftpb.ConfChange
	utils.Check(cc.Unmarshal(e.Data))
	fmt.Printf("applyConfChange: %+v\n", cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		addr, has := as.node.Peer(cc.NodeID)
		if has {
			go as.node.Connect(cc.NodeID, addr)
			fmt.Printf("connected to %s for %d\n", addr, cc.NodeID)
		}
	case raftpb.ConfChangeRemoveNode:
	case raftpb.ConfChangeUpdateNode:
	}
	//as.node.SavePeers()
	as.node.Raft().ApplyConfChange(cc)

}

var (
	id = flag.Uint64("id", 0, "id")
)

func main() {
	//1 => 127.0.0.1:3301
	//2 => 127.0.0.1:3302

	flag.Parse()

	stringId := fmt.Sprintf("%d", *id)
	x, _ := NewAspiraServer(*id, "127.0.0.1:330"+stringId, stringId+".lusf")

	x.InitAndStart(*id)

}
