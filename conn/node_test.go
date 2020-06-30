/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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

package conn

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/raftwal"

	cannyls "github.com/thesues/cannyls-go/storage"
	"golang.org/x/net/context"
)

func (n *Node) run(wg *sync.WaitGroup) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.Raft().Tick()
		case rd := <-n.Raft().Ready():
			n.SaveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					n.Raft().ApplyConfChange(cc)
				} else if entry.Type == raftpb.EntryNormal {
					if bytes.HasPrefix(entry.Data, []byte("hey")) {
						fmt.Printf("got %s\n", entry.Data)
						wg.Done()
					}
				}
			}
			n.Raft().Advance()
		}
	}
}

func TestProposal(t *testing.T) {
	db, err := cannyls.CreateCannylsStorage("wal.lusf", 10<<20, 0.1)
	defer os.Remove("wal.lusf")
	assert.Nil(t, err)
	store := raftwal.Init(db)

	rc := &aspirapb.RaftContext{Id: 1}
	n := NewNode(rc, store)

	peers := []raft.Peer{{ID: n.Id}}
	n.SetRaft(raft.StartNode(n.Cfg, peers))

	loop := 5
	var wg sync.WaitGroup
	wg.Add(loop)
	go n.run(&wg)

	for i := 0; i < loop; i++ {
		data := []byte(fmt.Sprintf("hey-%d", i))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Raft().Propose(ctx, data))
	}
	wg.Wait()
}
