/*
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

package aspiraclient

import (
	"context"
	"io"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/utils"
	zeroclient "github.com/thesues/aspira/zero_client"
	"google.golang.org/grpc"
)

type atomicGroups struct {
	groups []*aspirapb.GroupStatus
	index  map[uint64]int //gid=>pos in groups
}

type AspiraClient struct {
	zeroClient *zeroclient.ZeroClient

	sync.RWMutex //protect conns, works as a connection pool
	conns        map[string]*grpc.ClientConn

	zeroAddrs []string
	p         unsafe.Pointer //pointer to atomicGroups

	rand *rand.Rand
}

//thread-safe rand
type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *lockedSource) Int63() int64 {
	r.lk.Lock()
	defer r.lk.Unlock()
	return r.src.Int63()
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	defer r.lk.Unlock()
	r.src.Seed(seed)
}

func NewAspiraClient(addrs []string) *AspiraClient {
	ac := AspiraClient{
		zeroClient: zeroclient.NewZeroClient(),
		conns:      make(map[string]*grpc.ClientConn),
		zeroAddrs:  addrs,
		rand:       rand.New(&lockedSource{src: rand.NewSource(time.Now().UnixNano())}),
	}
	return &ac
}

func (ac *AspiraClient) Groups() *atomicGroups {
	return (*atomicGroups)(atomic.LoadPointer(&ac.p))
}

//UpdateClusterStatus block to receive groups
func (ac *AspiraClient) UpdateClusterStatus() error {
	//support version in the future
	groups, err := ac.zeroClient.ClusterStatus()
	if err != nil {
		return err
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].FreeBytes > groups[j].FreeBytes
	})
	index := make(map[uint64]int)
	for i, g := range groups {
		index[g.Gid] = i
	}
	g := atomicGroups{
		groups: groups,
		index:  index,
	}
	atomic.StorePointer(&ac.p, unsafe.Pointer(&g))
	return nil
}

//Connect to zero address
func (ac *AspiraClient) Connect() error {
	if len(ac.zeroAddrs) == 0 {
		return errors.Errorf("zero address are not set")
	}
	err := ac.zeroClient.Connect(ac.zeroAddrs)
	if err != nil {
		return err
	}
	return ac.UpdateClusterStatus()
}

//GetConn get grpc.Connection from pool, if not connected, create new
func (ac *AspiraClient) GetConn(addr string) (*grpc.ClientConn, error) {
	ac.RLock()
	defer ac.RUnlock()
	conn, ok := ac.conns[addr]
	if ok {
		return conn, nil
	}

	conn, err := grpc.Dial(addr,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(33<<20),
			grpc.MaxCallSendMsgSize(33<<20)),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	ac.conns[addr] = conn
	return conn, nil
}

func (ac *AspiraClient) PullStream(writer io.Writer, gid, oid uint64) error {
	var group *aspirapb.GroupStatus
	ag := ac.Groups() //groups is sorted by FreeBytes.

	var i int
	var ok bool
	if i, ok = ag.index[gid]; !ok {
		return errors.Errorf("can not find gid [%d]", gid)
	}

	targetStores := ag.groups[i].Stores
	if len(targetStores) == 0 {
		return errors.Errorf("no group found")
	}

	n := ac.rand.Intn(len(targetStores))
	var err error
	for loop := 0; loop < 3; loop++ {
		conn, err := ac.GetConn(targetStores[n].Address)
		if err != nil {
			n = (n + 1) % len(group.Stores)
			continue
		}
		err = ac.pull(writer, conn, gid, oid)
		if err == nil {
			return nil
		}
		n = (n + 1) % len(group.Stores)
	}
	return err
}

func (ac *AspiraClient) pull(writer io.Writer, conn *grpc.ClientConn, gid, oid uint64) error {
	client := aspirapb.NewStoreClient(conn)
	getStream, err := client.Get(context.Background(), &aspirapb.GetRequest{
		Gid: gid,
		Oid: oid,
	})
	if err != nil {
		return err
	}

	for {
		payload, err := getStream.Recv()
		if err != nil && err != io.EOF {
			return err
		}
		if payload != nil {
			if _, err = writer.Write(payload.Data); err != nil {
				return err
			}

		} else {
			break
		}
	}
	return nil
}

func (ac *AspiraClient) PushStream(reader io.Reader) (uint64, uint64, error) {
	groups := ac.Groups().groups
	selectedGroup := groups[:utils.Min(3, len(groups))]
	if len(selectedGroup) <= 0 {
		return 0, 0, errors.Errorf("no group found")
	}
	n := ac.rand.Intn(len(selectedGroup))

	for loop := 0; loop < 3; loop++ {
		storeAddr := selectedGroup[n].Stores[0].Address
		gid := selectedGroup[n].Gid

		conn, err := ac.GetConn(storeAddr)
		if err != nil {
			n = (n + 1) % len(selectedGroup)
			continue
		}
		_, oid, err := ac.push(reader, conn, gid)
		if err == nil {
			return gid, oid, err
		}
		n = (n + 1) % len(selectedGroup)
	}
	return 0, 0, errors.Errorf("tried 3 different gid, all failed")

}

func (ac *AspiraClient) push(reader io.Reader, conn *grpc.ClientConn, gid uint64) (uint64, uint64, error) {

	client := aspirapb.NewStoreClient(conn)
	putStream, err := client.PutStream(context.Background())

	if err != nil {
		return 0, 0, err
	}

	req := aspirapb.PutStreamRequest{
		Data: &aspirapb.PutStreamRequest_Gid{
			Gid: gid,
		},
	}

	err = putStream.Send(&req)
	utils.Check(err)
	buf := make([]byte, 64<<10)
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return 0, 0, err
		}
		if n == 0 {
			break
		}

		req := aspirapb.PutStreamRequest{
			Data: &aspirapb.PutStreamRequest_Payload{
				Payload: &aspirapb.Payload{
					Data: buf[:n],
				},
			},
		}
		err = putStream.Send(&req)
		utils.Check(err)
	}
	res, err := putStream.CloseAndRecv()
	return res.Gid, res.Oid, err
}
