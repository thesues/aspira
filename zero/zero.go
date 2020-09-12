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

package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"

	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/utils"
	_ "github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	"github.com/thesues/cannyls-go/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	idKey = "AspiraIDKey"
)

func storeKey(id uint64) string {
	return fmt.Sprintf("store_%d", id)
}

func workerKey(id uint64) string {
	return fmt.Sprintf("worker_%d", id)
}

type workerProgress struct {
	workerInfo *aspirapb.ZeroWorkerInfo
	progress   aspirapb.WorkerStatus_ProgressType
}

type storeProgress struct {
	storeInfo *aspirapb.ZeroStoreInfo
	lastEcho  time.Time
}

type Zero struct {
	Client    *clientv3.Client
	Id        uint64
	EmbedEted *embed.Etcd

	Cfg         *ZeroConfig
	allocIdLock sync.Mutex //used in AllocID

	sync.RWMutex //protect gidToWorkerID, workers, stores, gidFreeBytes
	isLeader     int32
	auditStopper *util.Stopper

	gidToWorkerID map[uint64][]uint64 //gid => workerID, update when AllocWorked is commited.
	gidFreeBytes  *sync.Map           //gid => FreeBytes, update when get heartbeat from worker.

	workers map[uint64]*workerProgress //workerID => workinfo{storeID, gid}, "progress", where workinfo is saved in etcd.
	stores  map[uint64]*storeProgress  //storeID=> storeInfo{name, address}, "last echo" where storeInfo is saved in etcd.

	policy RebalancePolicy
}

// NewZero, initial in-memory struct of Zero
func NewZero() *Zero {
	z := new(Zero)
	z.auditStopper = util.NewStopper()
	return z
}

//interface to etcd
func (z *Zero) listEtcdMembers() (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(z.Client.Ctx(), time.Second)
	defer cancel()
	return z.Client.MemberList(ctx)
}

/*
func (z *Zero) getValue(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := clientv3.NewKV(z.Client).Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}
*/

func (z *Zero) getCurrentLeader() uint64 {
	return uint64(z.EmbedEted.Server.Leader())
}

//_amLeader is call in func leadLoop
func (z *Zero) _amLeader() bool {
	return z.Id == z.getCurrentLeader()
}

func (z *Zero) amLeader() bool {
	return atomic.LoadInt32(&z.isLeader) == 1
}

func (z *Zero) audit() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-z.auditStopper.ShouldStop():
			return
		case <-ticker.C:
			//xlog.Logger.Info("audit")
			//xlog.Logger.Info(z.Display())
		}
	}
}

//buildGidToWorker needs extern lock
func (z *Zero) buildGidToWorker() {
	gidToWorkerId := make(map[uint64][]uint64)
	for _, w := range z.workers {
		xlog.Logger.Infof("GID is %d ", w.workerInfo.Gid)
		gidToWorkerId[w.workerInfo.Gid] = append(gidToWorkerId[w.workerInfo.Gid], w.workerInfo.WorkId)
	}

	z.gidToWorkerID = gidToWorkerId
}

func (z *Zero) LeaderLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			//became leader
			if z._amLeader() && atomic.LoadInt32(&z.isLeader) == 0 {
				//load from etcd
				xlog.Logger.Infof("new leader, read from etcd")
				var err error
				z.Lock()
				z.stores, err = loadStores(z.Client)
				if err != nil {
					xlog.Logger.Warnf("can not load store", err.Error())
					z.Unlock()
					continue
				}
				z.workers, err = loadWorkers(z.Client)
				if err != nil {
					xlog.Logger.Warnf("can not load workers", err.Error())
					z.Unlock()
					continue
				}
				z.buildGidToWorker()
				z.Unlock()

				z.gidFreeBytes = new(sync.Map)
				atomic.StoreInt32(&z.isLeader, 1)
				z.auditStopper.RunWorker(z.audit)

			} else if !z._amLeader() && atomic.LoadInt32(&z.isLeader) == 1 {
				//lost leader
				//stop audit
				z.auditStopper.Stop()
				atomic.StoreInt32(&z.isLeader, 0)
			}
		}

	}
}

func (z *Zero) ServGRPC() {
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(4<<20),
		grpc.MaxSendMsgSize(4<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	aspirapb.RegisterZeroServer(s, z)

	listener, err := net.Listen("tcp", z.Cfg.GrpcUrl)
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	go func() {
		s.Serve(listener)
	}()
}

/*
func (z *Zero) ZeroStatus(context.Context, *aspirapb.ZeroStatusRequest) (*aspirapb.ZeroStatusResponse, error) {
	return nil, nil
}

//grpc services
func (z *Zero) WorkerHeartbeat(context.Context, *aspirapb.WorkerHeartbeatRequest) (*aspirapb.WorkerHeartbeatResponse, error) {
	return nil, nil
}
*/

//FIXME: because store will report the status to leader, so reuse the stream to send addWorker info.
//if so, we can be sure that it's leader
/*
func (z *Zero) buildRaftGroup(stores []*aspirapb.ZeroStoreInfo) error {
	conns := make([]*grpc.ClientConn, len(store))
	for _, s := range stores {
		conn, err := grpc.Dial(s, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
		if err != nil {
			return err
		}
		conns = append(conns, conn)
	}
	//create primary node first//

	client := aspirapb.NewStoreClient(conn)
	//block until raft group started
	req := aspirapb.AddWorkerRequest{
		Gid:         gid,
		Id:          id,
		JoinCluster: remoteCluster,
	}
	_, err = client.AddWorker(context.Background(), &req)
	if err != nil {
		return err
	}
	fmt.Printf("Success\n")
	return nil
}
*/

var (
	ErrNotLeader = errors.Errorf("not a leader")
)

func formatInitalCluster(stores []*aspirapb.ZeroStoreInfo, ids []uint64) string {
	remotes := make([]string, len(stores))
	for i := 0; i < len(stores); i++ {
		remotes[i] = fmt.Sprintf("%d;%s", ids[i], stores[i].Address)
	}
	return strings.Join(remotes, ",")
}

func (z *Zero) AddWorkerGroup(ctx context.Context, req *aspirapb.ZeroAddWorkerGroupRequest) (*aspirapb.ZeroAddWorkerGroupResponse, error) {
	if !z.amLeader() {
		return nil, ErrNotLeader
	}
	alloReq := aspirapb.ZeroAllocIDRequest{
		Count: 4,
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)

	res, err := z.AllocID(ctx, &alloReq)
	cancel()
	if err != nil {
		return nil, err
	}

	ids := make([]uint64, 4)
	ids[0] = res.Start
	for i := 1; i < 4; i++ {
		ids[i] = ids[i-1] + 1
	}

	gid := ids[0]
	ids = ids[1:]
	//the first id is GID

	var candidatStores []*aspirapb.ZeroStoreInfo
	z.RLock()
	for _, store := range z.stores {
		//if time.Now().Sub(store.lastEcho) < time.Minute {
		candidatStores = append(candidatStores, proto.Clone(store.storeInfo).(*aspirapb.ZeroStoreInfo))
		//}
	}
	z.RUnlock()

	targetStores := z.policy.AllocNewRaftGroup(ids[0], 3, candidatStores)
	if targetStores == nil || len(targetStores) != len(ids) {
		return nil, errors.Errorf("can not alloc enough workers in this cluster")
	}

	stopper := util.NewStopper()
	resultChan := make(chan error, len(targetStores))
	initialCluster := formatInitalCluster(targetStores, ids)

	for i := 0; i < len(targetStores); i++ {
		myTarget := targetStores[i]
		id := ids[i]
		stopper.RunWorker(func() {
			conn, err := grpc.Dial(myTarget.Address, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
			defer conn.Close()
			if err != nil {
				resultChan <- err
				return
			}
			client := aspirapb.NewStoreClient(conn)
			req := aspirapb.AddWorkerRequest{
				Gid:            gid,
				Id:             id,
				InitialCluster: initialCluster,
				Type:           aspirapb.TnxType_prepare,
			}

			xlog.Logger.Infof("AddWorkerRequest %+v", req)
			_, err = client.AddWorker(context.Background(), &req)
			resultChan <- err
		})
	}
	stopper.Wait()
	close(resultChan)
	for myErr := range resultChan {
		if myErr != nil {
			err = errors.Errorf("[zero], one of remote error: [%+v]", myErr)
			xlog.Logger.Warnf(err.Error())
			return &aspirapb.ZeroAddWorkerGroupResponse{Gid: gid}, err
		}
	}

	var ops []clientv3.Op
	for i := 0; i < len(targetStores); i++ {
		myTarget := targetStores[i]
		id := ids[i]
		val := aspirapb.ZeroWorkerInfo{
			WorkId:  id,
			StoreId: myTarget.StoreId,
			Gid:     gid,
		}
		data, err := val.Marshal()
		utils.Check(err)
		ops = append(ops, clientv3.OpPut(workerKey(id), string(data)))
	}
	err = EtctSetKVS(z.Client, ops)
	if err != nil {
		return &aspirapb.ZeroAddWorkerGroupResponse{}, err
	}

	xlog.Logger.Infof("wrote to etcd")

	//change memory state
	z.Lock()
	for i, id := range ids {
		z.workers[id] = &workerProgress{
			workerInfo: &aspirapb.ZeroWorkerInfo{
				WorkId:  id,
				Gid:     gid,
				StoreId: targetStores[i].StoreId,
			},
			progress: aspirapb.WorkerStatus_Unknown,
		}
	}
	z.buildGidToWorker()
	z.Unlock()

	//send commit info
	//TODO reuse conns
	for i := 0; i < len(targetStores); i++ {
		myTarget := targetStores[i]
		id := ids[i]
		go func() {
			conn, err := grpc.Dial(myTarget.Address, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
			defer conn.Close()
			if err != nil {
				xlog.Logger.Warnf("can not connect store %s", myTarget.Address)
				return
			}
			client := aspirapb.NewStoreClient(conn)
			req := aspirapb.AddWorkerRequest{
				Gid:            gid,
				Id:             id,
				InitialCluster: initialCluster,
				Type:           aspirapb.TnxType_commit,
			}
			_, err = client.AddWorker(context.Background(), &req)
			if err != nil {
				xlog.Logger.Warnf(err.Error())
			}
		}()
	}
	xlog.Logger.Infof("CREATE [%d]", gid)
	return &aspirapb.ZeroAddWorkerGroupResponse{Gid: gid}, nil
}

func (z *Zero) RegistStore(ctx context.Context, req *aspirapb.ZeroRegistStoreRequest) (*aspirapb.ZeroRegistStoreResponse, error) {
	if !z.amLeader() {
		return &aspirapb.ZeroRegistStoreResponse{}, ErrNotLeader
	}
	z.Lock()
	defer z.Unlock()

	if _, ok := z.stores[req.StoreId]; ok {
		xlog.Logger.Infof("store %d %s already registered", req.StoreId, req.Name)
		return &aspirapb.ZeroRegistStoreResponse{}, errors.Errorf("already registered")
	}

	sInfo := &aspirapb.ZeroStoreInfo{
		Address: req.Address,
		StoreId: req.StoreId,
		Slots:   req.EmtpySlots,
		Name:    req.Name,
	}

	z.stores[req.StoreId] = &storeProgress{
		storeInfo: sInfo,
		lastEcho:  time.Now(),
	}

	key := storeKey(req.StoreId)
	val, err := sInfo.Marshal()
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return nil, err
	}
	if err = EtcdSetKV(z.Client, key, val); err != nil {
		return nil, err
	}

	return &aspirapb.ZeroRegistStoreResponse{}, nil
}

func (z *Zero) AllocID(ctx context.Context, req *aspirapb.ZeroAllocIDRequest) (*aspirapb.ZeroAllocIDResponse, error) {

	if req.Count == 0 {
		return &aspirapb.ZeroAllocIDResponse{}, errors.New("request count can not be 1")
	}
	var err error
	z.allocIdLock.Lock()
	defer z.allocIdLock.Unlock()

	curValue, err := EtcdGetKV(z.Client, idKey)
	if err != nil {
		return nil, err
	}

	//build txn, compare and set ID
	var cmp clientv3.Cmp
	var curr uint64

	if curValue == nil {
		cmp = clientv3.Compare(clientv3.CreateRevision(idKey), "=", 0)
	} else {
		curr = binary.BigEndian.Uint64(curValue)
		cmp = clientv3.Compare(clientv3.Value(idKey), "=", string(curValue))
	}

	var newValue [8]byte
	binary.BigEndian.PutUint64(newValue[:], curr+uint64(req.Count))

	txn := clientv3.NewKV(z.Client).Txn(context.Background())
	t := txn.If(cmp)
	resp, err := t.Then(clientv3.OpPut(idKey, string(newValue[:]))).Commit()
	if err != nil {
		return nil, err
	}
	if !resp.Succeeded {
		return nil, errors.New("generate id failed, we may not leader")
	}
	return &aspirapb.ZeroAllocIDResponse{Start: curr, End: curr + uint64(req.Count)}, nil
}

func (z *Zero) QueryWorker(ctx context.Context, req *aspirapb.ZeroQueryWorkerRequest) (*aspirapb.ZeroQueryWorkerResponse, error) {
	z.RLock()
	defer z.RLocker()
	p, ok := z.workers[req.Id]
	if !ok {
		return &aspirapb.ZeroQueryWorkerResponse{Type: aspirapb.TnxType_abort}, nil
	}
	if p.workerInfo.Gid == req.Gid && p.workerInfo.StoreId == req.StoreId {
		return &aspirapb.ZeroQueryWorkerResponse{Type: aspirapb.TnxType_commit}, nil
	}
	return &aspirapb.ZeroQueryWorkerResponse{Type: aspirapb.TnxType_abort}, nil
}

//func (z *Zero)
func (z *Zero) ClusterStatus(ctx context.Context, request *aspirapb.ClusterStatusRequest) (*aspirapb.ClusterStatusResponse, error) {
	z.RLock()
	defer z.RUnlock()
	var groups []*aspirapb.GroupStatus
	for gid, workers := range z.gidToWorkerID {
		gs := aspirapb.GroupStatus{
			Gid:       gid,
			FreeBytes: z.getFreeBytes(gid),
		}
		var stores []*aspirapb.ZeroStoreInfo //leader is the first, if no leader, continue

		//one group has 3 workers, find the corresponding store respectly.
		var hasLeader bool
		for _, workerID := range workers {
			w := z.workers[workerID]

			//assert
			if w.workerInfo.Gid != gid {
				xlog.Logger.Warnf("internal data maybe not corrent")
				continue
			}
			s := z.stores[w.workerInfo.StoreId].storeInfo
			//if the worker is leader, put the leader in front
			if w.progress == aspirapb.WorkerStatus_Leader {
				stores = append([]*aspirapb.ZeroStoreInfo{s}, stores...)
				hasLeader = true
			} else {
				stores = append(stores, s)
			}
		}
		if !hasLeader {
			xlog.Logger.Infof("group [%d] has not leader", gid)
			continue
		}
		gs.Stores = stores
		groups = append(groups, &gs)
	}

	return &aspirapb.ClusterStatusResponse{
		Groups: groups,
	}, nil
}
