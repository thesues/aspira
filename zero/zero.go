package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	_ "github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	"github.com/thesues/cannyls-go/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/coreos/etcd/embed"
)

var (
	idKey = "AspiraIDKey"
)

type Zero struct {
	Client    *clientv3.Client
	Id        uint64
	EmbedEted *embed.Etcd

	Cfg          *ZeroConfig
	allocIdLock  sync.Mutex   //used in AllocID
	reLock       sync.RWMutex //protect clusterStore
	workerLock   sync.RWMutex //protect clusterWorker
	isLeader     int32
	auditStopper *util.Stopper

	clusterWorker map[uint64]*aspirapb.WorkerInfo    //in memory
	clusterStore  map[uint64]*aspirapb.ZeroStoreInfo //saved in etcd and loaded when zero become leader
	addrToStoreID map[string]uint64                  //in memory, construct from clusterStore, map addr => storeID
	policy        RebalancePolicy
}

// NewZero, initial in-memory struct of Zero
func NewZero() *Zero {
	z := new(Zero)
	z.clusterStore = make(map[uint64]*aspirapb.ZeroStoreInfo)
	z.clusterWorker = make(map[uint64]*aspirapb.WorkerInfo)
	z.auditStopper = util.NewStopper()
	return z
}

//interface to etcd
func (z *Zero) listEtcdMembers() (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(z.Client.Ctx(), time.Second)
	defer cancel()
	return z.Client.MemberList(ctx)
}

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

func (z *Zero) getCurrentLeader() uint64 {
	return uint64(z.EmbedEted.Server.Leader())
}

func (z *Zero) amLeader() bool {
	return z.Id == z.getCurrentLeader()
}

/*
func (z *Zero) Report() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if z.amLeader() {
				xlog.Logger.Infof("I am leader %d", z.Id)
			}
		}
	}
}
*/

func (z *Zero) audit() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-z.auditStopper.ShouldStop():
			return
		case <-ticker.C:
			xlog.Logger.Info("audit")
		}
	}
}

func (z *Zero) LeaderLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if z.amLeader() && atomic.LoadInt32(&z.isLeader) == 0 {
				z.auditStopper.RunWorker(z.audit)
				atomic.StoreInt32(&z.isLeader, 1)
			} else if !z.amLeader() && atomic.LoadInt32(&z.isLeader) == 1 {
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

func (z *Zero) RegistStore(ctx context.Context, req *aspirapb.ZeroRegistStoreRequest) (*aspirapb.ZeroRegistStoreResponse, error) {
	if !z.amLeader() {
		return &aspirapb.ZeroRegistStoreResponse{}, errors.Errorf("not a leader")
	}
	z.reLock.Lock()
	defer z.reLock.Unlock()

	if _, ok := z.clusterStore[req.StoreId]; ok {
		xlog.Logger.Infof("store %d %s already registered", req.StoreId, req.Name)
		return &aspirapb.ZeroRegistStoreResponse{}, errors.Errorf("already registered")
	}

	z.clusterStore[req.StoreId] = &aspirapb.ZeroStoreInfo{
		Address:     req.Address,
		StoreId:     req.StoreId,
		EmtpySlots:  req.EmtpySlots,
		CurrentGids: nil,
		Name:        req.Name,
	}

	key := fmt.Sprintf("store_%d", req.StoreId)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	txn := clientv3.NewKV(z.Client).Txn(ctx)
	xxx, err := z.clusterStore[req.StoreId].Marshal()
	if err != nil {
		xlog.Logger.Warnf(err.Error())
	}
	_, err = txn.Then(clientv3.OpPut(key, string(xxx))).Commit()
	return &aspirapb.ZeroRegistStoreResponse{}, nil
	/*
		var stores []*aspirapb.ZeroStoreInfo
		for _, v := range z.clusterStore {
			stores = append(stores, v)
		}

		targetStore := z.policy.AllocNewRaftGroup(req.Gid, 3, stores)
		if targetStore == nil {
			return &aspirapb.ZeroRegistStoreResponse{}, errors.Errorf("can not allocate for %d", req.Gid)
		}

		//asdf
		//
		res := &aspirapb.ZeroRegistStoreResponse{
			Stores: targetStore,
		}
		return res, nil
	*/
}

func (z *Zero) AllocID(ctx context.Context, req *aspirapb.ZeroAllocIDRequest) (*aspirapb.ZeroAllocIDResponse, error) {

	if req.Count == 0 {
		return &aspirapb.ZeroAllocIDResponse{}, errors.New("request count can not be 1")
	}
	var err error
	z.allocIdLock.Lock()
	defer z.allocIdLock.Unlock()

	curValue, err := z.getValue(idKey)
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
