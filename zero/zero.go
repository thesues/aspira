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
	"go.etcd.io/etcd/raft"
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
	idLock       sync.Mutex
	isLeader     int32
	auditStopper *util.Stopper

	memberProgress map[uint64]raft.ProgressStateType
	memberState    aspirapb.MembershipState
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
	z.auditStopper = util.NewStopper()
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

	aspirapb.RegisterZeroGRPCServer(s, z)

	listener, err := net.Listen("tcp", z.Cfg.GrpcUrl)
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	go func() {
		s.Serve(listener)
	}()
}

func (z *Zero) ZeroStatus(context.Context, *aspirapb.ZeroStatusRequest) (*aspirapb.ZeroStatusResponse, error) {
	return nil, nil
}

//grpc services
func (z *Zero) WorkerHeartbeat(context.Context, *aspirapb.WorkerHeartbeatRequest) (*aspirapb.WorkerHeartbeatResponse, error) {
	return nil, nil
}

func (z *Zero) AllocID(context.Context, *aspirapb.AllocIDRequest) (*aspirapb.AllocIDResponse, error) {
	var err error
	z.idLock.Lock()
	defer z.idLock.Unlock()

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
	binary.BigEndian.PutUint64(newValue[:], curr+1)

	txn := clientv3.NewKV(z.Client).Txn(context.Background())
	t := txn.If(cmp)
	resp, err := t.Then(clientv3.OpPut(idKey, string(newValue[:]))).Commit()
	if err != nil {
		return nil, err
	}
	if !resp.Succeeded {
		return nil, errors.New("generate id failed, we may not leader")
	}
	return &aspirapb.AllocIDResponse{ID: curr + 1}, nil
}
