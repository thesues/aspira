package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	_ "github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"time"

	"github.com/coreos/etcd/embed"
)

var (
	idKey = "AspiraIDKey"
)

type Zero struct {
	Client    *clientv3.Client
	Id        uint64
	EmbedEted *embed.Etcd

	Cfg    *ZeroConfig
	idLock sync.Mutex
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

func (z *Zero) ServGRPC() {
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1<<25),
		grpc.MaxSendMsgSize(1<<25),
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

//grpc services
func (z *Zero) LeaderReport(aspirapb.ZeroGRPC_LeaderReportServer) error {
	return nil
}

func (z *Zero) AllocID(context.Context, *aspirapb.AllocIDRequest) (*aspirapb.AllocIDResponse, error) {
	var err error
	if !z.amLeader() {
		return nil, errors.Errorf("bad")
	}
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
