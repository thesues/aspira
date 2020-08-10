package zeroclient

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/xlog"
	"google.golang.org/grpc"
)

type ZeroClient struct {
	Conns      []*grpc.ClientConn
	lastLeader int
}

func NewZeroClient() *ZeroClient {
	return &ZeroClient{}
}

func (client *ZeroClient) Close() {
	for _, c := range client.Conns {
		c.Close()
	}
}

// Connect to Zero's grpc service, if all of connect failed to connnect, return error
func (client *ZeroClient) Connect(addrs []string) error {
	if len(addrs) == 0 {
		return errors.Errorf("addr is nil")
	}
	errCount := 0
	for _, addr := range addrs {
		c, err := grpc.Dial(addr, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
		if err != nil {
			errCount++
			continue
		}
		client.Conns = append(client.Conns, c)
	}

	if errCount == len(addrs) {
		return errors.Errorf("all connection failed")
	}
	client.lastLeader = 0
	return nil
}

// AllocID can query any node of zeros
func (client *ZeroClient) AllocID(count int) ([]uint64, error) {
	current := client.lastLeader
	loop := 0
	for {
		if client.Conns != nil && client.Conns[current] != nil {
			c := aspirapb.NewZeroClient(client.Conns[current])
			req := aspirapb.ZeroAllocIDRequest{
				Count: 1,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			res, err := c.AllocID(ctx, &req)
			cancel()
			if err != nil {
				current = (current + 1) % len(client.Conns)
				loop++
				if loop > len(client.Conns)*2 {
					return nil, errors.Errorf("AllocID failed")
				}
				continue
			}
			return []uint64{res.Start, res.End}, nil
		}

	}

}

func (client *ZeroClient) CreateHeartbeatStream() (aspirapb.Zero_StreamHeartbeatClient, context.CancelFunc, error) {
	loop := 0
	current := client.lastLeader
	for {
		if client.Conns != nil && client.Conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.Conns[current])
			ctx, cancel := context.WithCancel(context.Background())
			stream, err := zc.StreamHeartbeat(ctx)
			if err != nil {
				cancel()
				xlog.Logger.Warnf(err.Error())
				loop++
				current = (current + 1) % len(client.Conns)
				if loop > 3*len(client.Conns) {
					return nil, nil, errors.Errorf("RegisterSelfAsStore failed")
				}
				continue
			}
			return stream, cancel, nil
		}
	}
}

func (client *ZeroClient) RegisterSelfAsStore(req *aspirapb.ZeroRegistStoreRequest) error {

	loop := 0
	current := client.lastLeader

	for {
		if client.Conns != nil && client.Conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.Conns[current])
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := zc.RegistStore(ctx, req)
			cancel()
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				loop++
				current = (current + 1) % len(client.Conns)
				if loop > 3*len(client.Conns) {
					return errors.Errorf("RegisterSelfAsStore failed")
				}
				continue

			}
			client.lastLeader = current
			return nil
		}
	}
}

//FIXME, receive AddWorker command
