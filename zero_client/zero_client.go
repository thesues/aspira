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

package zeroclient

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
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
	client.Conns = nil
}

func (client *ZeroClient) Alive() bool {
	return len(client.Conns) > 0
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
				time.Sleep(50 * time.Millisecond)
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
				loop++
				current = (current + 1) % len(client.Conns)
				if loop > 3*len(client.Conns) {
					return nil, nil, errors.Errorf("CreateHeartbeatStream failed")
				}
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return stream, cancel, nil
		}
	}
}

func (client *ZeroClient) QueryWorker(id, gid, storeID uint64) (aspirapb.TnxType, error) {
	loop := 0
	current := client.lastLeader
	for {
		if client.Conns != nil && client.Conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.Conns[current])
			ctx, cancel := context.WithCancel(context.Background())
			req := aspirapb.ZeroQueryWorkerRequest{
				Id:      id,
				Gid:     gid,
				StoreId: storeID,
			}
			res, err := zc.QueryWorker(ctx, &req)
			cancel()
			if err != nil {
				current = (current + 1) % len(client.Conns)
				loop++
				if loop > len(client.Conns)*2 {
					return aspirapb.TnxType_retry, errors.Errorf("QueryWorker failed")
				}
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return res.Type, nil
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
				loop++
				current = (current + 1) % len(client.Conns)
				if loop > 3*len(client.Conns) {
					return errors.Errorf("RegisterSelfAsStore failed")
				}
				time.Sleep(time.Second)
				continue

			}
			client.lastLeader = current
			return nil
		}
	}
}

func (client *ZeroClient) ClusterStatus() ([]*aspirapb.GroupStatus, error) {
	loop := 0
	current := client.lastLeader

	for {
		if client.Conns != nil && client.Conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.Conns[current])
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			res, err := zc.ClusterStatus(ctx, &aspirapb.ClusterStatusRequest{})
			cancel()
			if err != nil {
				loop++
				current = (current + 1) % len(client.Conns)
				if loop > 3*len(client.Conns) {
					return nil, errors.Errorf("Get ClusterStatus failed")
				}
				continue
			}
			client.lastLeader = current
			return res.Groups, nil
		}
	}
}
