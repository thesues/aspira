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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"google.golang.org/grpc"
)

type ZeroClient struct {
	conns []*grpc.ClientConn //protected by RWMUTEX
	sync.RWMutex
	lastLeader int32 //protected by atomic
	addrs      []string
}

func NewZeroClient(zeroAddrs []string) *ZeroClient {
	return &ZeroClient{
		addrs: zeroAddrs,
	}
}

func (client *ZeroClient) CurrentLeader() string {
	current := atomic.LoadInt32(&client.lastLeader)
	return client.addrs[current]
}

func (client *ZeroClient) Close() {
	client.Lock()
	defer client.Unlock()
	for _, c := range client.conns {
		c.Close()
	}
	client.conns = nil
}

func (client *ZeroClient) Alive() bool {
	client.RLock()
	defer client.RUnlock()
	return len(client.conns) > 0
}

// Connect to Zero's grpc service, if all of connect failed to connnect, return error
func (client *ZeroClient) Connect() error {

	if len(client.addrs) == 0 {
		return errors.Errorf("addr is nil")
	}
	client.Lock()
	client.conns = nil
	errCount := 0
	for _, addr := range client.addrs {
		c, err := grpc.Dial(addr, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
		if err != nil {
			errCount++
		}
		client.conns = append(client.conns, c)
	}
	client.Unlock()
	if errCount == len(client.addrs) {
		return errors.Errorf("all connection failed")
	}
	atomic.StoreInt32(&client.lastLeader, 0)
	return nil
}

// AllocID can query any node of zeros
func (client *ZeroClient) AllocID(count int) ([]uint64, error) {
	client.RLock()
	defer client.RUnlock()

	current := atomic.LoadInt32(&client.lastLeader)
	for loop := 0; loop < len(client.conns); loop++ {
		if client.conns != nil && client.conns[current] != nil {
			c := aspirapb.NewZeroClient(client.conns[current])
			req := aspirapb.ZeroAllocIDRequest{
				Count: 1,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			res, err := c.AllocID(ctx, &req)
			cancel()
			if err != nil {
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(50 * time.Millisecond)
				continue
			}
			atomic.StoreInt32(&client.lastLeader, current)
			return []uint64{res.Start, res.End}, nil
		}

	}
	return nil, errors.Errorf("AllocID failed")
}

//CreateHeartbeatStream is called by store
func (client *ZeroClient) CreateHeartbeatStream() (aspirapb.Zero_StreamHeartbeatClient, context.CancelFunc, error) {
	client.RLock()
	defer client.RUnlock()

	current := atomic.LoadInt32(&client.lastLeader)
	for loop := 0; loop < 3*len(client.conns); loop++ {
		if client.conns != nil && client.conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.conns[current])

			res, err := zc.IsLeader(context.Background(), &aspirapb.ZeroIsLeaderRequest{})
			if err != nil || res.IsLeader == false {
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(50 * time.Millisecond)
				continue
			}

			ctx, cancel := context.WithCancel(context.Background())
			stream, err := zc.StreamHeartbeat(ctx)
			if err != nil {
				cancel()
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(50 * time.Millisecond)
				continue
			}
			//receive error code first

			atomic.StoreInt32(&client.lastLeader, current)
			return stream, cancel, nil
		}
	}
	return nil, nil, errors.Errorf("CreateHeartbeatStream failed")
}

//QueryWorker called by store
func (client *ZeroClient) QueryWorker(id, gid, storeID uint64) (aspirapb.TnxType, error) {
	current := atomic.LoadInt32(&client.lastLeader)
	client.RLock()
	defer client.RUnlock()

	for loop := 0; loop < len(client.conns)*2; loop++ {
		if client.conns != nil && client.conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.conns[current])
			ctx, cancel := context.WithCancel(context.Background())
			req := aspirapb.ZeroQueryWorkerRequest{
				Id:      id,
				Gid:     gid,
				StoreId: storeID,
			}
			res, err := zc.QueryWorker(ctx, &req)
			cancel()
			if err != nil {
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(50 * time.Millisecond)
				continue
			}
			atomic.StoreInt32(&client.lastLeader, current)
			return res.Type, nil
		}
	}
	return aspirapb.TnxType_retry, errors.Errorf("QueryWorker failed")

}

//RegisterSelfAsStore called by store
func (client *ZeroClient) RegisterSelfAsStore(req *aspirapb.ZeroRegistStoreRequest) error {
	current := atomic.LoadInt32(&client.lastLeader)
	client.RLock()
	defer client.RUnlock()

	for loop := 0; loop < 3*len(client.conns); loop++ {
		if client.conns != nil && client.conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.conns[current])
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := zc.RegistStore(ctx, req)
			cancel()
			if err != nil {
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(time.Second)
				continue

			}
			atomic.StoreInt32(&client.lastLeader, current)

			return nil
		}
	}
	return errors.Errorf("RegisterSelfAsStore failed")

}

//ClusterStatus called by aspira_client
func (client *ZeroClient) ClusterStatus() ([]*aspirapb.GroupStatus, error) {
	current := atomic.LoadInt32(&client.lastLeader)
	client.RLock()
	defer client.RUnlock()

	for loop := 0; loop < len(client.conns); loop++ {
		if client.conns != nil && client.conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.conns[current])
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			res, err := zc.ClusterStatus(ctx, &aspirapb.ClusterStatusRequest{})
			cancel()
			if err != nil {
				current = (current + 1) % int32(len(client.conns))
				continue
			}
			atomic.StoreInt32(&client.lastLeader, current)
			return res.Groups, nil
		}
	}
	return nil, errors.Errorf("Get ClusterStatus failed")

}

// AddGroup called by aspira_client
func (client *ZeroClient) AddGroup() (*aspirapb.ZeroAddWorkerGroupResponse, error) {

	current := atomic.LoadInt32(&client.lastLeader)
	client.RLock()
	defer client.RUnlock()

	var errCode []string
	for loop := 0; loop < len(client.conns); loop++ {
		if client.conns != nil && client.conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.conns[current])
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			res, err := zc.AddWorkerGroup(ctx, &aspirapb.ZeroAddWorkerGroupRequest{})
			cancel()
			if err != nil {
				current = (current + 1) % int32(len(client.conns))
				errCode = append(errCode, err.Error())
				continue
			}
			atomic.StoreInt32(&client.lastLeader, current)
			return res, nil
		}
	}
	return nil, errors.Errorf("AddGroup Failed\n%s\n", strings.Join(errCode, "\n"))
}

//Display called by aspira_client
func (client *ZeroClient) Display() (string, error) {
	current := atomic.LoadInt32(&client.lastLeader)
	client.RLock()
	defer client.RUnlock()
	var errCode []string
	for loop := 0; loop < len(client.conns); loop++ {
		if client.conns != nil && client.conns[current] != nil {
			zc := aspirapb.NewZeroClient(client.conns[current])
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			res, err := zc.Display(ctx, &aspirapb.ZeroDisplayRequest{})
			cancel()
			if err != nil {
				current = (current + 1) % int32(len(client.conns))
				errCode = append(errCode, err.Error())
				continue
			}
			atomic.StoreInt32(&client.lastLeader, current)
			return res.Data, nil
		}
	}
	return "", errors.Errorf("Display Failed\n%s\n", strings.Join(errCode, "\n"))
}
