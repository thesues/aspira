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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/thesues/aspira/protos/aspirapb"
	zeroclient "github.com/thesues/aspira/zero_client"
)

type ZeroTestSuite struct {
	suite.Suite
	zero *Zero
}

func (suite *ZeroTestSuite) SetupSuite() {
	os.Remove("zero.log")
	zConfig := &ZeroConfig{
		Name:                "zero",
		Dir:                 "zero.db",
		ClientUrls:          "http://127.0.0.1:2379",
		PeerUrls:            "http://127.0.0.1:12380",
		AdvertiseClientUrls: "http://127.0.0.1:12380",
		AdvertisePeerUrls:   "http://127.0.0.1:12380",
		InitialCluster:      "zero=http://127.0.0.1:12380",
		InitialClusterState: "new",
		ClusterToken:        "cluster",
		GrpcUrl:             "0.0.0.0:3403",
	}
	zero := NewZero()
	suite.zero = zero
	go zero.Serve(zConfig)
	time.Sleep(5 * time.Second)
}

func (suite *ZeroTestSuite) TearDownSuite() {
	suite.zero.EmbedEted.Close()
	os.RemoveAll("zero.db")
}

func (suite *ZeroTestSuite) TestHeartbeatStream() {
	//register a store
	c := zeroclient.NewZeroClient()
	err := c.Connect([]string{"127.0.0.1:3403"})
	suite.Nil(err)

	ids, err := c.AllocID(1)
	suite.Nil(err)
	suite.Equal(1, int(ids[1]-ids[0]))
	req := &aspirapb.ZeroRegistStoreRequest{
		Address:    "192.168.0.2:3301",
		StoreId:    ids[0],
		EmtpySlots: 20,
		Name:       "store_two",
	}
	err = c.RegisterSelfAsStore(req)
	stream, cancel, err := c.CreateHeartbeatStream()
	suite.Nil(err)

	//fake a worker 1000, gid 999
	suite.zero.workers[1000] = &workerProgress{
		workerInfo: &aspirapb.ZeroWorkerInfo{
			WorkId:  1000,
			StoreId: ids[0],
			Gid:     999,
		},
		progress: aspirapb.WorkerStatus_Unknown,
	}

	fmt.Printf("store id is %d\n", ids[0])

	suite.zero.gidToWorkerID[999] = []uint64{1000}

	for i := 0; i < 10; i++ {
		hb := aspirapb.ZeroHeartbeatRequest{
			StoreId: ids[0],
			Workers: map[uint64]*aspirapb.WorkerStatus{
				999: {
					Progress: map[uint64]aspirapb.WorkerStatus_ProgressType{
						1000: aspirapb.WorkerStatus_Probe,
					},
					RaftContext: &aspirapb.RaftContext{
						Id:   1000,
						Gid:  999,
						Addr: "localhost",
					},
				},
			},
		}
		err = stream.Send(&hb)
		if err != nil {
			fmt.Printf(err.Error())
		}
		suite.Nil(err)
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	suite.Equal(aspirapb.WorkerStatus_Leader, suite.zero.workers[1000].progress)
	cancel()
}

func (suite *ZeroTestSuite) TestRegistStore() {
	c := zeroclient.NewZeroClient()
	err := c.Connect([]string{"127.0.0.1:3403"})
	suite.Nil(err)

	ids, err := c.AllocID(1)
	suite.Nil(err)
	suite.Equal(1, int(ids[1]-ids[0]))
	req := &aspirapb.ZeroRegistStoreRequest{
		Address:    "192.168.0.1:3301",
		StoreId:    ids[0],
		EmtpySlots: 20,
		Name:       "store_one",
	}

	err = c.RegisterSelfAsStore(req)
	suite.Nil(err)

	suite.zero.RLock()
	defer suite.zero.RUnlock()
	v, ok := suite.zero.stores[ids[0]]
	suite.True(ok)
	suite.Equal(req.Address, v.storeInfo.Address)
	suite.Equal(req.StoreId, v.storeInfo.StoreId)
	suite.Equal(req.Name, v.storeInfo.Name)

}

func TestZeroTestSuite(t *testing.T) {
	suite.Run(t, new(ZeroTestSuite))
}
