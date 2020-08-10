package main

import (
	"fmt"
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

	for i := 0; i < 10; i++ {
		hb := aspirapb.ZeroHeartbeatRequest{
			StoreId: ids[0],
			Workers: nil,
		}
		err = stream.Send(&hb)
		if err != nil {
			fmt.Printf(err.Error())
		}
		suite.Nil(err)
	}
	time.Sleep(time.Second)
	cancel()

	suite.zero.reLock.Lock()
	suite.Equal(ids[0], suite.zero.clusterStore[ids[0]].StoreId)
	suite.zero.reLock.Unlock()
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

	suite.zero.reLock.Lock()
	defer suite.zero.reLock.Unlock()
	v, ok := suite.zero.clusterStore[ids[0]]
	suite.True(ok)
	suite.Equal(req.Address, v.Address)
	suite.Equal(req.StoreId, v.StoreId)
	suite.Equal(req.Name, v.Name)

}

func TestZeroTestSuite(t *testing.T) {
	suite.Run(t, new(ZeroTestSuite))
}
