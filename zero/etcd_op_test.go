package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/thesues/aspira/protos/aspirapb"
)

type EtcdUtilTestSuite struct {
	suite.Suite
	zero *Zero
}

func (suite *EtcdUtilTestSuite) SetupSuite() {
	os.Remove("etcd.log")
	zConfig := &ZeroConfig{
		Name:                "etcd",
		Dir:                 "etcd.db",
		ClientUrls:          "http://127.0.0.1:2379",
		PeerUrls:            "http://127.0.0.1:12380",
		AdvertiseClientUrls: "http://127.0.0.1:12380",
		AdvertisePeerUrls:   "http://127.0.0.1:12380",
		InitialCluster:      "etcd=http://127.0.0.1:12380",
		InitialClusterState: "new",
		ClusterToken:        "cluster",
		GrpcUrl:             "0.0.0.0:3000",
	}
	zero := NewZero()
	suite.zero = zero
	go zero.Serve(zConfig)
	time.Sleep(5 * time.Second)
}

func (suite *EtcdUtilTestSuite) TearDownSuite() {
	suite.zero.EmbedEted.Close()
	os.RemoveAll("etcd.db")
}

func (suite *EtcdUtilTestSuite) TestSetGetKV() {
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val%d", i)
		EtcdSetKV(suite.zero.Client, key, []byte(val))
	}
	data, err := EtcdGetKV(suite.zero.Client, "key_50")
	suite.Nil(err)
	suite.Equal("val50", string(data))
}

func (suite *EtcdUtilTestSuite) TestLoadStoreWorker() {
	c := suite.zero.Client
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("store_%d", i)
		w := &aspirapb.ZeroStoreInfo{
			Address: "local",
			StoreId: uint64(i),
			Slots:   10,
			Name:    "",
		}
		val, err := w.Marshal()
		suite.Nil(err)
		EtcdSetKV(c, key, val)
	}
	stores, err := LoadStores(c)
	suite.Nil(err)

	suite.Equal(uint64(99), stores[99].storeInfo.StoreId)

}

func TestEtcdUtil(t *testing.T) {
	suite.Run(t, new(EtcdUtilTestSuite))
}
