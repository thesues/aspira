package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	"google.golang.org/grpc"
)

type StoreTestSuite struct {
	suite.Suite
	conns  []*grpc.ClientConn
	stores []*AspiraStore
}

func (suite *StoreTestSuite) TearDownSuite() {
	for _, s := range suite.stores {
		s.Stop()
	}
	for _, dir := range []string{"db1", "db2", "db3"} {
		os.RemoveAll(dir)
	}
}

func (suite *StoreTestSuite) SetupSuite() {

	fmt.Println("SETUP SUITE")
	xlog.InitLog([]string{"test.log"})
	for _, dir := range []string{"db1", "db2", "db3"} {
		os.Mkdir(dir, 0755)
		//defer os.RemoveAll(dir)
	}
	s1 := NewAspiraStore("db1", ":3301", ":8081")
	s2 := NewAspiraStore("db2", ":3302", ":8082")
	s3 := NewAspiraStore("db3", ":3303", ":8083")
	ss := []*AspiraStore{s1, s2, s3}
	for k := range ss {
		ss[k].ServGRPC()
		ss[k].ServHTTP()
	}
	conn1, err := grpc.Dial("127.0.0.1:3301", grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
	utils.Check(err)
	conn2, err := grpc.Dial("127.0.0.1:3302", grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
	utils.Check(err)
	conn3, err := grpc.Dial("127.0.0.1:3303", grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
	utils.Check(err)

	c1 := aspirapb.NewStoreClient(conn1)
	c2 := aspirapb.NewStoreClient(conn2)
	c3 := aspirapb.NewStoreClient(conn3)
	//create a single node raft group at store1
	c1.AddWorker(context.Background(), &aspirapb.AddWorkerRequest{Gid: 200, Id: 1})
	time.Sleep(2 * time.Second)
	c2.AddWorker(context.Background(), &aspirapb.AddWorkerRequest{Gid: 200, Id: 2, JoinCluster: "127.0.0.1:3301"})
	c3.AddWorker(context.Background(), &aspirapb.AddWorkerRequest{Gid: 200, Id: 3, JoinCluster: "127.0.0.1:3301"})
	time.Sleep(10 * time.Second)
	suite.conns = []*grpc.ClientConn{conn1, conn2, conn3}
	suite.stores = []*AspiraStore{s1, s2, s3}

}

func (suite *StoreTestSuite) TestStorePutStreamGet() {

	f, err := os.Open("store_test.go")
	assert.Nil(suite.T(), err)
	defer f.Close()
	data, err := ioutil.ReadAll(f)

	//Write to c2
	c2 := aspirapb.NewStoreClient(suite.conns[1])

	putStream, err := c2.PutStream(context.Background())
	assert.Nil(suite.T(), err)

	req := aspirapb.PutStreamRequest{
		Data: &aspirapb.PutStreamRequest_Gid{
			Gid: 200,
		},
	}
	err = putStream.Send(&req)
	assert.Nil(suite.T(), err)
	n := 0
	for {
		size := utils.Min(512<<10, len(data)-n)
		if size == 0 {
			break
		}

		req := aspirapb.PutStreamRequest{
			Data: &aspirapb.PutStreamRequest_Payload{
				Payload: &aspirapb.Payload{
					Data: data[n : n+size],
				},
			},
		}
		err = putStream.Send(&req)
		assert.Nil(suite.T(), err)

		n += size
	}

	res, err := putStream.CloseAndRecv()
	assert.Nil(suite.T(), err)

	//read from c2
	r := suite.readData(200, res.Oid, suite.conns[1])
	assert.Equal(suite.T(), data, r)

}

func (suite *StoreTestSuite) readData(gid, oid uint64, conn *grpc.ClientConn) []byte {
	c2 := aspirapb.NewStoreClient(suite.conns[1])
	getStream, err := c2.Get(context.Background(), &aspirapb.GetRequest{
		Gid: gid,
		Oid: oid,
	})
	assert.Nil(suite.T(), err)
	result := new(bytes.Buffer)
	for {
		payload, err := getStream.Recv()
		if err != nil && err != io.EOF {
			assert.Error(suite.T(), err)
		}
		if payload != nil {
			result.Write(payload.Data)
		} else {
			break
		}
	}
	return result.Bytes()

}

func (suite *StoreTestSuite) TestPutGet() {

	f, err := os.Open("store_test.go")
	assert.Nil(suite.T(), err)
	defer f.Close()
	data, err := ioutil.ReadAll(f)

	//Write to c1
	c1 := aspirapb.NewStoreClient(suite.conns[0])
	response, err := c1.Put(context.Background(), &aspirapb.PutRequest{
		Gid:     200,
		Payload: &aspirapb.Payload{Data: data},
	})
	assert.Nil(suite.T(), err)
	oid := response.Oid

	//Read from c2
	r := suite.readData(200, oid, suite.conns[1])
	assert.Equal(suite.T(), data, r)

}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}
