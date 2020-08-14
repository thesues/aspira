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
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"contrib.go.opencensus.io/exporter/jaeger"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/conn"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/xlog"
	zeroclient "github.com/thesues/aspira/zero_client"
	otrace "go.opencensus.io/trace"
	"google.golang.org/grpc"
)

type AspiraStore struct {
	sync.RWMutex
	addr       string //grpc address
	httpAddr   string //http address
	name       string
	storeId    uint64
	workers    map[uint64]*AspiraWorker
	grpcServer *grpc.Server //internal comms, raftServer is registered on grpcServer
	httpServer *http.Server
}

func NewAspiraStore(name, addr, httpAddr string) (*AspiraStore, error) {

	if strings.HasPrefix(addr, ":") {
		return nil, errors.Errorf("must give full address, your addr is %s", addr)
	}
	as := &AspiraStore{
		addr:     addr,
		httpAddr: httpAddr,
		name:     name,
		workers:  make(map[uint64]*AspiraWorker),
		//stopper:  util.NewStopper(),
	}

	return as, nil
}

func (as *AspiraStore) RegisterStore() error {
	xlog.Logger.Infof("RegisterStore")
	storeIdPath := fmt.Sprintf("%s/store_id", as.name)
	idString, err := ioutil.ReadFile(storeIdPath)
	if err == nil {
		id, err := strconv.ParseUint(string(idString), 10, 64)
		if err != nil {
			xlog.Logger.Fatalf("can not read ioString")
		}
		as.storeId = id
		return nil
	}

	zeroClient := zeroclient.NewZeroClient()
	if err = zeroClient.Connect([]string{"127.0.0.1:3401", "127.0.0.1:3402", "127.0.0.1:3403"}); err != nil {
		return err
	}
	//TODO loop for a while?
	ids, err := zeroClient.AllocID(1)
	if err != nil {
		return err
	}
	req := aspirapb.ZeroRegistStoreRequest{
		Address:    as.addr,
		StoreId:    ids[0],
		EmtpySlots: 20,
		Name:       as.name,
	}

	if err = zeroClient.RegisterSelfAsStore(&req); err != nil {
		return err
	}

	as.storeId = ids[0]

	ioutil.WriteFile(storeIdPath, []byte(fmt.Sprintf("%d", as.storeId)), 0644)
	xlog.Logger.Infof("success to register to zero")
	return nil
}

func (as *AspiraStore) Stop() {
	for _, w := range as.workers {
		w.Stop()
	}
	as.grpcServer.Stop()
	as.httpServer.Close()
}

// Load from disk
func (s *AspiraStore) LoadAndRun() {
	xlog.Logger.Info("LoadAndRun")
	baseDirectory := s.name
	workDirs, err := ioutil.ReadDir(baseDirectory)
	if err != nil {
		xlog.Logger.Warnf("can not find any existing workers")
		return
	}
	for _, workDir := range workDirs {
		xlog.Logger.Info(workDir.Name())
		if workDir.IsDir() && strings.HasPrefix(workDir.Name(), "worker") {
			//example: worker_gid_id
			parts := strings.Split(workDir.Name(), "_")
			if len(parts) == 3 {
				gid, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					continue
				}
				id, err := strconv.ParseUint(parts[2], 10, 64)
				if err != nil {
					continue
				}
				path := fmt.Sprintf("%s/%s/%d.lusf", baseDirectory, workDir.Name(), id)
				xlog.Logger.Infof("start gid: %d, id: %d, path: %s", gid, id, path)
				worker, err := NewAspiraWorker(id, gid, s.addr, path)
				if err != nil {
					xlog.Logger.Warnf("can not install worker, %v", err)
					continue
				}
				go func() {
					if err = worker.InitAndStart("", ""); err != nil {
						xlog.Logger.Warnf(err.Error())
						return
					}
					s.Lock()
					s.workers[gid] = worker
					s.Unlock()

				}()
			}
		}
	}
}

//GetWorker, thread-safe
func (s *AspiraStore) GetWorker(gid uint64) *AspiraWorker {
	s.RLock()
	defer s.RUnlock()
	n, ok := s.workers[gid]
	if !ok {
		return nil
	}
	return n
}

//GetNode, thread-safe
func (s *AspiraStore) GetNode(gid uint64) *conn.Node {
	worker := s.GetWorker(gid)
	if worker == nil {
		return nil
	}
	return worker.Node
}

//ServGRPC start GRPC server
func (as *AspiraStore) ServGRPC() {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(33<<20),
		grpc.MaxSendMsgSize(33<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	raftServer := NewRaftServer(as)

	aspirapb.RegisterRaftServer(grpcServer, raftServer)
	aspirapb.RegisterStoreServer(grpcServer, as)

	listener, err := net.Listen("tcp", as.addr)
	if err != nil {
		xlog.Logger.Fatal(err.Error())
	}

	as.grpcServer = grpcServer
	go func() {
		defer xlog.Logger.Infof("GRPC server return")
		grpcServer.Serve(listener)
	}()

}

//getHeartbeatStream must return a valid hearbeat stream
func (s *AspiraStore) getHeartbeatStream(zeroAddrs []string) (aspirapb.Zero_StreamHeartbeatClient, context.CancelFunc) {

Connect:
	c := zeroclient.NewZeroClient()
	for {
		if err := c.Connect(zeroAddrs); err != nil {
			xlog.Logger.Warnf("can not connect to any of [%+v]", zeroAddrs)
			time.Sleep(50 * time.Millisecond)
		}
		break
	}

	stream, cancel, err := c.CreateHeartbeatStream()
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		c.Close()
		goto Connect
	}
	return stream, cancel
}

//StartHeartbeat collect local worker info and report to zero
func (s *AspiraStore) StartHeartbeat(zeroAddrs []string) {

	ticker := time.NewTicker(5 * time.Second)

	var stream aspirapb.Zero_StreamHeartbeatClient
	var cancel context.CancelFunc
	var err error

	stream, cancel = s.getHeartbeatStream(zeroAddrs)
	for {
		select {
		case <-ticker.C:
			req := aspirapb.ZeroHeartbeatRequest{
				StoreId: s.storeId,
				Workers: make(map[uint64]*aspirapb.WorkerStatus),
			}

			s.RLock()
			for gid, worker := range s.workers {
				req.Workers[gid] = worker.WorkerStatus()
			}
			s.RUnlock()

			err = stream.Send(&req)
			if err != nil {
				cancel()
				xlog.Logger.Warnf("heatbeat stream returns %v, try again", err)
				stream, cancel = s.getHeartbeatStream(zeroAddrs)
			} else {
				xlog.Logger.Debugf("Reported to zero %v", req)
			}
		}
	}
}

//startNewWorker non-block
func (s *AspiraStore) startNewWorker(id, gid uint64, addr, joinAddress string, initialCluster string) error {

	if _, ok := s.workers[gid]; ok {
		s.Unlock()
		return errors.Errorf("can not put the same group[%d] into the same store", gid)
	}
	path := fmt.Sprintf("%s/worker_%d_%d/%d.lusf", s.name, gid, id, id)
	if err := os.Mkdir(filepath.Dir(path), 0755); err != nil {
		return nil
	}
	x, err := NewAspiraWorker(id, gid, addr, path)
	if err != nil {
		s.Unlock()
		return err
	}
	go func() {
		if err = x.InitAndStart(joinAddress, initialCluster); err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		s.Lock()
		s.workers[gid] = x
		s.Unlock()
	}()
	return nil
}

func (s *AspiraStore) StopWorker(id, gid uint64) error {
	return nil
}

var (
	hasJaeger   = flag.Bool("jaeger", false, "connect to jaeger")
	strict      = flag.Bool("strict", false, "strict sync every entry")
	addr        = flag.String("addr", "", "")
	httpAddr    = flag.String("http_addr", "", "")
	name        = flag.String("name", "default", "")
	logToStdout = flag.Bool("stdout", false, "if log to stdout")
)

func main() {
	//1 => 127.0.0.1:3301
	//2 => 127.0.0.1:3302

	flag.Parse()
	if *hasJaeger {
		je, _ := jaeger.NewExporter(jaeger.Options{
			Endpoint:    "http://localhost:14268",
			ServiceName: "aspira",
		})
		otrace.RegisterExporter(je)
		otrace.ApplyConfig(otrace.Config{DefaultSampler: otrace.AlwaysSample()})
	}
	logOutputs := []string{*name + ".log"}
	if *logToStdout {
		logOutputs = append(logOutputs, os.Stdout.Name())
	}

	//xlog.InitLog(logOutputs)
	xlog.InitLog(nil)

	as, err := NewAspiraStore(*name, *addr, *httpAddr)
	if err != nil {
		panic(err.Error())
	}
	as.ServGRPC()
	as.ServHTTP()

	//register to zeros
	if err := as.RegisterStore(); err != nil {
		xlog.Logger.Fatal("RegisterStore failed err is %+v", err.Error())
	}

	as.LoadAndRun()

	go as.StartHeartbeat([]string{"127.0.0.1:3401", "127.0.0.1:3402", "127.0.0.1:3403"})

	xlog.Logger.Infof("store is ready!")
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGUSR1)
	for {
		select {
		case <-sc:
			as.Stop()
			return
		}
	}

}
