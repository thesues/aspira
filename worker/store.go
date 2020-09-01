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
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	zeroclient "github.com/thesues/aspira/zero_client"
	otrace "go.opencensus.io/trace"
	"google.golang.org/grpc"
)

type Gid uint64
type Id uint64

type AspiraStore struct {
	sync.RWMutex
	addr     string //grpc address
	httpAddr string //http address
	name     string
	storeId  uint64
	workers  map[Gid]*AspiraWorker
	//prepareTasks map[Id]*aspirapb.AddWorkerRequest
	grpcServer *grpc.Server //internal comms, raftServer is registered on grpcServer
	httpServer *http.Server
	zClient    *zeroclient.ZeroClient
	zeroAddrs  []string
}

func createDirectoryIfNotExist(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0755)
	}
}

func NewAspiraStore(name, addr, httpAddr string, zeroAddrs []string) (*AspiraStore, error) {

	if strings.HasPrefix(addr, ":") {
		return nil, errors.Errorf("must give full address, your addr is %s", addr)
	}
	as := &AspiraStore{
		addr:      addr,
		httpAddr:  httpAddr,
		name:      name,
		workers:   make(map[Gid]*AspiraWorker),
		zClient:   zeroclient.NewZeroClient(),
		zeroAddrs: zeroAddrs,
	}
	createDirectoryIfNotExist(fmt.Sprintf("%s/prepare", as.name))
	return as, nil
}

func (as *AspiraStore) RegisterStore() error {
	xlog.Logger.Infof("RegisterStore")
	storeIDPath := fmt.Sprintf("%s/store_id", as.name)
	idString, err := ioutil.ReadFile(storeIDPath)
	if err == nil {
		id, err := strconv.ParseUint(string(idString), 10, 64)
		if err != nil {
			xlog.Logger.Fatalf("can not read ioString")
		}
		as.storeId = id
		return nil
	}

	//TODO loop for a while?
	ids, err := as.zClient.AllocID(1)
	if err != nil {
		return err
	}
	req := aspirapb.ZeroRegistStoreRequest{
		Address:    as.addr,
		StoreId:    ids[0],
		EmtpySlots: 20,
		Name:       as.name,
	}

	if err = as.zClient.RegisterSelfAsStore(&req); err != nil {
		return err
	}

	as.storeId = ids[0]

	ioutil.WriteFile(storeIDPath, []byte(fmt.Sprintf("%d", as.storeId)), 0644)
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
					s.SetWorker(gid, worker)
				}()
			}
		}
	}
}

func (s *AspiraStore) SetWorker(gid uint64, w *AspiraWorker) {
	s.Lock()
	defer s.Unlock()
	s.workers[Gid(gid)] = w
}

//GetWorker, thread-safe
func (s *AspiraStore) GetWorker(gid uint64) *AspiraWorker {
	s.RLock()
	defer s.RUnlock()
	n, ok := s.workers[Gid(gid)]
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
/*
func (s *AspiraStore) getHeartbeatStream() (aspirapb.Zero_StreamHeartbeatClient, context.CancelFunc) {
	stream, cancel, err := s.zClient.CreateHeartbeatStream()
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		for {
			stream, cancel, err = s.zClient.CreateHeartbeatStream()
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				time.Sleep(1 * time.Second)
			}
			break
		}

	}
	return stream, cancel
}
*/

func (as *AspiraStore) StartCheckPrepare() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			prepares, err := as.loadPrepares()
			if err != nil {
				xlog.Logger.Error(err.Error)
				continue
			}

			//addWorker
			//or delete prepare
			for _, req := range prepares {
				res, err := as.zClient.QueryWorker(req.Id, req.Gid, as.storeId)
				if err != nil || res == aspirapb.TnxType_retry {
					continue
				}
				switch res {
				case aspirapb.TnxType_commit:
					err = as.startNewWorker(req.Id, req.Gid, as.addr, req.JoinCluster, req.InitialCluster)
					if err != nil {
						xlog.Logger.Warnf("can not start NewWorker")
					}
				case aspirapb.TnxType_abort:
					as.Lock()
					//delete the prepare file
					fileName := preparePath(as.name, req.Id)
					err = os.Remove(fileName)
					if err != nil {
						xlog.Logger.Warnf("get above msg, removing file %s failed, [%v]", fileName, err)
					}
					as.Unlock()
				default:
					xlog.Logger.Warnf("unexpected type %v", res)
					break
				}

			}
		}
	}
}

//StartHeartbeat collect local worker info and report to zero
func (as *AspiraStore) StartHeartbeat() {

	ticker := time.NewTicker(5 * time.Second)

	var stream aspirapb.Zero_StreamHeartbeatClient
	var cancel context.CancelFunc
	var err error

	stream, cancel, err = as.zClient.CreateHeartbeatStream()

	for {
		select {
		case <-ticker.C:
			req := aspirapb.ZeroHeartbeatRequest{
				StoreId: as.storeId,
				Workers: make(map[uint64]*aspirapb.WorkerStatus),
			}

			as.RLock()
			for gid, worker := range as.workers {
				req.Workers[uint64(gid)] = worker.WorkerStatus()
			}
			as.RUnlock()

			if stream == nil {
				xlog.Logger.Errorf("can not send hb to zero")
				stream, cancel, _ = as.zClient.CreateHeartbeatStream()
				continue
			}
			err = stream.Send(&req)
			if err != nil {
				cancel()
				stream, cancel, _ = as.zClient.CreateHeartbeatStream()
				continue
			} else {
				xlog.Logger.Debugf("Reported to zero %v", req)
			}
		}
	}
}

func (as *AspiraStore) removePrepare(id uint64) {
	if _, err := os.Stat(preparePath(as.name, id)); err == nil {
		os.Remove(preparePath(as.name, id))
	}
}

//startNewWorker non-block
func (as *AspiraStore) startNewWorker(id, gid uint64, addr, joinAddress string, initialCluster string) error {

	//if it has prepare task file, delete the file.

	w := as.GetWorker(gid)
	if w != nil {
		as.removePrepare(id)
		return errors.Errorf("can not put the same group[%d] into the same store", gid)
	}

	as.Lock()
	path := fmt.Sprintf("%s/worker_%d_%d/%d.lusf", as.name, gid, id, id)
	if err := os.Mkdir(filepath.Dir(path), 0755); err != nil {
		as.Unlock()
		return nil
	}
	x, err := NewAspiraWorker(id, gid, addr, path)
	if err != nil {
		as.Unlock()
		return err
	}

	as.removePrepare(id)

	as.Unlock()
	go func() {
		if err = x.InitAndStart(joinAddress, initialCluster); err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		as.SetWorker(gid, x)
	}()
	return nil
}

func (as *AspiraStore) StopWorker(id, gid uint64) error {
	return nil
}

func (as *AspiraStore) savePrepare(addWorkerRequest *aspirapb.AddWorkerRequest) error {
	path := fmt.Sprintf("%s/prepare/prepare_%d", as.name, addWorkerRequest.Id)
	data, err := addWorkerRequest.Marshal()
	utils.Check(err)

	file, err := os.OpenFile(path, os.O_SYNC|os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		xlog.Logger.Infof("can not create file %s", path)
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	return err

}

func preparePath(name string, id uint64) string {
	return fmt.Sprintf("%s/prepare/prepare_%d", name, id)
}
func (as *AspiraStore) loadPrepares() (map[Id]*aspirapb.AddWorkerRequest, error) {
	prepareTasks := make(map[Id]*aspirapb.AddWorkerRequest)

	as.Lock()
	defer as.Unlock()
	pattern := fmt.Sprintf("%s/prepare/prepare_*", as.name)
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return prepareTasks, err
	}

	for _, path := range paths {
		fileInfo, err := os.Stat(path)
		if err != nil {
			continue
		}
		if time.Now().Sub(fileInfo.ModTime()) < 30*time.Second { //ignore the latest prepare File
			continue
		}
		data, err := ioutil.ReadFile(path)
		utils.Check(err)
		var req aspirapb.AddWorkerRequest
		err = req.Unmarshal(data)
		utils.Check(err)
		prepareTasks[Id(req.Id)] = &req
	}
	return prepareTasks, nil
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

	xlog.InitLog(logOutputs)

	as, err := NewAspiraStore(*name, *addr, *httpAddr, []string{"127.0.0.1:3401", "127.0.0.1:3402", "127.0.0.1:3403"})
	if err != nil {
		panic(err.Error())
	}
	if as.zClient.Connect(as.zeroAddrs) != nil {
		xlog.Logger.Warnf("can not connected to zeros")
	}
	as.ServGRPC()
	as.ServHTTP()

	//register to zeros
	if err := as.RegisterStore(); err != nil {
		xlog.Logger.Fatal("RegisterStore failed err is %+v", err.Error())
	}

	as.LoadAndRun()

	go as.StartHeartbeat()
	go as.StartCheckPrepare()

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
