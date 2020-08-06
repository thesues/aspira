package main

import (
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

func NewAspiraStore(name, addr, httpAddr string) *AspiraStore {
	as := &AspiraStore{
		addr:     addr,
		httpAddr: httpAddr,
		name:     name,
		workers:  make(map[uint64]*AspiraWorker),
		//stopper:  util.NewStopper(),
	}

	return as
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
				s.workers[gid] = worker
				go worker.InitAndStart("")
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

func (s *AspiraStore) startNewWorker(id, gid uint64, addr, joinAddress string) error {
	s.Lock()
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
	s.workers[gid] = x
	s.Unlock()

	go x.InitAndStart(joinAddress)
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
	xlog.InitLog(logOutputs)

	as := NewAspiraStore(*name, *addr, *httpAddr)
	as.ServGRPC()
	as.ServHTTP()

	//register to zeros
	if err := as.RegisterStore(); err != nil {
		xlog.Logger.Warnf("RegisterStore failed err is %+v", err.Error())
	}

	as.LoadAndRun()

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
