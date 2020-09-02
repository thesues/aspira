package main

import (
	"log"
	"path/filepath"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
)

//block function
func (z *Zero) Serve(config *ZeroConfig) {
	cfg, err := config.GetEmbedConfig()
	if err != nil {
		xlog.Logger.Fatal(err)
	}
	e, err := embed.StartEtcd(cfg)

	xlog.InitLog([]string{filepath.Join(cfg.Dir, cfg.Name+".log")})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	select {
	case <-e.Server.ReadyNotify():
		xlog.Logger.Info("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		xlog.Logger.Info("Server took too long to start!")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.ACUrls[0].String()},
		DialTimeout: time.Second,
	})
	utils.Check(err)

	z.Client = client
	z.Id = uint64(e.Server.ID())
	z.EmbedEted = e
	z.Cfg = config
	z.policy = RandomReplication{}

	z.ServHTTP()
	z.ServGRPC()

	go z.LeaderLoop()

	//register zero as GRPC service
	xlog.Logger.Fatal(<-e.Err())

}

func main() {
	zeroConfig := NewConfig()
	zero := NewZero()
	zero.Serve(zeroConfig)
	//serve(zeroConfig)
}
