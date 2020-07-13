package main

import (
	"log"

	"github.com/coreos/etcd/clientv3"
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"

	"time"

	"github.com/coreos/etcd/embed"
)

func serve(config *ZeroConfig) {
	cfg, err := config.GetEmbedConfig()
	if err != nil {
		xlog.Logger.Fatal(err)
	}
	e, err := embed.StartEtcd(cfg)
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

	zero := &Zero{
		Client:    client,
		Id:        uint64(e.Server.ID()),
		EmbedEted: e,
		Cfg:       config,
	}
	go zero.Report()
	zero.ServGRPC()

	//register zero as GRPC service
	xlog.Logger.Fatal(<-e.Err())
}

func main() {
	zeroConfig := NewConfig()
	serve(zeroConfig)
}
