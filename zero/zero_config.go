package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/coreos/etcd/embed"
	_ "github.com/thesues/aspira/xlog"
	"github.com/urfave/cli/v2"
)

type ZeroConfig struct {
	Name                string
	Dir                 string
	ClientUrls          string
	PeerUrls            string
	AdvertisePeerUrls   string
	AdvertiseClientUrls string
	InitialCluster      string
	InitialClusterState string
	ClusterToken        string
	GrpcUrl             string
}

func parseUrls(s string) (ret []url.URL, err error) {
	for _, urlString := range strings.Split(s, ",") {
		u, err := url.Parse(urlString)
		if err != nil {
			return nil, err
		}
		ret = append(ret, *u)
	}
	return ret, nil
}

func (config *ZeroConfig) GetEmbedConfig() (*embed.Config, error) {
	embedConfig := embed.NewConfig()
	embedConfig.Name = config.Name
	embedConfig.Dir = fmt.Sprintf("%s.db", config.Name)
	embedConfig.ClusterState = config.InitialClusterState
	embedConfig.InitialClusterToken = config.ClusterToken
	embedConfig.InitialCluster = config.InitialCluster

	lcurls, err := parseUrls(config.ClientUrls)
	if err != nil {
		return nil, err
	}
	embedConfig.LCUrls = lcurls

	acurls, err := parseUrls(config.AdvertiseClientUrls)
	if err != nil {
		return nil, err
	}
	embedConfig.ACUrls = acurls

	lpurls, err := parseUrls(config.PeerUrls)
	if err != nil {
		return nil, err
	}
	embedConfig.LPUrls = lpurls

	apurls, err := parseUrls(config.AdvertisePeerUrls)
	if err != nil {
		return nil, err
	}
	embedConfig.APUrls = apurls

	return embedConfig, nil
}

func NewConfig() *ZeroConfig {
	var config ZeroConfig
	var err error
	app := &cli.App{
		HelpName: "zero",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "name",
				Usage:       "human-readable name for etcd",
				Destination: &config.Name,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "listen-client-urls",
				Usage:       "client url listen on",
				Destination: &config.ClientUrls,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "advertise-client-urls",
				Usage:       "advertise url for client traffic",
				Destination: &config.AdvertiseClientUrls,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "listen-peer-urls",
				Usage:       "",
				Destination: &config.PeerUrls,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "advertise-peer-urls",
				Usage:       "",
				Destination: &config.AdvertisePeerUrls,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "initial-cluster",
				Usage:       "initial cluster configuration for bootstrapping",
				Destination: &config.InitialCluster,
			},
			&cli.StringFlag{
				Name:        "initial-cluster-state",
				Usage:       "",
				Destination: &config.InitialClusterState,
			},
			&cli.StringFlag{
				Name:        "initial-cluster-token",
				Usage:       "",
				Destination: &config.ClusterToken,
			},
			&cli.StringFlag{
				Name:        "listen-grpc",
				Destination: &config.GrpcUrl,
				Required:    true,
			},
		},
	}
	if err = app.Run(os.Args); err != nil {
		fmt.Println(app.Usage)
		panic(fmt.Sprintf("%v", err))
	}
	fmt.Printf("%+v\n", config)

	//xlog.Logger.Infof("%+v", config)
	return &config
}
