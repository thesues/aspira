package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/utils"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
)

func putAspira(c *cli.Context) error {
	gid := c.Uint64("gid")
	cluster := c.String("cluster")
	fileName := c.Args().First()

	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	conn, err := grpc.Dial(cluster, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := aspirapb.NewStoreClient(conn)

	res, err := client.Put(context.Background(), &aspirapb.PutRequest{
		Gid:     gid,
		Payload: &aspirapb.Payload{Data: data},
	})
	if err != nil {
		return err
	}
	fmt.Printf("oid is %d", res.Oid)
	return nil
}

func sputAspira(c *cli.Context) error {
	gid := c.Uint64("gid")
	cluster := c.String("cluster")
	fileName := c.Args().First()

	conn, err := grpc.Dial(cluster, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())

	if err != nil {
		return err
	}

	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	client := aspirapb.NewStoreClient(conn)

	putStream, err := client.PutStream(context.Background())

	if err != nil {
		return err
	}

	req := aspirapb.PutStreamRequest{
		Data: &aspirapb.PutStreamRequest_Gid{
			Gid: gid,
		},
	}

	err = putStream.Send(&req)
	utils.Check(err)
	buf := make([]byte, 64<<10)
	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			panic("read file err")
		}
		if n == 0 {
			break
		}

		req := aspirapb.PutStreamRequest{
			Data: &aspirapb.PutStreamRequest_Payload{
				Payload: &aspirapb.Payload{
					Data: buf[:n],
				},
			},
		}
		err = putStream.Send(&req)
		utils.Check(err)
	}
	res, err := putStream.CloseAndRecv()
	utils.Check(err)
	fmt.Printf("%+v", res)
	return nil
}

/*
func addWorker(c *cli.Context) error {
	gid := c.Uint64("gid")
	id := c.Uint64("id")
	localStore := c.String("localStore")
	remoteCluster := c.String("remoteCluster")
	conn, err := grpc.Dial(localStore, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := aspirapb.NewStoreClient(conn)
	//block until raft group started
	req := aspirapb.AddWorkerRequest{
		Gid:         gid,
		Id:          id,
		JoinCluster: remoteCluster,
	}
	_, err = client.AddWorker(context.Background(), &req)
	if err != nil {
		return err
	}
	fmt.Printf("Success\n")
	return nil
}
*/

func addGroup(c *cli.Context) error {
	cluster := c.String("cluster")
	conn, err := grpc.Dial(cluster, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := aspirapb.NewZeroClient(conn)

	if _, err = client.AddWorkerGroup(context.Background(), &aspirapb.ZeroAddWorkerGroupRequest{}); err != nil {
		return err
	}
	return nil
}

func getAspira(c *cli.Context) error {

	gid := c.Uint64("gid")
	oid := c.Uint64("oid")
	cluster := c.String("cluster")
	fileName := c.Args().First()

	conn, err := grpc.Dial(cluster, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := aspirapb.NewStoreClient(conn)
	getStream, err := client.Get(context.Background(), &aspirapb.GetRequest{
		Gid: gid,
		Oid: oid,
	})
	if err != nil {
		return err
	}

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		payload, err := getStream.Recv()
		if err != nil && err != io.EOF {
			return err
		}
		if payload != nil {
			if _, err = f.Write(payload.Data); err != nil {
				return err
			}

		} else {
			break
		}
	}
	return nil
}

func main() {

	app := cli.NewApp()
	app.Name = "admin"
	app.Usage = "admin subcommand"
	app.Commands = []*cli.Command{
		{
			Name:  "put",
			Usage: "put --cluster <path> --gid <gid>  <file>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3301"},
				&cli.Uint64Flag{Name: "gid"},
			},
			Action: putAspira,
		},
		{
			Name:  "sput",
			Usage: "sput --cluster <path> --gid <gid> <file>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3301"},
				&cli.Uint64Flag{Name: "gid"},
			},
			Action: sputAspira,
		},
		{
			Name:  "get",
			Usage: "get --cluster <path> --gid <gid> --oid <oid> <file>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3301"},
				&cli.Uint64Flag{Name: "gid"},
				&cli.Uint64Flag{Name: "oid"},
			},
			Action: getAspira,
		},
		{
			Name:  "zero_add_group",
			Usage: "zero_add_group --cluster <path>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401"},
			},
			Action: addGroup,
		},
		/*
			{
				Name:  "add_worker",
				Usage: "add_worker --localStore <local> --remoteCluster <remote> --gid <gid> --id <id>",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "localStore", Required: true},
					&cli.Uint64Flag{Name: "gid", Required: true},
					&cli.Uint64Flag{Name: "id", Required: true},
					&cli.StringFlag{Name: "remoteCluster", Required: false},
				},
				Action: addWorker,
			},
		*/

	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}

}
