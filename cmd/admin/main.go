package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	aspiraclient "github.com/thesues/aspira/aspira_client"
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

func streamDateToLocal(writer io.Writer, conn *grpc.ClientConn, gid, oid uint64) error {

	client := aspirapb.NewStoreClient(conn)
	getStream, err := client.Get(context.Background(), &aspirapb.GetRequest{
		Gid: gid,
		Oid: oid,
	})
	if err != nil {
		return err
	}

	for {
		payload, err := getStream.Recv()
		if err != nil && err != io.EOF {
			return err
		}
		if payload != nil {
			if _, err = writer.Write(payload.Data); err != nil {
				return err
			}

		} else {
			break
		}
	}
	return nil
}
func streamDataToRemote(reader io.Reader, conn *grpc.ClientConn, gid uint64) (uint64, uint64, error) {
	client := aspirapb.NewStoreClient(conn)

	putStream, err := client.PutStream(context.Background())

	if err != nil {
		return 0, 0, err
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
		n, err := reader.Read(buf)
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
	return res.Gid, res.Oid, err
}

func sputAspiraWorker(c *cli.Context) error {
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

	gid, oid, err := streamDataToRemote(f, conn, gid)

	fmt.Printf("gid :%d, oid : %d, [%v]", gid, oid, err)
	return err
}

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
	if fileName == "" {
		return errors.Errorf("fileName is required")
	}
	conn, err := grpc.Dial(cluster, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	return streamDateToLocal(f, conn, gid, oid)
}

func sgetFile(c *cli.Context) (err error) {
	gid := c.Uint64("gid")
	oid := c.Uint64("oid")
	code := c.String("code")
	fileName := c.Args().First()
	zeroAddrs := strings.Split(c.String("cluster"), ",")
	//valid the input
	if code != "" {
		gid, oid, err = decodeGidOid(code)
		if err != nil {
			return err
		}
	} else if gid == 0 && oid == 0 {
		return errors.Errorf("gid/oid or code must be set")
	}

	if fileName == "" {
		return errors.Errorf("fileName is required")
	}

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	client := aspiraclient.NewAspiraClient(zeroAddrs)
	if err = client.Connect(); err != nil {
		return err
	}
	err = client.PullStream(f, gid, oid)
	return err
}

func sputFile(c *cli.Context) error {
	zeroAddrs := strings.Split(c.String("cluster"), ",")
	fileName := c.Args().First()

	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	client := aspiraclient.NewAspiraClient(zeroAddrs)

	if err := client.Connect(); err != nil {
		return err
	}

	gid, oid, err := client.PushStream(f)

	if err == nil {
		fmt.Printf("gid :%d, oid : %d, code : %s\n", gid, oid, encodeGidOid(gid, oid))
		return nil
	}
	return err

}

func decodeGidOid(s string) (gid uint64, oid uint64, err error) {
	parts := strings.Split(s, ":")
	gid, err = strconv.ParseUint(parts[0], 36, 64)
	if err != nil {
		return 0, 0, err
	}
	oid, err = strconv.ParseUint(parts[1], 36, 64)
	if err != nil {
		return 0, 0, err
	}
	return gid, oid, nil
}

func encodeGidOid(gid, oid uint64) string {
	return strconv.FormatUint(gid, 36) + ":" + strconv.FormatUint(oid, 36)
	return fmt.Sprintf("%x:%x", gid, oid)
}

func main() {

	app := cli.NewApp()
	app.Name = "admin"
	app.Usage = "admin subcommand"
	app.Commands = []*cli.Command{
		{
			Name:  "directput",
			Usage: "directput --cluster <path> --gid <gid>  <file>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3301"},
				&cli.Uint64Flag{Name: "gid"},
			},
			Action: putAspira,
		},
		{
			Name:  "directsput",
			Usage: "directsput --cluster <path> --gid <gid> <file>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3301"},
				&cli.Uint64Flag{Name: "gid"},
			},
			Action: sputAspiraWorker,
		},
		{
			Name:  "directget",
			Usage: "directget --cluster <path> --gid <gid> --oid <oid> <file>",
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
		{
			Name:  "sput",
			Usage: "sput --cluster <path> <file>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401"},
			},
			Action: sputFile,
		},
		{
			Name:  "sget",
			Usage: "sget --cluster <path> --gid <gid> --oid <oid> --code <base64> <file>",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401"},
				&cli.Uint64Flag{Name: "gid"},
				&cli.Uint64Flag{Name: "oid"},
				&cli.StringFlag{Name: "code"},
			},
			Action: sgetFile,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}

}
