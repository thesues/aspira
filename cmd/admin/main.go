package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

	res, err := client.AddWorkerGroup(context.Background(), &aspirapb.ZeroAddWorkerGroupRequest{})
	if err != nil {
		return err
	}

	fmt.Printf("Gid %d created\n", res.Gid)
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

	f, err := os.OpenFile(fileName, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
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
}

func setRandStringBytes(data []byte) {
	rand.NewSource(time.Now().Unix())
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for i := range data {
		data[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
}

func wbench(c *cli.Context) error {
	size := c.Int("size")
	threadNum := c.Int("thread")
	cluster := c.String("cluster")
	duration := c.Int("duration")
	return bench("wbench", size, threadNum, cluster, duration, true)
}

func rbench(c *cli.Context) error {
	return nil
}

func wrbench(c *cli.Context) error {
	return nil
}

type Result struct {
	Gid       uint64
	Oid       uint64
	StartTime float64 //time.Now().Second
	Elapsed   float64
}

func bench(benchType string, size int, threadNum int, clusterAddr string, duration int, recordLantency bool) error {
	stopper := utils.NewStopper()

	zeroAddrs := strings.Split(clusterAddr, ",")

	data := make([]byte, size)
	setRandStringBytes(data)
	client := aspiraclient.NewAspiraClient(zeroAddrs)
	if err := client.Connect(); err != nil {
		return err
	}
	start := time.Now()

	var count uint64
	done := make(chan struct{})

	livePrint := func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		fmt.Print("\033[s") // save the cursor position

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				//https://stackoverflow.com/questions/56103775/how-to-print-formatted-string-to-the-same-line-in-stdout-with-go
				//how to print in one line
				fmt.Print("\033[u\033[K")
				ops := atomic.LoadUint64(&count) / uint64(time.Now().Sub(start).Seconds())
				fmt.Printf("ops:%d/s  throughput:%s", ops, humanReadableTroughput(float64(size)*float64(ops)))
			}
		}
	}

	var lock sync.Mutex
	var results []Result
	benchStartTime := time.Now()

	go func() {
		for i := 0; i < threadNum; i++ {
			loop := 0 //sample to record lantency
			stopper.RunWorker(func() {
				for {
					select {
					case <-stopper.ShouldStop():
						return
					default:
						start := time.Now()
						gid, oid, err := client.PushData(data)
						if err != nil {
							fmt.Println(err)
							return
						}
						end := time.Now()
						if recordLantency && loop%10 == 0 {
							lock.Lock()
							results = append(results, Result{
								Gid:       gid,
								Oid:       oid,
								StartTime: start.Sub(benchStartTime).Seconds(),
								Elapsed:   end.Sub(start).Seconds(),
							})
							lock.Unlock()
						}

						atomic.AddUint64(&count, 1)
						loop++
					}
				}

			})
		}
		stopper.Wait()
		close(done)
	}()

	go livePrint()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1)
	select {
	case <-sc:
		stopper.Stop()
	case <-time.After(time.Duration(duration) * time.Second):
		stopper.Stop()
	case <-done:
		break
	}

	if recordLantency {
		sort.Slice(results, func(i, j int) bool {
			return results[i].StartTime < results[i].StartTime
		})

		f, err := os.OpenFile("result.json", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		defer f.Close()
		if err == nil {
			out, err := json.Marshal(results)
			if err == nil {
				f.Write(out)
			} else {
				fmt.Println("failed to write result.json")
			}
		}
	}

	printSummary(time.Now().Sub(start), atomic.LoadUint64(&count), size, threadNum)
	return nil
}

func humanReadableTroughput(t float64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	power := int(math.Log10(t) / 3)
	if power >= len(units) || power == 0 {
		return ""
	}
	return fmt.Sprintf("%.2f%s/sec", t/math.Pow(1000, float64(power)), units[power])
}

func printSummary(elapsed time.Duration, totalCount uint64, size int, threadNum int) {
	if int(elapsed.Seconds()) == 0 {
		return
	}
	fmt.Printf("\nSummary\n")
	fmt.Printf("Threads :%d\n", threadNum)
	fmt.Printf("Size    :%d\n", size)
	fmt.Printf("Time taken for tests :%v seconds\n", elapsed.Seconds())
	fmt.Printf("Complete requests :%d\n", totalCount)
	fmt.Printf("Total transferred :%d bytes\n", totalCount*uint64(size))
	fmt.Printf("Requests per second :%d [#/sec]\n", totalCount/uint64(elapsed.Seconds()))
	t := float64(totalCount*uint64(size)) / elapsed.Seconds()
	fmt.Printf("Thoughput per sencond :%s\n", humanReadableTroughput(t))
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
			Name:  "add_group",
			Usage: "add_group --cluster <path>",
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

		{
			Name: "wbench",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401"},
				&cli.IntFlag{Name: "size", Value: 4096, Aliases: []string{"s"}},
				&cli.IntFlag{Name: "thread", Value: 8, Aliases: []string{"t"}},
				&cli.IntFlag{Name: "duration", Value: 10, Aliases: []string{"d"}},
			},
			Action: wbench,
		},
		{
			Name:   "plot",
			Action: plot,
		},

		{
			Name: "rbench",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401"},
			},
			Action: rbench,
		},
		{
			Name: "wrbench",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "cluster", Value: "127.0.0.1:3401"},
			},
			Action: wrbench,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}

}
