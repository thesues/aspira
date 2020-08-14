package main

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
)

var (
	versionKey = "AspiraVersionKey"
)

func EtcdSetKV(c *clientv3.Client, key string, val []byte, opts ...clientv3.OpOption) error {
	kv := clientv3.NewKV(c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := kv.Put(ctx, key, string(val), opts...)
	return err
}

//TODO, add version id, compare version ID and set...
/*
func EtcdSetKV(c *clientv3.Client, key string, val []byte, opts ...clientv3.OpOption) error {

	currentVersion, err := EtcdGetKV(c, versionKey)
	if err != nil {
		return err
	}

	//build txn, compare and set ID
	var cmp clientv3.Cmp
	var curr uint64

	if currentVersion == nil {
		cmp = clientv3.Compare(clientv3.CreateRevision(versionKey), "=", 0)
	} else {
		curr = binary.BigEndian.Uint64(currentVersion)
		cmp = clientv3.Compare(clientv3.Value(versionKey), "=", string(currentVersion))
	}

	var newVersion [8]byte
	binary.BigEndian.PutUint64(newVersion[:], curr+1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn := clientv3.NewKV(c).Txn(ctx)

	t := txn.If(cmp)
	task := t.Then(
		clientv3.OpPut(versionKey, string(newVersion[:])),
		clientv3.OpPut(key, string(val), opts...))

	for {

		res, err := task.Commit()
		if res.Succeeded == false || err != nil {
			continue
		}
		break
	}
	return nil
}
*/

func EtcdGetKV(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func loadStores(c *clientv3.Client) (map[uint64]*storeProgress, error) {
	kvs, err := EtcdRange(c, "store")
	if err != nil {
		return nil, err
	}
	stores := make(map[uint64]*storeProgress)
	for _, kv := range kvs {
		id, err := parseKey(string(kv.Key), "store")
		if err != nil {
			return nil, errors.WithStack(err)
		}
		var zstore aspirapb.ZeroStoreInfo
		if err = zstore.Unmarshal(kv.Value); err != nil {
			return nil, errors.WithStack(err)
		}
		stores[id] = &storeProgress{
			storeInfo: &zstore,
			lastEcho:  time.Time{},
		}
	}
	return stores, nil

}

func loadWorkers(c *clientv3.Client) (map[uint64]*workerProgress, error) {
	kvs, err := EtcdRange(c, "worker")
	if err != nil {
		return nil, err
	}
	workers := make(map[uint64]*workerProgress)
	for _, kv := range kvs {
		id, err := parseKey(string(kv.Key), "worker")
		if err != nil {
			return nil, errors.WithStack(err)
		}
		var zworker aspirapb.ZeroWorkerInfo
		if err = zworker.Unmarshal(kv.Value); err != nil {
			return nil, errors.WithStack(err)
		}
		workers[id] = &workerProgress{
			workerInfo: &zworker,
			progress:   aspirapb.WorkerStatus_Unknown,
		}
	}
	return workers, nil
}

func EtcdRange(c *clientv3.Client, prefix string) ([]*mvccpb.KeyValue, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(0),
	}
	kv := clientv3.NewKV(c)
	ctx := context.Background()
	resp, err := kv.Get(ctx, prefix, opts...)
	if err != nil {
		return nil, err
	}
	return resp.Kvs, err
}

func parseKey(s string, prefix string) (uint64, error) {
	//example: "worker_1" // or "store_1"

	parts := strings.Split(s, "_")
	if len(parts) != 2 {
		return 0, errors.Errorf("parse key[%s] failed :", s)
	}
	if parts[0] != prefix {
		return 0, errors.Errorf("parse key[%s] failed, parts[0] not match :", s)
	}
	return strconv.ParseUint(parts[1], 10, 64)
}
