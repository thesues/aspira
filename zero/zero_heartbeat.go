package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	_ "github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
)

func sortUint64(a []uint64) {
	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})
}

func findNewAndObsolete(newReports []uint64, current []uint64) (newids []uint64, obsolet []uint64) {

	i := 0
	j := 0
	for i < len(newReports) && j < len(current) {
		if newReports[i] == current[j] {
			i++
			j++
		} else if newReports[i] < current[j] {
			newids = append(newids, newReports[i])
			i++
		} else {
			obsolet = append(obsolet, current[j])
			j++
		}
	}

	for i < len(newReports) {
		newids = append(newids, newReports[i])
		i++
	}

	for j < len(current) {
		obsolet = append(obsolet, current[j])
		j++
	}
	return
}

// update clusterWorker
// currentGID is read from etcd, so if we have any worker on store, the gids will be different.
// merge them, and store storeInfo into etcd
func (z *Zero) updateWorkerInfo(storeId uint64, m map[uint64]*aspirapb.WorkerInfo) {
	xlog.Logger.Info("updateWorkerInfo")
	defer xlog.Logger.Info("done")

	z.workerLock.Lock()
	//TODO, add some validation code
	var gidsInStore []uint64
	for gid, info := range m {
		if info.Leader {
			z.clusterWorker[gid] = proto.Clone(info).((*aspirapb.WorkerInfo))
		}
		gidsInStore = append(gidsInStore, gid)
	}
	z.workerLock.Unlock()

	z.reLock.RLock()
	storeInfo, _ := z.clusterStore[storeId]
	z.reLock.RUnlock()

	//gidsInStore and storeInfo.CurrentGids should be sorted. maybe remove the following 2 lines
	sortUint64(gidsInStore)           //report status
	sortUint64(storeInfo.CurrentGids) //saved status

	newIDs, obsolete := findNewAndObsolete(gidsInStore, storeInfo.CurrentGids)

	if len(newIDs) != 0 || len(obsolete) != 0 {

		storeInfo.CurrentGids = gidsInStore
		z.reLock.Lock()
		z.clusterStore[storeId] = storeInfo
		z.reLock.Unlock()

		if len(newIDs) != 0 {
			xlog.Logger.Infof("Add new ids [%+v] for store[%d]", newIDs)
		}
		if len(obsolete) != 0 {
			xlog.Logger.Infof("Remove obsolete ids [%+v] for store[%d]", obsolete)
		}

		//save new storeInfo in etcd
		key := fmt.Sprintf("store_%d", storeId)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		txn := clientv3.NewKV(z.Client).Txn(ctx)
		xxx, err := storeInfo.Marshal()
		if err != nil {
			xlog.Logger.Warnf(err.Error())
		}
		_, err = txn.Then(clientv3.OpPut(key, string(xxx))).Commit()
		if err != nil {
			xlog.Logger.Warnf(err.Error())
		}
	}
}

func (z *Zero) StreamHeartbeat(stream aspirapb.Zero_StreamHeartbeatServer) error {
	if !z.amLeader() {
		return errors.Errorf("not a leader")
	}
	for {
		req, err := stream.Recv()
		xlog.Logger.Infof("%+v, err %v", req, err)
		if !z.amLeader() {
			return errors.Errorf("not a leader")
		}
		if err == io.EOF || req == nil {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		z.reLock.RLock()
		_, ok := z.clusterStore[req.StoreId]
		z.reLock.RUnlock()
		if !ok {
			return errors.Errorf("store %d not registered", req.StoreId)
		}

		z.updateWorkerInfo(req.StoreId, req.Workers)
	}
}
