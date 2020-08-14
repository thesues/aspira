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
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	_ "github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
)

func (z *Zero) StreamHeartbeat(stream aspirapb.Zero_StreamHeartbeatServer) error {

	if !z.amLeader() {
		return errors.Errorf("not a leader")
	}
	for {
		req, err := stream.Recv()
		if !z.amLeader() {
			return errors.Errorf("not a leader")
		}
		if err == io.EOF || req == nil {
			xlog.Logger.Infof("remote heartbeat is closed")
			return err
		}
		if err != nil {
			xlog.Logger.Infof("streamheartbeat %+v", err)
			return errors.WithStack(err)
		}

		z.RLock() //maybe thread-safe,
		store, ok := z.stores[req.StoreId]
		z.RUnlock()
		if !ok {
			xlog.Logger.Info()
			return errors.Errorf("store %d not registered", req.StoreId)
		}
		//valid address

		//update last echo time of store, or other usage info
		store.lastEcho = time.Now()

		if req.Workers == nil {
			continue
		}

		for gid, status := range req.Workers {
			if status == nil {
				continue
			}

			for _, workerID := range z.gidToWorkerID[gid] {
				z.RLock()
				worker := z.workers[workerID]
				z.RUnlock()

				if workerID == status.RaftContext.Id {
					//leader, default raft library will set leader's status to aspirapb.WorkerStatus_Probe
					worker.progress = aspirapb.WorkerStatus_Leader
					continue
				}
				p, ok := status.Progress[workerID]
				if !ok {
					z.workers[workerID].progress = aspirapb.WorkerStatus_Unknown
					//worker missing on report...set this worker missing in the futhure.
					continue
				}

				//valid?
				if worker.workerInfo == nil || worker.workerInfo.Gid != gid ||
					worker.workerInfo.WorkId != workerID {
					//FIXME
					xlog.Logger.Warnf("reported worker's status should be %+v, but yours is [gid: %d, storeId: %d, id: %d]",
						worker.workerInfo,
						gid, workerID,
					)
					continue
				}
				z.workers[workerID].progress = p
			}
		}
	}
}
