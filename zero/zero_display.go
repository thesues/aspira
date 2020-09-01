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
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/olekukonko/tablewriter"
	_ "github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
)

//less function
func cmpString(a, b string) bool {
	if len(a) < len(b) {
		return true
	} else if len(a) > len(b) {
		return false
	}
	//len
	for i := 0; i < len(a); i++ {
		if a[i]-'0' < b[i]-'0' {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return false
}

func (z *Zero) DisplayStore() string {
	z.RLock()
	defer z.RUnlock()
	var data [][]string
	for _, s := range z.stores {
		var row []string
		row = append(row, []string{
			fmt.Sprintf("%d", s.storeInfo.StoreId),
			fmt.Sprintf("%s", s.storeInfo.Address),
			fmt.Sprintf("%v", s.lastEcho.Format(time.RFC3339))}...)
		data = append(data, row)
	}
	output := new(bytes.Buffer)
	table := tablewriter.NewWriter(output)
	table.SetHeader([]string{"StoreID", "ADDRESS", "LastEcho"})

	sort.SliceStable(data, func(i, j int) bool {
		return cmpString(data[i][0], data[j][0])
	})
	table.AppendBulk(data)
	table.Render()
	return string(output.Bytes())

}

func (z *Zero) getFreeBytes(gid uint64) uint64 {
	value, ok := z.gidFreeBytes.Load(gid)
	if !ok {
		return 0
	}
	freeBytes := value.(uint64)
	return freeBytes
}

func (z *Zero) DisplayWorker() string {
	output := new(bytes.Buffer)
	var data [][]string
	table := tablewriter.NewWriter(output)
	z.RLock()
	defer z.RUnlock()
	//table.SetAutoMergeCells(true)
	table.SetHeader([]string{"GID", "WorkerID", "StoreID", "StoreAddress", "Progress", "FreeBytes"})
	//table.SetRowLine(true)

	xlog.Logger.Infof("%+v", z.workers)
	xlog.Logger.Infof("%+v", z.stores)

	for _, w := range z.workers {
		var row []string

		row = append(row, []string{
			fmt.Sprintf("%d", w.workerInfo.Gid),
			fmt.Sprintf("%d", w.workerInfo.WorkId),
			fmt.Sprintf("%d", w.workerInfo.StoreId),
			z.stores[w.workerInfo.StoreId].storeInfo.Address,
			w.progress.String(),
			fmt.Sprintf("%d", z.getFreeBytes(w.workerInfo.Gid)),
		}...)
		data = append(data, row)
	}

	sort.SliceStable(data, func(a, b int) bool {
		//compare gid first, and workerId
		if data[a][0] == data[b][0] {
			return cmpString(data[a][1], data[b][1])
		}
		return cmpString(data[a][0], data[b][0])
	})
	table.AppendBulk(data)
	table.Render()
	return string(output.Bytes())
}
