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

	"github.com/olekukonko/tablewriter"
	_ "github.com/thesues/aspira/utils"
)

func (z *Zero) Display() string {
	output := new(bytes.Buffer)
	var data [][]string
	table := tablewriter.NewWriter(output)
	z.RLock()
	defer z.RUnlock()
	table.SetAutoMergeCells(true)
	table.SetHeader([]string{"GID", "WorkerID", "StoreID", "StoreAddress", "Progress"})
	table.SetRowLine(true)

	//merge gid
	for gid, workerIDs := range z.gidToWorkerID {
		var row []string
		for _, workerID := range workerIDs {
			row = append(row, fmt.Sprintf("%d", gid))
			p := z.workers[workerID]
			storeID := p.workerInfo.StoreId
			store := z.stores[storeID]
			row = append(row, []string{
				fmt.Sprintf("%d", workerID),
				fmt.Sprintf("%d", storeID),
				store.storeInfo.Address,
				p.progress.String()}...)

		}
		data = append(data, row)
	}
	table.AppendBulk(data)
	table.Render()
	return string(output.Bytes())
}
