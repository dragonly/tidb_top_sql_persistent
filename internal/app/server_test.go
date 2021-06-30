/*
Copyright © 2021 Li Yilong <liyilongko@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
)

type CPUTimeRecordSortByFirst []*tipb.CPUTimeRecord

func (a CPUTimeRecordSortByFirst) Len() int      { return len(a) }
func (a CPUTimeRecordSortByFirst) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a CPUTimeRecordSortByFirst) Less(i, j int) bool {
	return a[i].TimestampList[0] < a[j].TimestampList[0]
}

func TestSendData(t *testing.T) {
	addr := "localhost:23333"
	server := StartGrpcServer(addr)
	conn, client := newGrpcClient(context.TODO(), addr)
	defer conn.Close()
	var cpuTimeRecordBatch []*tipb.CPUTimeRecord
	count := 1000
	for i := 0; i < count; i++ {
		cpuTimeRecordBatch = append(cpuTimeRecordBatch,
			&tipb.CPUTimeRecord{
				SqlDigest:     []byte("SQLDigest"),
				PlanDigest:    []byte("PlanDigest"),
				TimestampList: []uint64{uint64(i)},
				CpuTimeMsList: []uint32{uint32(i)},
			},
		)
	}
	client.sendBatch(cpuTimeRecordBatch)
	// wait sender to send out the record
	time.Sleep(10 * time.Millisecond)
	cpuTimeRecordList := server.sender.store.(*MemStore).cpuTimeRecordList
	sort.Sort(CPUTimeRecordSortByFirst(cpuTimeRecordList))
	assert.Equal(t, count, len(cpuTimeRecordList))
	for i := 0; i < count; i++ {
		assert.Equal(t, *cpuTimeRecordBatch[i], *cpuTimeRecordList[i])
	}
}
