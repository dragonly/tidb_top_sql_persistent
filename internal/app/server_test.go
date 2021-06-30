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
	"testing"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
)

func TestSendData(t *testing.T) {
	addr := "localhost:23333"
	server := StartGrpcServer(addr)
	conn, client := newGrpcClient(context.TODO(), addr)
	defer conn.Close()

	// CPU time record
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
	client.sendCPUTimeRecordBatch(cpuTimeRecordBatch)
	// wait sender to send out the record
	time.Sleep(10 * time.Millisecond)
	cpuTimeRecordList := server.sender.store.(*MemStore).cpuTimeRecordList
	assert.Equal(t, count, len(cpuTimeRecordList))
	for i := 0; i < count; i++ {
		assert.Equal(t, *cpuTimeRecordBatch[i], *cpuTimeRecordList[i])
	}

	// SQL meta
	var sqlMetaBatch []*tipb.SQLMeta
	count = 100
	for i := 0; i < count; i++ {
		sqlMetaBatch = append(sqlMetaBatch,
			&tipb.SQLMeta{
				SqlDigest:     []byte("SQLDigest"),
				NormalizedSql: "NormalizedSQL",
			},
		)
	}
	client.sendSQLMetaBatch(sqlMetaBatch)
	time.Sleep(10 * time.Millisecond)
	sqlMetaList := server.sender.store.(*MemStore).sqlMetaList
	assert.Equal(t, count, len(sqlMetaList))
	for i := 0; i < count; i++ {
		assert.Equal(t, *sqlMetaBatch[i], *sqlMetaList[i])
	}

	// Plan meat
	var planMetaBatch []*tipb.PlanMeta
	count = 100
	for i := 0; i < count; i++ {
		planMetaBatch = append(planMetaBatch,
			&tipb.PlanMeta{
				PlanDigest:     []byte("PlanDigest"),
				NormalizedPlan: "NormalizedPlan",
			},
		)
	}
	client.sendPlanMetaBatch(planMetaBatch)
	time.Sleep(10 * time.Millisecond)
	planMetaList := server.sender.store.(*MemStore).planMetaList
	assert.Equal(t, count, len(planMetaList))
	for i := 0; i < count; i++ {
		assert.Equal(t, *planMetaBatch[i], *planMetaList[i])
	}
}
