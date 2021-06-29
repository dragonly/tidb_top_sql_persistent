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

func TestTopSQLAgentServer(t *testing.T) {
	addr := "localhost:23333"
	server := StartGrpcServer(addr)
	conn, client := newGrpcClient(context.TODO(), addr)
	defer conn.Close()
	cpuTimeRecordBatch := []*tipb.CPUTimeRecord{
		{
			SqlDigest:     []byte("SQLDigest"),
			PlanDigest:    []byte("PlanDigest"),
			TimestampList: []uint64{uint64(1)},
			CpuTimeMsList: []uint32{uint32(100)},
		},
	}
	client.sendBatch(cpuTimeRecordBatch)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, len(server.sender.store.(*MemStore).cpuTimeRecordList))
}
