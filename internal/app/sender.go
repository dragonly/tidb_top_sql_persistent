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
	"log"

	pb "github.com/dragonly/tidb_top_sql_persistent/internal/app/protobuf"
)

type tidbSender struct {
	stream pb.TopSQLAgent_CollectCPUTimeClient
}

func NewTiDBSender(stream pb.TopSQLAgent_CollectCPUTimeClient) *tidbSender {
	return &tidbSender{
		stream: stream,
	}
}

// Start starts a goroutine, which sends tidb-server's last minute's data to the gRPC server
func (s *tidbSender) Start() {
	var reqBatch []*pb.CollectCPUTimeRequest
	for i := 0; i < 10; i++ {
		req := &pb.CollectCPUTimeRequest{
			TimestampList: []uint64{uint64(i)},
			CpuTimeMsList: []uint32{uint32(i * 100)},
			NormalizedSql: "select ? from t1",
		}
		reqBatch = append(reqBatch, req)
	}

	s.sendBatch(reqBatch)
}

func (s *tidbSender) sendBatch(batch []*pb.CollectCPUTimeRequest) {
	for _, req := range batch {
		req.TimestampList[0] += 1
		if err := s.stream.Send(req); err != nil {
			log.Fatalf("send stream request failed: %v", err)
		}
	}
	resp, err := s.stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("receive stream response failed: %v", err)
	}
	log.Printf("received stream response: %v", resp)
}
