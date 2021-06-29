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
	"log"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
)

type tidbSender struct {
	stream tipb.TopSQLAgent_ReportCPUTimeRecordsClient
}

func NewTiDBSender(stream tipb.TopSQLAgent_ReportCPUTimeRecordsClient) *tidbSender {
	return &tidbSender{
		stream: stream,
	}
}

// Start starts a goroutine, which sends tidb-server's last minute's data to the gRPC server
func (s *tidbSender) Start() {
	var reqBatch []*tipb.CPUTimeRecord
	for i := 0; i < 10; i++ {
		req := &tipb.CPUTimeRecord{
			SqlDigest:     []byte("SQLDigest"),
			PlanDigest:    []byte("PlanDigest"),
			TimestampList: []uint64{uint64(i)},
			CpuTimeMsList: []uint32{uint32(i * 100)},
		}
		reqBatch = append(reqBatch, req)
	}

	s.sendBatch(reqBatch)
}

func (s *tidbSender) sendBatch(batch []*tipb.CPUTimeRecord) {
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

func newGrpcClient(ctx context.Context, addr string) (*grpc.ClientConn, *tidbSender) {
	dialCtx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connecting server failed: %v", err)
	}
	grpcClient := tipb.NewTopSQLAgentClient(conn)
	stream, err := grpcClient.ReportCPUTimeRecords(ctx)
	if err != nil {
		log.Fatalf("open stream failed: %v", err)
	}

	tidbSender := NewTiDBSender(stream)
	return conn, tidbSender
}

func SendRequest() {
	addr := "localhost:23333"
	conn, sender := newGrpcClient(context.TODO(), addr)
	sender.Start()
	conn.Close()
}
