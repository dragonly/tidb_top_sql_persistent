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

type Client struct {
	cpuTimeRecordStream tipb.TopSQLAgent_ReportCPUTimeRecordsClient
	sqlMetaStream       tipb.TopSQLAgent_ReportSQLMetaClient
	planMetaStream      tipb.TopSQLAgent_ReportPlanMetaClient
}

func NewClient(
	cpuTimeRecordStream tipb.TopSQLAgent_ReportCPUTimeRecordsClient,
	sqlMetaStream tipb.TopSQLAgent_ReportSQLMetaClient,
	planMetaStream tipb.TopSQLAgent_ReportPlanMetaClient,
) *Client {
	return &Client{
		cpuTimeRecordStream: cpuTimeRecordStream,
		sqlMetaStream:       sqlMetaStream,
		planMetaStream:      planMetaStream,
	}
}

// Start starts a goroutine, which sends tidb-server's last minute's data to the gRPC server
func (s *Client) Start() {
	var reqBatch []*tipb.CPUTimeRecord
	for i := 0; i < 10; i++ {
		req := &tipb.CPUTimeRecord{
			SqlDigest:              []byte("SQLDigest"),
			PlanDigest:             []byte("PlanDigest"),
			RecordListTimestampSec: []uint64{uint64(i)},
			RecordListCpuTimeMs:    []uint32{uint32(i * 100)},
		}
		reqBatch = append(reqBatch, req)
	}

	s.sendCPUTimeRecordBatch(reqBatch)
}

func (s *Client) sendCPUTimeRecordBatch(batch []*tipb.CPUTimeRecord) {
	for _, req := range batch {
		if err := s.cpuTimeRecordStream.Send(req); err != nil {
			log.Fatalf("send stream request failed: %v", err)
		}
	}
	resp, err := s.cpuTimeRecordStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("receive stream response failed: %v", err)
	}
	log.Printf("received stream response: %v", resp)
}

func (s *Client) sendSQLMetaBatch(batch []*tipb.SQLMeta) {
	for _, req := range batch {
		if err := s.sqlMetaStream.Send(req); err != nil {
			log.Fatalf("send stream request failed: %v", err)
		}
	}
	resp, err := s.sqlMetaStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("receive stream response failed: %v", err)
	}
	log.Printf("received stream response: %v", resp)
}

func (s *Client) sendPlanMetaBatch(batch []*tipb.PlanMeta) {
	for _, req := range batch {
		if err := s.planMetaStream.Send(req); err != nil {
			log.Fatalf("send stream request failed: %v", err)
		}
	}
	resp, err := s.planMetaStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("receive stream response failed: %v", err)
	}
	log.Printf("received stream response: %v", resp)
}

func newGrpcClient(ctx context.Context, addr string) (*grpc.ClientConn, *Client) {
	dialCtx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connecting server failed: %v", err)
	}
	grpcClient := tipb.NewTopSQLAgentClient(conn)
	cpuTimeRecordStream, err := grpcClient.ReportCPUTimeRecords(ctx)
	if err != nil {
		log.Fatalf("open cpu time record stream failed: %v", err)
	}
	sqlMetaStream, err := grpcClient.ReportSQLMeta(ctx)
	if err != nil {
		log.Fatalf("open cpu time record stream failed: %v", err)
	}
	planMetaStream, err := grpcClient.ReportPlanMeta(ctx)
	if err != nil {
		log.Fatalf("open cpu time record stream failed: %v", err)
	}

	tidbSender := NewClient(
		cpuTimeRecordStream,
		sqlMetaStream,
		planMetaStream,
	)
	return conn, tidbSender
}

func SendRequest() {
	addr := "localhost:23333"
	conn, sender := newGrpcClient(context.TODO(), addr)
	sender.Start()
	conn.Close()
}
