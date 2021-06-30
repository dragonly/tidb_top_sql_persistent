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
	"io"
	"log"
	"net"

	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
)

var _ tipb.TopSQLAgentServer = &TopSQLAgentServer{}

// TopSQLAgentServer is the main struct which controls the Top SQL metrics collecting and reporting.
//
// In general, there are several parts in the agent which are working together:
// - gRPC server receives raw data from tidb-server/tikv-server
// - receiver writes gRPC data to WAL, which typically resides in the disk. There're also in-memory
//   implementations which is used in testing.
// - WAL-buffer-prefetcher prefetches WAL data to an in-memory buffer, which is a sliding-window-like
//   in-memory view of the WAL data. The prefetch buffer has an upper limit, which can be seemed as
//   the max window size of the view.
// - sender reads data from prefetched buffer, and asynchronously sends it to the remote store.
//
// When the data is receiving and sending quickly enough, the data will only be written to WAL once, and
// served directly to the sender from receiver in memory, which is IO efficient.
// When the agent process crashes, it will re-construct the prefetch buffer, and restarts sending data
// to the store as usual.
// After all, the prefetched buffer is the only source for data from which to get out of the agent process.
type TopSQLAgentServer struct {
	receiver *Receiver
	sender   *Sender
}

func NewAgentServer(wal WAL, store Store) *TopSQLAgentServer {
	prefetcher := NewPrefetcher(wal, PrefetcherConfig{
		CPUTimeRecordCount: 1000,
		SQLMetaCount:       100,
		PlanMetaCount:      100,
	})
	receiver := NewReceiver(wal, prefetcher)
	sender := NewSender(prefetcher, store)
	return &TopSQLAgentServer{
		receiver: receiver,
		sender:   sender,
	}
}

func (as *TopSQLAgentServer) Start() {
	go as.sender.start()
}

func (as *TopSQLAgentServer) ReportPlanMeta(stream tipb.TopSQLAgent_ReportPlanMetaServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		as.receiver.receivePlanMeta(req)
	}
	resp := &tipb.EmptyResponse{}
	stream.SendAndClose(resp)
	return nil
}

func (as *TopSQLAgentServer) ReportSQLMeta(stream tipb.TopSQLAgent_ReportSQLMetaServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		as.receiver.receiveSQLMeta(req)
	}
	resp := &tipb.EmptyResponse{}
	stream.SendAndClose(resp)
	return nil
}

func (as *TopSQLAgentServer) ReportCPUTimeRecords(stream tipb.TopSQLAgent_ReportCPUTimeRecordsServer) error {
	log.Println("server: ReportCPUTimeRecords")
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		as.receiver.receiveCPUTimeRecords(req)
	}
	resp := &tipb.EmptyResponse{}
	stream.SendAndClose(resp)
	return nil
}

func StartGrpcServer(addr string) *TopSQLAgentServer {
	if len(addr) == 0 {
		addr = ":23333"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on tcp address %s, %v", addr, err)
	}
	server := grpc.NewServer()
	wal := NewMemWAL()
	store := NewMemStore()
	agentServer := NewAgentServer(wal, store)
	agentServer.Start()
	tipb.RegisterTopSQLAgentServer(server, agentServer)

	log.Printf("start listening on %s", addr)
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Fatalf("failed to start gRPC server: %v", err)
		}
	}()
	return agentServer
}
