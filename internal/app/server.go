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

var _ tipb.TopSQLAgentServer = &agentServer{}

type agentServer struct{}

func (*agentServer) ReportPlanMeta(tipb.TopSQLAgent_ReportPlanMetaServer) error {
	return nil
}

func (*agentServer) ReportSQLMeta(tipb.TopSQLAgent_ReportSQLMetaServer) error {
	return nil
}

func (*agentServer) ReportCPUTimeRecords(stream tipb.TopSQLAgent_ReportCPUTimeRecordsServer) error {
	log.Print("start collecting from tidb-server")
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		log.Printf("received: %v\n", req)
	}
	resp := &tipb.EmptyResponse{}
	stream.SendAndClose(resp)
	return nil
}

func StartServer() {
	addr := ":23333"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on tcp address %s, %v", addr, err)
	}
	server := grpc.NewServer()
	tipb.RegisterTopSQLAgentServer(server, &agentServer{})

	log.Printf("start listening on %s", addr)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to start gRPC server: %v", err)
	}
}
