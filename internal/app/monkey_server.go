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
	"net"

	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
)

var _ tipb.TopSQLAgentServer = &monkeyServer{}

type monkeyServer struct{}

func (*monkeyServer) ReportPlanMeta(tipb.TopSQLAgent_ReportPlanMetaServer) error {
	return nil
}

func (*monkeyServer) ReportSQLMeta(tipb.TopSQLAgent_ReportSQLMetaServer) error {
	return nil
}

func (*monkeyServer) ReportCPUTimeRecords(stream tipb.TopSQLAgent_ReportCPUTimeRecordsServer) error {
	return nil
}

func StartMonkeyServer(proxyAddress string) {
	lisProxy, err := net.Listen("tcp", proxyAddress)
	addrProxy := lisProxy.Addr().(*net.TCPAddr)
	if err != nil {
		log.Fatalf("[proxy] failed to listen on tcp address %s:%d, %v", addrProxy.IP, addrProxy.Port, err)
	}
	defer lisProxy.Close()
	log.Printf("[proxy] start listening on %s:%d", addrProxy.IP, addrProxy.Port)

	lisGRPC, err := net.Listen("tcp", ":0")
	addrGRPC := lisGRPC.Addr().(*net.TCPAddr)
	if err != nil {
		log.Fatalf("[gRPC] failed to listen on tcp address %s:%d, %v", addrGRPC.IP, addrGRPC.Port, err)
	}
	defer lisGRPC.Close()
	log.Printf("[gRPC] start listening on %s:%d", addrGRPC.IP, addrGRPC.Port)

	server := grpc.NewServer()
	tipb.RegisterTopSQLAgentServer(server, &monkeyServer{})
	log.Printf("[gRPC] start gRPC server")
	if err := server.Serve(lisGRPC); err != nil {
		log.Fatalf("failed to start gRPC server: %v", err)
	}
}
