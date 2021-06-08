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
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
)

var _ tipb.TopSQLAgentServer = &monkeyServer{}

type monkeyServer struct{}

func (*monkeyServer) ReportPlanMeta(stream tipb.TopSQLAgent_ReportPlanMetaServer) error {
	log.Println("call ReportPlanMeta()")
	resp := &tipb.EmptyResponse{}
	stream.SendAndClose(resp)
	return nil
}

func (*monkeyServer) ReportSQLMeta(stream tipb.TopSQLAgent_ReportSQLMetaServer) error {
	log.Println("call ReportSQLMeta()")
	resp := &tipb.EmptyResponse{}
	stream.SendAndClose(resp)
	return nil
}

func (*monkeyServer) ReportCPUTimeRecords(stream tipb.TopSQLAgent_ReportCPUTimeRecordsServer) error {
	log.Println("call ReportCPUTimeRecords()")
	resp := &tipb.EmptyResponse{}
	stream.SendAndClose(resp)
	return nil
}

func startProxyServer(lisProxy net.Listener, toPort int) error {
	toAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", toPort))
	if err != nil {
		return err
	}
	log.Printf("top addr: %+v\n", toAddr)
	for {
		connFrom, err := lisProxy.Accept()
		log.Printf("new client connection: %+v\n", connFrom)
		if err != nil {
			log.Printf("[proxy] failed to create connection from client, %v", err)
			continue
		}
		go func() {
			defer connFrom.Close()
			connTo, err := net.DialTCP("tcp", nil, toAddr)
			log.Printf("new upstream connection: %+v\n", connTo)
			if err != nil {
				log.Printf("[proxy] failed to create connection to upstream, %v", err)
				return
			}
			defer connTo.Close()
			wg := sync.WaitGroup{}
			wg.Add(2)
			go copy(&wg, connTo, connFrom)
			go copy(&wg, connFrom, connTo)
			wg.Wait()
		}()
	}
}

func copy(wg *sync.WaitGroup, dst io.Writer, src io.Reader) {
	n, err := io.Copy(dst, src)
	log.Printf("copied %d bytes from %v to %v, err: %v", n, src, dst, err)
	wg.Done()
}

func StartMonkeyServer(proxyAddress string) {
	// proxy
	lisProxy, err := net.Listen("tcp", proxyAddress)
	addrProxy := lisProxy.Addr().(*net.TCPAddr)
	if err != nil {
		log.Fatalf("[proxy] failed to listen on TCP address %s:%d, %v", addrProxy.IP, addrProxy.Port, err)
	}
	defer lisProxy.Close()
	log.Printf("[proxy] start listening on %s:%d", addrProxy.IP, addrProxy.Port)

	// gRPC
	lisGRPC, err := net.Listen("tcp", ":0")
	addrGRPC := lisGRPC.Addr().(*net.TCPAddr)
	if err != nil {
		log.Fatalf("[gRPC] failed to listen on TCP address %s:%d, %v", addrGRPC.IP, addrGRPC.Port, err)
	}
	defer lisGRPC.Close()
	log.Printf("[gRPC] start listening on %s:%d", addrGRPC.IP, addrGRPC.Port)

	server := grpc.NewServer()
	tipb.RegisterTopSQLAgentServer(server, &monkeyServer{})
	log.Printf("[gRPC] start gRPC server")
	go func() {
		if err := server.Serve(lisGRPC); err != nil {
			log.Fatalf("failed to start gRPC server: %v", err)
		}
	}()

	log.Println("[proxy] start proxying TCP traffic")
	startProxyServer(lisProxy, addrGRPC.Port)
}
