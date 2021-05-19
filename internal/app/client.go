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
	pb "github.com/dragonly/tidb_top_sql_persistent/internal/app/protobuf"
	"google.golang.org/grpc"
	"log"
	"time"
)

func SendRequest() {
	addr := "localhost:23333"
	dialCtx, cancel := context.WithTimeout(context.TODO(), time.Second)
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connecting server failed: %v", err)
	}
	defer conn.Close()
	c := pb.NewAgentClient(conn)
	req := pb.CPUTimeRequestTiDB{
		Timestamp:     []uint64{1},
		CpuTime:       []uint32{100},
		SqlNormalized: "select ? from t1",
	}
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	stream, err := c.CollectTiDB(ctx)
	if err != nil {
		log.Fatalf("open stream failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		req.Timestamp[0] += 1
		if err := stream.Send(&req); err != nil {
			log.Fatalf("send stream request failed: %v", err)
		}
		resp, err := stream.Recv()
		if err != nil {
			log.Fatalf("receive stream response failed: %v", err)
		}
		log.Printf("received stream response: %v", resp)
	}
}
