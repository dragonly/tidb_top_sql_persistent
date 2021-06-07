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

func SendRequest() {
	addr := "localhost:23333"
	dialCtx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connecting server failed: %v", err)
	}
	defer conn.Close()
	c := tipb.NewTopSQLAgentClient(conn)
	ctx, cancel := context.WithTimeout(dialCtx, time.Second)
	defer cancel()
	stream, err := c.ReportCPUTimeRecords(ctx)
	if err != nil {
		log.Fatalf("open stream failed: %v", err)
	}

	tidbSender := NewTiDBSender(stream)
	tidbSender.Start()
}
