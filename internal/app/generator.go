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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	influxdb "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	sqlNum         = 200
	instanceNum    = 100
	startTs        = 0
	endTs          = 60 * 60 * 24
	writeWorkerNum = 10
)

func GenerateCPUTimeRecords(recordChan chan *tipb.CPUTimeRecord) {
	count := 0
	lastCount := 0
	lastTime := time.Now()
	qps := 0
	for minute := startTs; minute < endTs/60; minute++ {
		for i := 0; i < instanceNum; i++ {
			for j := 0; j < sqlNum; j++ {
				var tsList []uint64
				var cpuTimeList []uint32
				for i := 0; i < 60; i++ {
					tsList = append(tsList, uint64(minute*60+i))
					cpuTimeList = append(cpuTimeList, rand.Uint32()%1000)
				}
				recordChan <- &tipb.CPUTimeRecord{
					SqlDigest:     []byte(fmt.Sprintf("i%d_sql%d", i, j)),
					PlanDigest:    []byte(fmt.Sprintf("i%d_plan%d", i, j)),
					TimestampList: tsList,
					CpuTimeMsList: cpuTimeList,
				}
				count++
				if count%100 == 0 {
					log.Printf("generated %d records, qps %d\n", count, qps)
					now := time.Now()
					duration := now.Sub(lastTime)
					if duration > time.Second {
						qps = 60 * (count - lastCount) * int(time.Second) / int(duration)
						lastCount = count
						lastTime = now
					}
				}
			}
		}
	}
}

func GenerateSQLMeta(sqlMetaChan chan *tipb.SQLMeta) {
	for i := 0; i < instanceNum; i++ {
		for j := 0; j < sqlNum; j++ {
			sqlDigest := []byte(fmt.Sprintf("i%d_sql%d", i, j))
			sql := make([]byte, rand.Uint32()%1024*1000+1024*100)
			sql = append(sql, sqlDigest...)
			for i := len(sqlDigest); i < len(sql); i++ {
				sql[i] = 'o'
			}
			sqlMetaChan <- &tipb.SQLMeta{
				SqlDigest:     sqlDigest,
				NormalizedSql: string(sql),
			}
		}
	}
}

func GeneratePlanMeta(planMetaChan chan *tipb.PlanMeta) {
	for i := 0; i < instanceNum; i++ {
		for j := 0; j < sqlNum; j++ {
			planDigest := []byte(fmt.Sprintf("i%d_plan%d", i, j))
			plan := make([]byte, rand.Uint32()%1024*1000+1024*100)
			plan = append(plan, planDigest...)
			for i := len(planDigest); i < len(plan); i++ {
				plan[i] = 'o'
			}
			planMetaChan <- &tipb.PlanMeta{
				PlanDigest:     planDigest,
				NormalizedPlan: string(plan),
			}
		}
	}
}

func writeCPUTimeRecordsInfluxDB(wg *sync.WaitGroup, writeAPI api.WriteAPIBlocking, recordsChan chan *tipb.CPUTimeRecord) {
	// i := 0
	for {
		records := <-recordsChan
		var points []*write.Point
		for i, ts := range records.TimestampList {
			cpuTimeMs := records.CpuTimeMsList[i]
			p := influxdb.NewPoint("cpu_time",
				map[string]string{
					"sql_digest":  string(records.SqlDigest),
					"plan_digest": string(records.PlanDigest),
				},
				map[string]interface{}{
					"cpu_time_ms": cpuTimeMs,
				},
				time.Unix(int64(ts), 0),
			)
			points = append(points, p)
		}
		writeAPI.WritePoint(context.TODO(), points...)
		// i++
		// if i%100 == 0 {
		// 	log.Printf("write %d records\n", i)
		// }
	}
}

func WriteInfluxDB() {
	recordChan := make(chan *tipb.CPUTimeRecord, writeWorkerNum)
	sqlMetaChan := make(chan *tipb.SQLMeta, writeWorkerNum)
	planMetaChan := make(chan *tipb.PlanMeta, writeWorkerNum)
	var wg sync.WaitGroup
	wg.Add(1)
	go GenerateCPUTimeRecords(recordChan)
	go GenerateSQLMeta(sqlMetaChan)
	go GeneratePlanMeta(planMetaChan)

	org := "pingcap"
	bucket := "test"
	token := "cUDigADLBUvQHTabhzjBjL_YM1MVofBUUSZx_-uwKy8mR4S_Eqjt6myugvj3ryOfRUBHOGnlyCbTkKbNGVt1rQ=="
	url := "http://localhost:2333"
	client := influxdb.NewClient(url, token)
	writeAPI := client.WriteAPIBlocking(org, bucket)
	// t, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	// p := influxdb.NewPoint("stat",
	// 	map[string]string{"uint": "temperatur"},
	// 	map[string]interface{}{"avg": 24.5, "max": 46},
	// 	t,
	// )
	// writeAPI.WritePoint(context.TODO(), p)
	for i := 0; i < writeWorkerNum; i++ {
		go writeCPUTimeRecordsInfluxDB(&wg, writeAPI, recordChan)
	}
	wg.Wait()

	client.Close()
}
