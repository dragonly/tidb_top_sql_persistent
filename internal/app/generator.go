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
	"strings"
	"sync"
	"time"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
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
	writeWorkerNum = 5
)

func GenerateCPUTimeRecords(recordChan chan *tipb.CPUTimeRecord) {
	count := 0
	lastCount := 0
	lastTime := time.Now()
	qps := 0
	for minute := startTs; minute < endTs/60; minute++ {
		log.Printf("[cpu time] timestamp: %d\n", minute*60)
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
					log.Printf("[cpu time] generated %d records, qps %d\n", count, qps)
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
	count := 0
	lastCount := 0
	lastTime := time.Now()
	qps := 0
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
			count++
			if count%100 == 0 {
				log.Printf("[sql meta] generated %d records, qps %d\n", count, qps)
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

func GeneratePlanMeta(planMetaChan chan *tipb.PlanMeta) {
	count := 0
	lastCount := 0
	lastTime := time.Now()
	qps := 0
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
			count++
			if count%100 == 0 {
				log.Printf("[plan meta] generated %d records, qps %d\n", count, qps)
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

func writeCPUTimeRecordsInfluxDB(writeAPI api.WriteAPIBlocking, recordsChan chan *tipb.CPUTimeRecord) {
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

func writeSQLMeta(writeAPI api.WriteAPIBlocking, sqlMetaChan chan *tipb.SQLMeta) {
	for {
		meta := <-sqlMetaChan
		p := influxdb.NewPoint("sql_meta",
			map[string]string{
				"sql_digest": string(meta.SqlDigest),
			},
			map[string]interface{}{
				"normalized_sql": meta.NormalizedSql,
			},
			time.Now(),
		)
		writeAPI.WritePoint(context.TODO(), p)
	}
}

func writePlanMeta(writeAPI api.WriteAPIBlocking, planMetaChan chan *tipb.PlanMeta) {
	for {
		meta := <-planMetaChan
		p := influxdb.NewPoint("plan_meta",
			map[string]string{
				"plan_digest": string(meta.PlanDigest),
			},
			map[string]interface{}{
				"normalized_plan": meta.NormalizedPlan,
			},
			time.Now(),
		)
		writeAPI.WritePoint(context.TODO(), p)
	}
}

func WriteInfluxDB() {
	recordChan := make(chan *tipb.CPUTimeRecord, writeWorkerNum*2)
	sqlMetaChan := make(chan *tipb.SQLMeta, writeWorkerNum*2)
	planMetaChan := make(chan *tipb.PlanMeta, writeWorkerNum*2)
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
		// go writeCPUTimeRecordsInfluxDB(writeAPI, recordChan)
		go writeSQLMeta(writeAPI, sqlMetaChan)
		go writePlanMeta(writeAPI, planMetaChan)
	}
	wg.Wait()

	client.Close()
}

func writeCPUTimeRecordsTiDB(recordsChan chan *tipb.CPUTimeRecord) {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test?charset=utf8")
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	defer db.Close()

	var sqlBuf strings.Builder
	sqlBuf.WriteString("INSERT INTO cpu_time (sql_digest, plan_digest, timestamp, cpu_time_ms) VALUES ")
	for i := 0; i < 59; i++ {
		sqlBuf.WriteString("(?,?,?,?),")
	}
	sqlBuf.WriteString("(?,?,?,?)")
	sql := sqlBuf.String()
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Fatalf("failed to prepare for SQL statement, %v", err)
	}

	for {
		records := <-recordsChan
		values := make([]interface{}, 0, len(records.TimestampList)*4)
		for i, ts := range records.TimestampList {
			cpuTimeMs := records.CpuTimeMsList[i]
			// var instance_id, sql_id int
			// _, err := fmt.Sscanf(string(records.SqlDigest), "i%d_sql%d", &instance_id, &sql_id)
			// if err != nil {
			// 	log.Fatalf("failed to parse instance ID in invalid SQL digest '%s', %v", records.SqlDigest, err)
			// }
			// log.Printf("parsed %d items\n", n)
			values = append(values, string(records.SqlDigest), string(records.PlanDigest), ts, cpuTimeMs)
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			log.Fatalf("failed to execute SQL statement, %v", err)
		}
		// log.Printf("execute SQL result: %v", result)
	}
}

func WriteTiDB() {
	recordChan := make(chan *tipb.CPUTimeRecord, writeWorkerNum*2)
	go GenerateCPUTimeRecords(recordChan)
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test?charset=utf8")
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS cpu_time (
		id BIGINT AUTO_RANDOM NOT NULL,
		sql_digest VARBINARY(32) NOT NULL,
		plan_digest VARBINARY(32),
		timestamp INTEGER NOT NULL,
		cpu_time_ms INTEGER NOT NULL,
		PRIMARY KEY (id)
	);`)
	log.Printf("create table err: %v\n", err)
	_, err = db.Exec("ALTER TABLE cpu_time SET TIFLASH REPLICA 1;")
	log.Printf("set tiflash err: %v\n", err)
	for i := 0; i < writeWorkerNum; i++ {
		go writeCPUTimeRecordsTiDB(recordChan)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
