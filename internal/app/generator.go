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
	"strconv"
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
	endTs          = 60 * 60 * 1
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
			sep := strings.Split(string(records.SqlDigest), "_")
			instanceID := sep[0][1:]
			p := influxdb.NewPoint("cpu_time",
				map[string]string{
					"sql_digest":  string(records.SqlDigest),
					"plan_digest": string(records.PlanDigest),
					"instance_id": instanceID,
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

func writeSQLMetaInfluxDB(writeAPI api.WriteAPIBlocking, sqlMetaChan chan *tipb.SQLMeta) {
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

func writePlanMetaInfluxDB(writeAPI api.WriteAPIBlocking, planMetaChan chan *tipb.PlanMeta) {
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

	url := "http://localhost:2333"
	token := "cUDigADLBUvQHTabhzjBjL_YM1MVofBUUSZx_-uwKy8mR4S_Eqjt6myugvj3ryOfRUBHOGnlyCbTkKbNGVt1rQ=="
	org := "pingcap"
	bucket := "test"
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
		go writeCPUTimeRecordsInfluxDB(writeAPI, recordChan)
		go writeSQLMetaInfluxDB(writeAPI, sqlMetaChan)
		go writePlanMetaInfluxDB(writeAPI, planMetaChan)
	}
	wg.Wait()

	client.Close()
}

// TODO: join with sql/plan meta
func queryInfluxDB(queryAPI api.QueryAPI, startTs, endTs, instance_id int) {
	query := fmt.Sprintf(`from(bucket:"test")
	|> range(start:%d, stop:%d)
	|> filter(fn: (r) =>
		r._measurement == "cpu_time" and
		r.instance_id == "%d"
	)
	// |> window(every: 1m)
	// |> sum(column: "_value")
	// |> duplicate(column: "_stop", as: "_time")
	|> drop(columns: ["_start", "_stop", "_measurement"])
	|> group(columns: ["_time"])
	|> sort(columns: ["_value"], desc: true)
	|> limit(n:5)
	//|> window(every: inf)
	`, startTs, endTs, instance_id)
	result, err := queryAPI.Query(context.TODO(), query)
	if err != nil {
		log.Printf("failed to execute query, %v\n%s\n", err, query)
		return
	}
	for result.Next() {
		if result.TableChanged() {
			log.Printf("table: %s\n", result.TableMetadata().String())
		}
		log.Printf("value: %v\n", result.Record().Values())
	}
	if result.Err() != nil {
		log.Printf("query parsing error: %v\n", result.Err().Error())
	}
}

func QueryInfluxDB() {
	url := "http://localhost:2333"
	token := "cUDigADLBUvQHTabhzjBjL_YM1MVofBUUSZx_-uwKy8mR4S_Eqjt6myugvj3ryOfRUBHOGnlyCbTkKbNGVt1rQ=="
	org := "pingcap"
	client := influxdb.NewClient(url, token)
	queryAPI := client.QueryAPI(org)
	queryInfluxDB(queryAPI, 0, 60*60, 1)
}

func QueryTiDB() {
	dsn := "root:@tcp(127.0.0.1:4000)/test?charset=utf8"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	defer db.Close()

	queryTiDB(db, 0, 60, 0)
}

func queryTiDB(db *sql.DB, startTs, endTs, instance_id int) {
	sql := `
	SELECT *
	FROM (SELECT *, RANK() OVER (PARTITION BY topsql.time_window ORDER BY topsql.cpu_time_sum DESC ) AS rk
		FROM (SELECT instance_id, sql_digest, floor(timestamp / 60) AS time_window, sum(cpu_time_ms) AS cpu_time_sum
			FROM cpu_time
			WHERE timestamp >= %d
				AND timestamp < %d
				AND instance_id = %d
			GROUP BY time_window, sql_digest) topsql) sql_ranked
	WHERE sql_ranked.rk <= 5
	`
	sql = fmt.Sprintf(sql, startTs, endTs, instance_id)
	rows, err := db.Query(sql)
	if err != nil {
		log.Fatalf("failed to query TiDB, %v", err)
	}
	defer rows.Close()
	log.Println("cpu_time: cpu_time_count, minute")
	for rows.Next() {
		var cpu_time_count int
		var minute int
		if err := rows.Scan(&cpu_time_count, &minute); err != nil {
			log.Fatalf("failed to iterate rows, %v", err)
		}
		log.Printf("row: %d, %d\n", cpu_time_count, minute)
	}
}

func writeCPUTimeRecordsTiDB(recordsChan chan *tipb.CPUTimeRecord, dsn string) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	defer db.Close()

	var sqlBuf strings.Builder
	sqlBuf.WriteString("INSERT INTO cpu_time (sql_digest, plan_digest, timestamp, cpu_time_ms, instance_id) VALUES ")
	for i := 0; i < 59; i++ {
		sqlBuf.WriteString("(?, ?, ?, ?, ?),")
	}
	sqlBuf.WriteString("(?, ?, ?, ?, ?)")
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
			sep := strings.Split(string(records.SqlDigest), "_")
			instanceID, err := strconv.Atoi(sep[0][1:])
			if err != nil {
				log.Fatalf("failed to parse instance ID in SQL digest '%s', %v", records.SqlDigest, err)
			}
			values = append(values, string(records.SqlDigest), string(records.PlanDigest), ts, cpuTimeMs, instanceID)
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			log.Fatalf("failed to execute SQL statement, %v", err)
		}
		// log.Printf("execute SQL result: %v", result)
	}
}

func writeSQLMetaTiDB(sqlMetaChan chan *tipb.SQLMeta, dsn string) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	defer db.Close()

	sql := "INSERT INTO sql_meta (sql_digest, normalized_sql) VALUES (?, ?)"
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Fatalf("failed to prepare for SQL statement, %v", err)
	}

	for {
		sqlMeta := <-sqlMetaChan
		values := []interface{}{sqlMeta.SqlDigest, sqlMeta.NormalizedSql}
		_, err = stmt.Exec(values...)
		if err != nil {
			log.Fatalf("failed to insert sql meta, %v", err)
		}
	}
}

func writePlanMetaTiDB(planMetaChan chan *tipb.PlanMeta, dsn string) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	defer db.Close()

	sql := "INSERT INTO plan_meta (plan_digest, normalized_plan) VALUES (?, ?)"
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Fatalf("failed to prepare for SQL statement, %v", err)
	}

	for {
		planMeta := <-planMetaChan
		values := []interface{}{planMeta.PlanDigest, planMeta.NormalizedPlan}
		_, err = stmt.Exec(values...)
		if err != nil {
			log.Fatalf("failed to insert plan meta, %v", err)
		}
	}
}

func WriteTiDB() {
	recordChan := make(chan *tipb.CPUTimeRecord, writeWorkerNum*2)
	sqlMetaChan := make(chan *tipb.SQLMeta, writeWorkerNum*2)
	planMetaChan := make(chan *tipb.PlanMeta, writeWorkerNum*2)
	go GenerateCPUTimeRecords(recordChan)
	go GenerateSQLMeta(sqlMetaChan)
	go GeneratePlanMeta(planMetaChan)

	dsn := "root:@tcp(127.0.0.1:4000)/test?charset=utf8"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS cpu_time (
		id BIGINT AUTO_RANDOM NOT NULL,
		sql_digest VARBINARY(32) NOT NULL,
		plan_digest VARBINARY(32),
		timestamp INTEGER NOT NULL,
		cpu_time_ms INTEGER NOT NULL,
		instance_id INTEGER NOT NULL,
		PRIMARY KEY (id)
	);`); err != nil {
		log.Fatalf("create table err: %v\n", err)
	}
	if _, err = db.Exec("ALTER TABLE cpu_time SET TIFLASH REPLICA 1;"); err != nil {
		log.Fatalf("set tiflash replica err: %v\n", err)
	}
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS sql_meta (
		id BIGINT AUTO_RANDOM NOT NULL,
		sql_digest VARBINARY(32) NOT NULL,
		normalized_sql LONGTEXT NOT NULL,
		PRIMARY KEY (id)
	);`); err != nil {
		log.Fatalf("create table err: %v\n", err)
	}
	if _, err = db.Exec("ALTER TABLE sql_meta SET TIFLASH REPLICA 1;"); err != nil {
		log.Fatalf("set tiflash replica err: %v\n", err)
	}
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS plan_meta (
		id BIGINT AUTO_RANDOM NOT NULL,
		plan_digest VARBINARY(32) NOT NULL,
		normalized_plan LONGTEXT NOT NULL,
		PRIMARY KEY (id)
	);`); err != nil {
		log.Fatalf("create table err: %v\n", err)
	}
	if _, err = db.Exec("ALTER TABLE plan_meta SET TIFLASH REPLICA 1;"); err != nil {
		log.Fatalf("set tiflash replica err: %v\n", err)
	}
	for i := 0; i < writeWorkerNum; i++ {
		go writeCPUTimeRecordsTiDB(recordChan, dsn)
		go writeSQLMetaTiDB(sqlMetaChan, dsn)
		go writePlanMetaTiDB(planMetaChan, dsn)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
