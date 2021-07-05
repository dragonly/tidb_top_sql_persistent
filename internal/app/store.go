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
	"database/sql"
	"log"
	"strings"

	"github.com/pingcap/tipb/go-tipb"
)

type Store interface {
	WriteCPUTimeRecord(record *tipb.CPUTimeRecord, instanceID string) error
	WriteSQLMeta(*tipb.SQLMeta) error
	WritePlanMeta(*tipb.PlanMeta) error
}

// MemStore is for testing purpose
type MemStore struct {
	cpuTimeRecordList []*tipb.CPUTimeRecord
	sqlMetaList       []*tipb.SQLMeta
	planMetaList      []*tipb.PlanMeta
}

func NewMemStore() *MemStore {
	return &MemStore{}
}

func (s *MemStore) WriteCPUTimeRecord(record *tipb.CPUTimeRecord, instanceID string) error {
	s.cpuTimeRecordList = append(s.cpuTimeRecordList, record)
	return nil
}

func (s *MemStore) WriteSQLMeta(meta *tipb.SQLMeta) error {
	s.sqlMetaList = append(s.sqlMetaList, meta)
	return nil
}

func (s *MemStore) WritePlanMeta(meta *tipb.PlanMeta) error {
	s.planMetaList = append(s.planMetaList, meta)
	return nil
}

// TiDBStore uses TiDB as the storage bakend
type TiDBStore struct {
	db *sql.DB
}

func NewTiDBStore(dsn string) *TiDBStore {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	return &TiDBStore{
		db: db,
	}
}

func (s *TiDBStore) WriteCPUTimeRecord(record *tipb.CPUTimeRecord, instanceID string) error {
	var sqlBuf strings.Builder
	sqlBuf.WriteString("INSERT INTO cpu_time (sql_digest, plan_digest, timestamp, cpu_time_ms, instance_id) VALUES ")
	for i := 0; i < len(record.RecordListTimestampSec)-1; i++ {
		sqlBuf.WriteString("(?, ?, ?, ?, ?),")
	}
	sqlBuf.WriteString("(?, ?, ?, ?, ?)")
	sqlStr := sqlBuf.String()
	values := make([]interface{}, 0, len(record.RecordListTimestampSec)*5)
	for i, ts := range record.RecordListTimestampSec {
		cpuTimeMs := record.RecordListCpuTimeMs[i]
		values = append(values, string(record.SqlDigest), string(record.PlanDigest), ts, cpuTimeMs, instanceID)
	}
	if _, err := s.db.Exec(sqlStr, values...); err != nil {
		return err
	}
	return nil
}

func (s *TiDBStore) WriteSQLMeta(meta *tipb.SQLMeta) error {
	sqlStr := "INSERT INTO sql_meta (sql_digest, normalized_sql) VALUES (?, ?)"
	values := []interface{}{meta.SqlDigest, meta.NormalizedSql}
	if _, err := s.db.Exec(sqlStr, values...); err != nil {
		return err
	}
	return nil
}

func (s *TiDBStore) WritePlanMeta(meta *tipb.PlanMeta) error {
	sqlStr := "INSERT INTO plan_meta (plan_digest, normalized_plan) VALUES (?, ?)"
	values := []interface{}{meta.PlanDigest, meta.NormalizedPlan}
	if _, err := s.db.Exec(sqlStr, values...); err != nil {
		return err
	}
	return nil
}
