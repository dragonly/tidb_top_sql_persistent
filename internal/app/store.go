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
	"fmt"
	"log"
	"strings"

	"github.com/pingcap/tipb/go-tipb"
)

type Store interface {
	WriteCPUTimeRecord(record *tipb.CPUTimeRecord, instanceID string) error
	WriteSQLMeta(*tipb.SQLMeta) error
	WritePlanMeta(*tipb.PlanMeta) error
	InitSchema() error
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

func (s *MemStore) InitSchema() error {
	return nil
}

// TiDBStore uses TiDB as the storage bakend
type TiDBStore struct {
	clusterID uint64
	db        *sql.DB
}

func NewTiDBStore(dsn string, clusterID uint64) *TiDBStore {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v\n", err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping db: %v\n", err)
	}
	return &TiDBStore{
		db:        db,
		clusterID: clusterID,
	}
}

func (s *TiDBStore) WriteCPUTimeRecord(record *tipb.CPUTimeRecord, instanceID string) error {
	var sqlBuf strings.Builder
	cpuTimeTable := fmt.Sprintf("tidb%d_cpu_time", s.clusterID)
	sqlBuf.WriteString(fmt.Sprintf("INSERT INTO %s (sql_digest, plan_digest, timestamp, cpu_time_ms, instance_id) VALUES ", cpuTimeTable))
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
	sqlMetaTable := fmt.Sprintf("tidb%d_sql_meta", s.clusterID)
	sqlStr := fmt.Sprintf("INSERT INTO %s (sql_digest, normalized_sql) VALUES (?, ?)", sqlMetaTable)
	values := []interface{}{meta.SqlDigest, meta.NormalizedSql}
	if _, err := s.db.Exec(sqlStr, values...); err != nil {
		return err
	}
	return nil
}

func (s *TiDBStore) WritePlanMeta(meta *tipb.PlanMeta) error {
	planMetaTable := fmt.Sprintf("tidb%d_plan_meta", s.clusterID)
	sqlStr := fmt.Sprintf("INSERT INTO %s (plan_digest, normalized_plan) VALUES (?, ?)", planMetaTable)
	values := []interface{}{meta.PlanDigest, meta.NormalizedPlan}
	if _, err := s.db.Exec(sqlStr, values...); err != nil {
		return err
	}
	return nil
}

func (s *TiDBStore) InitSchema() error {
	// TODO(dragonly): partition table maintenance
	cpuTimeTable := fmt.Sprintf("tidb%d_cpu_time", s.clusterID)
	sqlMetaTable := fmt.Sprintf("tidb%d_sql_meta", s.clusterID)
	planMetaTable := fmt.Sprintf("tidb%d_plan_meta", s.clusterID)
	if _, err := s.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		sql_digest VARBINARY(32) NOT NULL,
		plan_digest VARBINARY(32),
		timestamp INTEGER NOT NULL,
		cpu_time_ms INTEGER NOT NULL,
		instance_id VARCHAR(32) NOT NULL
		);`, cpuTimeTable)); err != nil {
		log.Printf("create table err: %v\n", err)
		return err
	}
	if _, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %s SET TIFLASH REPLICA 1;", cpuTimeTable)); err != nil {
		log.Printf("set tiflash replica err: %v\n", err)
		return err
	}

	if _, err := s.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT AUTO_RANDOM NOT NULL,
		sql_digest VARBINARY(32) NOT NULL,
		normalized_sql LONGTEXT NOT NULL,
		PRIMARY KEY (id),
		INDEX i_sql_digest (sql_digest),
		UNIQUE (sql_digest)
	);`, sqlMetaTable)); err != nil {
		log.Printf("create table err: %v\n", err)
		return err
	}

	if _, err := s.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT AUTO_RANDOM NOT NULL,
		plan_digest VARBINARY(32) NOT NULL,
		normalized_plan LONGTEXT NOT NULL,
		PRIMARY KEY (id),
		INDEX i_plan_digest (plan_digest),
		UNIQUE (plan_digest)
	);`, planMetaTable)); err != nil {
		log.Printf("create table err: %v\n", err)
		return err
	}

	return nil
}
