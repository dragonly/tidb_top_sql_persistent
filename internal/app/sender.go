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
	"time"

	"github.com/pingcap/tipb/go-tipb"
)

type Store interface {
	WriteCPUTimeRecord(*tipb.CPUTimeRecord)
	WriteSQLMeta(*tipb.SQLMeta)
	WritePlanMeta(*tipb.PlanMeta)
}

// MemStore is for testing purpose
type MemStore struct {
	cpuTimeRecordList []*tipb.CPUTimeRecord
	sqlMetaList       []*tipb.SQLMeta
	planMeta          []*tipb.PlanMeta
}

func NewMemStore() *MemStore {
	return &MemStore{}
}

func (s *MemStore) WriteCPUTimeRecord(records *tipb.CPUTimeRecord) {
	s.cpuTimeRecordList = append(s.cpuTimeRecordList, records)
}

func (s *MemStore) WriteSQLMeta(meta *tipb.SQLMeta) {
	s.sqlMetaList = append(s.sqlMetaList, meta)
}

func (s *MemStore) WritePlanMeta(meta *tipb.PlanMeta) {
	s.planMeta = append(s.planMeta, meta)
}

type Sender struct {
	prefetcher *Prefetcher
	store      Store
}

func NewSender(prefetcher *Prefetcher, store Store) *Sender {
	return &Sender{
		prefetcher: prefetcher,
		store:      store,
	}
}

func (s *Sender) start() {
	for {
		s.sendNext()
		time.Sleep(time.Millisecond)
	}
}

// sendNext sends the next batch of records from WAL
func (s *Sender) sendNext() {
	// TODO: send batch
	if record := s.prefetcher.readOneCPUTimeRecordOrNil(); record != nil {
		s.store.WriteCPUTimeRecord(record)
		s.prefetcher.RemoveOneCPUTimeRecordAtFront()
	}
	if sqlMeta := s.prefetcher.readOneSQLMetaOrNil(); sqlMeta != nil {
		s.store.WriteSQLMeta(sqlMeta)
		s.prefetcher.RemoveOneSQLMetaAtFront()
	}
	if planMeta := s.prefetcher.readOnePlanMetaOrNil(); planMeta != nil {
		s.store.WritePlanMeta(planMeta)
		s.prefetcher.RemoveOnePlanMetaAtFront()
	}
}
