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

	"github.com/pingcap/tipb/go-tipb"
)

type Store interface {
	WriteCPURecordMulti()
	WriteSQLMetaMulti()
	WritePlanMetaMulti()
}

type TiDB struct {
}

func (tidb *TiDB) WriteCPURecordMulti() {
}

func (tidb *TiDB) WriteSQLMetaMulti() {
}

func (tidb *TiDB) WritePlanMetaMulti() {
}

type Sender struct {
	store         Store
	wal           WAL
	senderJobChan chan struct{}
}

func NewSender(wal WAL, senderJobChan chan struct{}) *Sender {
	return &Sender{
		wal:           wal,
		senderJobChan: senderJobChan,
	}
}

func (s *Sender) start() {
	for {
		s.sendNextBatch()
	}
}

// sendNextBatch sends the next batch of records from WAL
func (s *Sender) sendNextBatch() {
	// TODO: send batch
	next := s.wal.ReadMulti(1)[0]
	switch tp := next.(type) {
	case tipb.CPUTimeRecord:
		s.store.WriteCPURecordMulti()
	case tipb.SQLMeta:
		s.store.WriteSQLMetaMulti()
	case tipb.PlanMeta:
		s.store.WritePlanMetaMulti()
	default:
		panic(fmt.Sprintf("unexpected type %v", tp))
	}
}
