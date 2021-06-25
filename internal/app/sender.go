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
}

type TiDB struct {
}

func (tidb *TiDB) WriteCPURecordMulti() {

}

type Sender struct {
	store Store
	wal   WAL
}

func NewSender() *Sender {
	return &Sender{}
}

// sendNext sends the next record from WAL
func (s *Sender) sendNext() {
	next := s.wal.ReadNext()
	switch t := next.(type) {
	case tipb.CPUTimeRecord:

	case tipb.SQLMeta:
	case tipb.PlanMeta:
	default:
		panic(fmt.Sprintf("unexpected type %v", t))
	}
}
