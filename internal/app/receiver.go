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

import "github.com/pingcap/tipb/go-tipb"

// TODO: add back pressure to ensure memory not growing to da moon
type Receiver struct {
	// the following lists are used to buffer received lists, which should be written to WAL in batch
	CPUTimeRecordList []*tipb.CPUTimeRecord
	SQLMetaList       []*tipb.SQLMeta
	PlanMetaList      []*tipb.PlanMeta
}

func NewReceiver() *Receiver {
	return &Receiver{
		CPUTimeRecordList: make([]*tipb.CPUTimeRecord, 0, 100),
		SQLMetaList:       make([]*tipb.SQLMeta, 0, 100),
		PlanMetaList:      make([]*tipb.PlanMeta, 0, 100),
	}
}

func (r *Receiver) receiveCPUTimeRecords(record *tipb.CPUTimeRecord) {
	r.CPUTimeRecordList = append(r.CPUTimeRecordList, record)
	// write to WAL in batch
}

func (r *Receiver) receiveSQLMeta(meta *tipb.SQLMeta) {
	r.SQLMetaList = append(r.SQLMetaList, meta)
	// write to WAL in batch
}

func (r *Receiver) receivePlanMeta(meta *tipb.PlanMeta) {
	r.PlanMetaList = append(r.PlanMetaList, meta)
	// write to WAL in batch
}
