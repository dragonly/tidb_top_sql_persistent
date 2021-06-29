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
	wal        WAL
	prefetcher *Prefetcher
}

func NewReceiver(wal WAL, prefetcher *Prefetcher) *Receiver {
	return &Receiver{
		wal:        wal,
		prefetcher: prefetcher,
	}
}

func (r *Receiver) receiveCPUTimeRecords(record *tipb.CPUTimeRecord) {
	// TODO: write to WAL in batch
	r.wal.WriteMulti([]interface{}{record})
	r.prefetcher.WriteToBuffer(record)
}

func (r *Receiver) receiveSQLMeta(meta *tipb.SQLMeta) {
	// TODO: write to WAL in batch
	r.wal.WriteMulti([]interface{}{meta})
	r.prefetcher.WriteToBuffer(meta)
}

func (r *Receiver) receivePlanMeta(meta *tipb.PlanMeta) {
	// TODO: write to WAL in batch
	r.wal.WriteMulti([]interface{}{meta})
	r.prefetcher.WriteToBuffer(meta)
}
