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
	"sync"

	"github.com/pingcap/tipb/go-tipb"
)

type prefetchBuffer struct {
	mu                sync.RWMutex
	cpuTimeRecordList []*tipb.CPUTimeRecord
	sqlMetaList       []*tipb.SQLMeta
	planMetaList      []*tipb.PlanMeta
	// capacity is not a hard limit. If the last read message from WAL is big enough, the buf could be larger
	// than capacity, but no more message is read from WAL if buffer size is larger than capacity. This is done
	// to avoid wasting IO as possible.
	capacity uint32
	size     uint32
}

type Prefetcher struct {
	wal WAL
	buf prefetchBuffer
}

// TODO: load from WAL
// Currently we write prefetch buffer directly from the receiver.
func NewPrefetcher(wal WAL, capacity uint32) *Prefetcher {
	return &Prefetcher{
		wal: wal,
		buf: prefetchBuffer{
			cpuTimeRecordList: make([]*tipb.CPUTimeRecord, 0, 10),
			sqlMetaList:       make([]*tipb.SQLMeta, 0, 10),
			planMetaList:      make([]*tipb.PlanMeta, 0, 10),
			capacity:          capacity,
			size:              0,
		},
	}
}

// start starts the prefetching goroutine
func (p *Prefetcher) start() {

}

func (p *Prefetcher) WriteToBuffer(data interface{}) {
	p.buf.mu.Lock()
	defer p.buf.mu.Unlock()
	bufSize := p.buf.size
	bufCap := p.buf.capacity
	if bufSize >= bufCap {
		return
	}
	switch tp := data.(type) {
	case *tipb.CPUTimeRecord:
		record := data.(*tipb.CPUTimeRecord)
		p.buf.cpuTimeRecordList = append(p.buf.cpuTimeRecordList, record)
	case *tipb.SQLMeta:
		meta := data.(*tipb.SQLMeta)
		p.buf.sqlMetaList = append(p.buf.sqlMetaList, meta)
	case *tipb.PlanMeta:
		meta := data.(*tipb.PlanMeta)
		p.buf.planMetaList = append(p.buf.planMetaList, meta)
	default:
		panic(fmt.Sprintf("unexpected type %v", tp))
	}
	dataSize := estimateSize(data)
	p.buf.size = bufSize + dataSize
}

func (p *Prefetcher) readOneCPUTimeRecordOrNil() *tipb.CPUTimeRecord {
	if len(p.buf.cpuTimeRecordList) == 0 {
		return nil
	}
	return p.buf.cpuTimeRecordList[0]
}

func (p *Prefetcher) readOneSQLMetaOrNil() *tipb.SQLMeta {
	if len(p.buf.sqlMetaList) == 0 {
		return nil
	}
	return p.buf.sqlMetaList[0]
}

func (p *Prefetcher) readOnePlanMetaOrNil() *tipb.PlanMeta {
	if len(p.buf.planMetaList) == 0 {
		return nil
	}
	return p.buf.planMetaList[0]
}

func (p *Prefetcher) RemoveOneCPUTimeRecordAtFront() {
	p.buf.cpuTimeRecordList[0] = p.buf.cpuTimeRecordList[len(p.buf.cpuTimeRecordList)-1]
	p.buf.cpuTimeRecordList = p.buf.cpuTimeRecordList[:len(p.buf.cpuTimeRecordList)-1]
}

func (p *Prefetcher) RemoveOneSQLMetaAtFront() {
	p.buf.sqlMetaList[0] = p.buf.sqlMetaList[len(p.buf.sqlMetaList)-1]
	p.buf.sqlMetaList = p.buf.sqlMetaList[:len(p.buf.sqlMetaList)-1]
}

func (p *Prefetcher) RemoveOnePlanMetaAtFront() {
	p.buf.planMetaList[0] = p.buf.planMetaList[len(p.buf.planMetaList)-1]
	p.buf.planMetaList = p.buf.planMetaList[:len(p.buf.planMetaList)-1]
}

func estimateSize(data interface{}) uint32 {
	switch tp := data.(type) {
	case *tipb.CPUTimeRecord:
		record := data.(*tipb.CPUTimeRecord)
		size := len(record.SqlDigest)
		size += len(record.PlanDigest)
		size += 1 // record.IsInternalSql
		size += (8 /*timestamp list*/ + 4 /*cpu time ms list*/) * len(record.TimestampList)
		return uint32(size)
	case *tipb.SQLMeta:
		meta := data.(*tipb.SQLMeta)
		return uint32(len(meta.SqlDigest) + len(meta.NormalizedSql))
	case *tipb.PlanMeta:
		meta := data.(*tipb.PlanMeta)
		return uint32(len(meta.PlanDigest) + len(meta.NormalizedPlan))
	default:
		panic(fmt.Sprintf("unexpected type %v", tp))
	}
}
