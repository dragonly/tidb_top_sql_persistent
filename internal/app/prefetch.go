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
	"github.com/pingcap/tipb/go-tipb"
)

type prefetchBuffer struct {
	cpuTimeRecordChan chan *tipb.CPUTimeRecord
	sqlMetaChan       chan *tipb.SQLMeta
	planMetaChan      chan *tipb.PlanMeta
}

type Prefetcher struct {
	wal WAL // WAL is currently useless
	buf prefetchBuffer
}

type PrefetcherConfig struct {
	CPUTimeRecordCount uint32
	SQLMetaCount       uint32
	PlanMetaCount      uint32
}

// TODO: load from WAL
// Currently we write prefetch buffer directly from the receiver.
func NewPrefetcher(wal WAL, config PrefetcherConfig) *Prefetcher {
	return &Prefetcher{
		wal: wal,
		buf: prefetchBuffer{
			cpuTimeRecordChan: make(chan *tipb.CPUTimeRecord, config.CPUTimeRecordCount),
			sqlMetaChan:       make(chan *tipb.SQLMeta, config.SQLMetaCount),
			planMetaChan:      make(chan *tipb.PlanMeta, config.PlanMetaCount),
		},
	}
}

// start starts the prefetching goroutine
func (p *Prefetcher) start() {

}

func (p *Prefetcher) WriteOneCPUTimeRecordOrDrop(record *tipb.CPUTimeRecord) {
	select {
	case p.buf.cpuTimeRecordChan <- record:
	default:
	}
}
func (p *Prefetcher) WriteOneSQLMetaOrDrop(meta *tipb.SQLMeta) {
	select {
	case p.buf.sqlMetaChan <- meta:
	default:
	}
}
func (p *Prefetcher) WriteOnePlanMetaOrDrop(meta *tipb.PlanMeta) {
	select {
	case p.buf.planMetaChan <- meta:
	default:
	}
}

func (p *Prefetcher) ReadOneCPUTimeRecord() *tipb.CPUTimeRecord {
	return <-p.buf.cpuTimeRecordChan
}

func (p *Prefetcher) ReadOneSQLMetaOrNil() *tipb.SQLMeta {
	return <-p.buf.sqlMetaChan
}

func (p *Prefetcher) ReadOnePlanMetaOrNil() *tipb.PlanMeta {
	return <-p.buf.planMetaChan
}
