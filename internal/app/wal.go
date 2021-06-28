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
	"sync"

	"github.com/pingcap/tipb/go-tipb"
)

type Type uint8

const (
	TypeCPUTimeRecord Type = iota
	TypeSQLMeta
	TypePlanMeta
)

type ringBuffer struct {
	mu         sync.Mutex
	buf        []interface{}
	capacity   uint32
	start, end uint32
}

func (r *ringBuffer) Write(data interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	newEnd := (r.end + 1) % r.capacity
	if newEnd == r.start {
		return
	}
	r.buf[newEnd] = data
	r.end = newEnd
}

func (r *ringBuffer) ReadMulti(count uint32) []interface{} {
	return r.buf[r.start : r.start+count]
}

func (r *ringBuffer) RemoveFromStart(count uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.start = (r.start + count) % r.capacity
}

type WAL interface {
	WriteMulti([]interface{})
	ReadMulti(count uint32) []interface{}
	Acknowledge(count uint32)
}

var _ WAL = &MemWAL{}

type MemWAL struct {
	ring ringBuffer
}

func NewMemWAL() *MemWAL {
	return &MemWAL{
		ring: ringBuffer{
			buf:      make([]interface{}, 1024),
			capacity: 1024,
			start:    0,
			end:      0,
		},
	}
}

func (wal *MemWAL) WriteMulti(data []interface{}) {
	for _, d := range data {
		wal.ring.Write(d)
	}
}

func (wal *MemWAL) ReadMulti(count uint32) []interface{} {
	return wal.ring.ReadMulti(count)
}

func (wal *MemWAL) Acknowledge(count uint32) {
	wal.ring.RemoveFromStart(count)
}

type DiskWAL struct {
}

func NewDiskWAL() *DiskWAL {
	return &DiskWAL{}
}

// WriteMulti writes multiple encoded bytes to the current segment file
func (wal *DiskWAL) WriteMulti(data []interface{}) {

}

// ReadNext reads the next record from the current segment file
func (wal *DiskWAL) ReadNext() interface{} {
	return nil
}

// rollSegment closes the current segment file and open a new one, if necessary
func (wal *DiskWAL) rollSegment() error {
	return nil
}

func (wal *DiskWAL) encodeCPUTimeRecord(record *tipb.CPUTimeRecord) []byte {
	return nil
}

func (wal *DiskWAL) encodeSQLMeta(meta *tipb.SQLMeta) []byte {
	return nil
}

func (wal *DiskWAL) encodePlanMeta(meta *tipb.PlanMeta) []byte {
	return nil
}
