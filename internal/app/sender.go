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

import "log"

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

func (s *Sender) Start() {
	// TODO: send in batch
	go func() {
		for {
			s.sendNextCPUTimeRecord()
		}
	}()
	go func() {
		for {
			s.sendNextSQLMeta()
		}
	}()
	go func() {
		for {
			s.sendNextPlanMeta()
		}
	}()
}

func (s *Sender) sendNextCPUTimeRecord() {
	record := s.prefetcher.ReadOneCPUTimeRecord()
	if err := s.store.WriteCPUTimeRecord(record, "fake_instance_id"); err != nil {
		log.Printf("executing sql failed: %v\n", err)
	}
}

func (s *Sender) sendNextSQLMeta() {
	meta := s.prefetcher.ReadOneSQLMetaOrNil()
	if err := s.store.WriteSQLMeta(meta); err != nil {
		log.Printf("executing sql failed: %v\n", err)
	}
}

func (s *Sender) sendNextPlanMeta() {
	meta := s.prefetcher.ReadOnePlanMetaOrNil()
	if err := s.store.WritePlanMeta(meta); err != nil {
		log.Printf("executing sql failed: %v\n", err)
	}
}
