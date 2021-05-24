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
	"github.com/bluele/gcache"
)

// TopSQLRecord represents a single record of how much cpu time a sql plan consumes in one second.
//
// PlanDigest can be empty, because:
// 1. some sql statements has no plan, like `COMMIT`
// 2. when a sql statement is being compiled, there's no plan yet
type TopSQLRecord struct {
	SQLDigest  string
	PlanDigest string
	CPUTimeMs  uint32
}

type planBinaryDecoderFunc func(string) (string, error)

type TopSQL struct {
	// calling this can take a while, so should not block critical paths
	planBinaryDecoder   planBinaryDecoderFunc
	normalizedSQLCache  gcache.Cache
	normalizedPlanCache gcache.Cache
}

// NewTopSQL creates a new TopSQL struct
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// maxSQLNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewTopSQL(
	planBinaryDecoder planBinaryDecoderFunc,
	maxSQLNum int,
) (*TopSQL, error) {
	normalizedSQLCache := gcache.New(maxSQLNum).LFU().Build()
	normalizedPlanCache := gcache.New(maxSQLNum).LFU().Build()
	return &TopSQL{
		planBinaryDecoder:   planBinaryDecoder,
		normalizedSQLCache:  normalizedSQLCache,
		normalizedPlanCache: normalizedPlanCache,
	}, nil
}

// Collect collects a batch of cpu time records at timestamp.
// timestamp is the unix timestamp in second.
//
// This function is expected to return immediately in a non-blocking behavior.
func (ts *TopSQL) Collect(timestamp uint64, records []TopSQLRecord) {
	// TODO
}

// RegisterNormalizedSQL registers a normalized sql string to a sql digest, while the former can be of >1M long.
// The in-memory space for registered normalized sql are limited by TopSQL.normalizedSQLCapacity.
//
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
func (ts *TopSQL) RegisterNormalizedSQL(sqlDigest string, sqlNormalized string) {
	ts.normalizedSQLCache.Set(sqlDigest, sqlNormalized)
}

// RegisterNormalizedPlan is like RegisterNormalizedSQL, but for normalized plan strings.
func (ts *TopSQL) RegisterNormalizedPlan(planDigest string, planNormalized []byte) {
	// TODO
}
