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
	"strings"
	"sync"
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

type planBinaryDecodeFunc func(string) (string, error)

// TopSQLDataPoint represents the cumulative SQL plan CPU time in current minute window
type TopSQLDataPoint struct {
	CPUTimeMsList []uint32
	TimestampList []uint64
}

type planRegisterJob struct {
	planDigest     string
	planNormalized string
}

type TopSQL struct {
	// // calling this can take a while, so should not block critical paths
	// planBinaryDecoder planBinaryDecodeFunc

	// topSQLCache is an LFU cache, which stores top sql records in the next minute from the last send point
	topSQLCache *LFUCache

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are normalized SQL strings
	normalizedSQLMap   map[string]string
	normalizedSQLMutex sync.RWMutex

	// normalizedPlanMap is a plan version of normalizedSQLMap
	// this should only be set from the dedicated worker
	normalizedPlanMap map[string]string

	planRegisterChan chan *planRegisterJob

	// current tidb-server instance ID
	instanceID string
}

func encodeCacheKey(sqlDigest, planDigest string) string {
	return sqlDigest + "-" + planDigest
}

func decodeCacheKey(key string) (string, string) {
	split := strings.Split(key, "-")
	sqlDigest := split[0]
	PlanDigest := split[1]
	return sqlDigest, PlanDigest
}

// topSQLEvictFuncGenerator is a closure wrapper, which returns the EvictedFunc used on LFU cache eviction
// the closure variables are normalizedSQLMap and normalizedPlanMap
func topSQLEvictFuncGenerator(normalizedSQLMutex *sync.RWMutex, normalizedSQLMap map[string]string, normalizedPlanMap map[string]string) EvictedHookFunc {
	topSQLEvictFunc := func(key interface{}, value interface{}) {
		keyStr, ok := key.(string)
		if !ok {
			fmt.Printf("failed to convert key [%v] to string", key)
			return
		}
		sqlDigest, planDigest := decodeCacheKey(keyStr)
		normalizedSQLMutex.Lock()
		delete(normalizedSQLMap, sqlDigest)
		normalizedSQLMutex.Unlock()
		delete(normalizedPlanMap, planDigest)
	}
	return topSQLEvictFunc
}

// NewTopSQL creates a new TopSQL struct
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// maxSQLNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
// TODO: grpc stream
func NewTopSQL(
	planBinaryDecoder planBinaryDecodeFunc,
	maxSQLNum int,
	instanceID string,
) *TopSQL {
	normalizedSQLMap := make(map[string]string)
	normalizedPlanMap := make(map[string]string)
	planRegisterChan := make(chan *planRegisterJob, 10)
	topSQLCache := NewLFUCache(maxSQLNum)

	go registerNormalizedPlanWorker(normalizedPlanMap, planBinaryDecoder, planRegisterChan)

	ts := &TopSQL{
		topSQLCache:       topSQLCache,
		normalizedSQLMap:  normalizedSQLMap,
		normalizedPlanMap: normalizedPlanMap,
		planRegisterChan:  planRegisterChan,
		instanceID:        instanceID,
	}
	ts.topSQLCache.EvictedHook = topSQLEvictFuncGenerator(&ts.normalizedSQLMutex, normalizedSQLMap, normalizedPlanMap)
	return ts
}

// Collect collects a batch of cpu time records at timestamp.
// timestamp is the unix timestamp in second.
//
// This function is expected to return immediately in a non-blocking behavior.
// TODO: benchmark test concurrent performance
func (ts *TopSQL) Collect(timestamp uint64, records []TopSQLRecord) {
	for _, record := range records {
		encodedKey := encodeCacheKey(record.SQLDigest, record.PlanDigest)
		value := ts.topSQLCache.Get(encodedKey)
		if value == nil {
			// not found, we should add a new entry for this SQL plan
			entry := &TopSQLDataPoint{
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
			// When gcache.Cache.serializeFunc is nil, we don't need to check error from `Set()`
			ts.topSQLCache.Set(encodedKey, entry)
		} else {
			// SQL plan entry exists, we should update it's CPU time and timestamp list
			entry, _ := value.(*TopSQLDataPoint)
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		// Finally, we should add the CPUTimeMS into the frequency of the SQL plan
		ts.topSQLCache.IncrementFrequency(encodedKey, uint64(record.CPUTimeMs))
	}
}

// RegisterNormalizedSQL registers a normalized sql string to a sql digest, while the former can be of >1M long.
// The in-memory space for registered normalized sql are limited by TopSQL.normalizedSQLCapacity.
//
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
// TODO: benchmark test concurrent performance
func (ts *TopSQL) RegisterNormalizedSQL(sqlDigest string, sqlNormalized string) {
	ts.normalizedSQLMutex.RLock()
	_, exist := ts.normalizedSQLMap[sqlDigest]
	ts.normalizedSQLMutex.RUnlock()
	if !exist {
		ts.normalizedSQLMutex.Lock()
		ts.normalizedSQLMap[sqlDigest] = sqlNormalized
		ts.normalizedSQLMutex.Unlock()
	}
}

// RegisterNormalizedPlan is like RegisterNormalizedSQL, but for normalized plan strings.
// TODO: benchmark test concurrent performance
func (ts *TopSQL) RegisterNormalizedPlan(planDigest string, planNormalized string) {
	ts.planRegisterChan <- &planRegisterJob{
		planDigest:     planDigest,
		planNormalized: planNormalized,
	}
}

// this should be the only place where the normalizedPlanMap is set
func registerNormalizedPlanWorker(normalizedPlanMap map[string]string, planDecoder planBinaryDecodeFunc, jobChan chan *planRegisterJob) {
	// NOTE: if use multiple worker goroutine, we should make sure that access to the digest map is thread-safe
	for {
		job := <-jobChan
		if _, exist := normalizedPlanMap[job.planDigest]; exist {
			continue
		}
		planDecoded, err := planDecoder(job.planNormalized)
		if err != nil {
			fmt.Printf("decode plan failed: %v\n", err)
			continue
		}
		normalizedPlanMap[job.planDigest] = planDecoded
	}
}