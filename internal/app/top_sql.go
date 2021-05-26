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
type digestMap map[string]string

// TopSQLCacheEntry represents the cumulative SQL plan CPU time in current minute window
type TopSQLCacheEntry struct {
	CPUTimeMsList []uint32
	TimestampList []uint64
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
func topSQLEvictFuncGenerator(normalizedSQLMap digestMap, normalizedPlanMap digestMap) EvictedHookFunc {
	topSQLEvictFunc := func(key interface{}, value interface{}) {
		keyStr, ok := key.(string)
		if !ok {
			fmt.Printf("failed to convert key [%v] to string", key)
			return
		}
		sqlDigest, planDigest := decodeCacheKey(keyStr)
		delete(normalizedSQLMap, sqlDigest)
		delete(normalizedPlanMap, planDigest)
	}
	return topSQLEvictFunc
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
	normalizedSQLMap digestMap

	// normalizedPlanMap is a plan version of normalizedSQLMap
	// this should only be set from the dedicated worker
	normalizedPlanMap digestMap

	planRegisterChan chan *planRegisterJob

	// current tidb-server instance ID
	instanceID string
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
) (*TopSQL, error) {
	normalizedSQLMap := make(digestMap)
	normalizedPlanMap := make(digestMap)
	planRegisterChan := make(chan *planRegisterJob, 10)
	topSQLCache := NewLFUCache(maxSQLNum, topSQLEvictFuncGenerator(normalizedSQLMap, normalizedPlanMap))

	go registerNormalizedPlanWorker(normalizedPlanMap, planBinaryDecoder, planRegisterChan)

	return &TopSQL{
		topSQLCache:       topSQLCache,
		normalizedSQLMap:  normalizedSQLMap,
		normalizedPlanMap: normalizedPlanMap,
		planRegisterChan:  planRegisterChan,
		instanceID:        instanceID,
	}, nil
}

// Collect collects a batch of cpu time records at timestamp.
// timestamp is the unix timestamp in second.
//
// This function is expected to return immediately in a non-blocking behavior.
// TODO: benchmark test concurrent performance
// TODO: use cpu time as frequency
func (ts *TopSQL) Collect(timestamp uint64, records []TopSQLRecord) {
	for _, record := range records {
		encodedKey := encodeCacheKey(record.SQLDigest, record.PlanDigest)
		value := ts.topSQLCache.Get(encodedKey)
		if value == nil {
			// not found, we should add a new entry for this SQL plan
			entry := &TopSQLCacheEntry{
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
			// When gcache.Cache.serializeFunc is nil, we don't need to check error from `Set()`
			ts.topSQLCache.Set(encodedKey, entry)
			// We need to increment frequency by calling `Get()`
			ts.topSQLCache.Get(encodedKey)
		} else {
			// SQL plan entry exists, we should update it's CPU time and timestamp list
			// Note that the entry's frequency is already incremented by the former `Get()` call
			entry, _ := value.(TopSQLCacheEntry)
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
	}
}

// RegisterNormalizedSQL registers a normalized sql string to a sql digest, while the former can be of >1M long.
// The in-memory space for registered normalized sql are limited by TopSQL.normalizedSQLCapacity.
//
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
// TODO: benchmark test concurrent performance
func (ts *TopSQL) RegisterNormalizedSQL(sqlDigest string, sqlNormalized string) {
	if _, exist := ts.normalizedSQLMap[sqlDigest]; !exist {
		ts.normalizedSQLMap[sqlDigest] = sqlNormalized
	}
}

// RegisterNormalizedPlan is like RegisterNormalizedSQL, but for normalized plan strings.
// TODO: benchmark test concurrent performance
func (ts *TopSQL) RegisterNormalizedPlan(planDigest string, planNormalized string) {
	if _, exist := ts.normalizedPlanMap[planDigest]; !exist {
		ts.planRegisterChan <- &planRegisterJob{
			planDigest:     planDigest,
			planNormalized: planNormalized,
		}
	}
}

// this should be the only place where the normalizedPlanMap is set
func registerNormalizedPlanWorker(normalizedPlanMap digestMap, planDecoder planBinaryDecodeFunc, jobChan chan *planRegisterJob) {
	for {
		job := <-jobChan
		planDecoded, err := planDecoder(job.planNormalized)
		if err != nil {
			fmt.Printf("decode plan failed: %v\n", err)
			continue
		}
		normalizedPlanMap[job.planDigest] = planDecoded
	}
}
