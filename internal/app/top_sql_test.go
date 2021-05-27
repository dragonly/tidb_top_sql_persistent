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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	maxSQLNum = 5000
)

func testPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
}

func populateCache(ts *TopSQL, begin, end int, timestamp uint64) {
	// register normalized sql
	for i := begin; i < end; i++ {
		key := "sqlDigest" + strconv.Itoa(i+1)
		value := "sqlNormalized" + strconv.Itoa(i+1)
		ts.RegisterNormalizedSQL(key, value)
	}
	// register normalized plan
	for i := begin; i < end; i++ {
		key := "planDigest" + strconv.Itoa(i+1)
		value := "planNormalized" + strconv.Itoa(i+1)
		ts.RegisterNormalizedPlan(key, value)
	}
	// collect
	var records []TopSQLRecord
	for i := begin; i < end; i++ {
		records = append(records, TopSQLRecord{
			SQLDigest:  "sqlDigest" + strconv.Itoa(i+1),
			PlanDigest: "planDigest" + strconv.Itoa(i+1),
			CPUTimeMs:  uint32(i + 1),
		})
	}
	ts.Collect(timestamp, records)
}

func initializeCache(t *testing.T, maxSQLNum int) *TopSQL {
	ts := NewTopSQL(testPlanBinaryDecoderFunc, maxSQLNum, "tidb-server")
	populateCache(ts, 0, maxSQLNum, 1)
	return ts
}

func TestTopSQL_CollectAndGet(t *testing.T) {
	ts := initializeCache(t, maxSQLNum)
	for i := 0; i < maxSQLNum; i++ {
		sqlDigest := "sqlDigest" + strconv.Itoa(i+1)
		planDigest := "planDigest" + strconv.Itoa(i+1)
		key := encodeCacheKey(sqlDigest, planDigest)
		entry := ts.topSQLCache.Get(key).(*TopSQLDataPoint)
		assert.Equal(t, uint32(i+1), entry.CPUTimeMsList[0])
		assert.Equal(t, uint64(1), entry.TimestampList[0])
	}
}

func TestTopSQL_CollectAndVerifyFrequency(t *testing.T) {
	ts := initializeCache(t, maxSQLNum)
	// traverse the frequency list, and check frequency/item content
	elem := ts.topSQLCache.freqList.Front()
	for i := 0; i < maxSQLNum; i++ {
		elem = elem.Next()
		entry := elem.Value.(*freqEntry)
		assert.Equal(t, uint64(i+1), entry.freq)
		assert.Equal(t, 1, len(entry.items))
		for item := range entry.items {
			point := item.value.(*TopSQLDataPoint)
			assert.Equal(t, uint32(i+1), point.CPUTimeMsList[0])
			assert.Equal(t, uint64(1), point.TimestampList[0])
		}
	}
}

func TestTopSQL_CollectAndEvict(t *testing.T) {
	ts := initializeCache(t, maxSQLNum)
	// Collect 10 records with timestamp 2 and sql plan digest from 6 to 15.
	// This should evict the first 5 sql plan digest, effectively 1-5
	populateCache(ts, maxSQLNum/2, maxSQLNum/2*3, 2)
	for i := 0; i < maxSQLNum/2; i++ {
		sqlDigest := "sqlDigest" + strconv.Itoa(i+1)
		planDigest := "planDigest" + strconv.Itoa(i+1)
		key := encodeCacheKey(sqlDigest, planDigest)
		_, exist := ts.topSQLCache.items[key]
		assert.Equal(t, false, exist, "cache key '%' should be evicted", key)
		_, exist = ts.normalizedSQLMap[sqlDigest]
		assert.Equal(t, false, exist, "normalized SQL with digest '%s' should be evicted", sqlDigest)
		_, exist = ts.normalizedPlanMap[planDigest]
		assert.Equal(t, false, exist, "normalized plan with digest '%s' should be evicted", planDigest)
	}
	// Because CPU time is populated as i+1, we should expect digest 6-10 to have CPU time 12, 14, 16, 18, 20
	// and digest 11-15 to have CPU time 11-15.
	for i := maxSQLNum / 2; i < maxSQLNum/2*3; i++ {
		sqlDigest := "sqlDigest" + strconv.Itoa(i+1)
		planDigest := "planDigest" + strconv.Itoa(i+1)
		key := encodeCacheKey(sqlDigest, planDigest)
		item, exist := ts.topSQLCache.items[key]
		assert.Equal(t, true, exist, "cache key '%s' should exist", exist)
		entry := item.freqElement.Value.(*freqEntry)
		if i < maxSQLNum {
			assert.Equal(t, uint64((i+1)*2), entry.freq)
		} else {
			assert.Equal(t, uint64(i+1), entry.freq)
		}
	}
}
