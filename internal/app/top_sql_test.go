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

func testPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
}

func initializeCache(t *testing.T, maxSQLNum int) *TopSQL {
	ts, err := NewTopSQL(testPlanBinaryDecoderFunc, maxSQLNum, "tidb-server")
	assert.NoError(t, err, "NewTopSQL should not return error")

	// register normalized sql
	for i := 0; i < maxSQLNum; i++ {
		key := "sqlDigest" + strconv.Itoa(i)
		value := "sqlNormalized" + strconv.Itoa(i)
		ts.RegisterNormalizedSQL(key, value)
	}
	// register normalized plan
	for i := 0; i < maxSQLNum; i++ {
		key := "planDigest" + strconv.Itoa(i)
		value := "planNormalized" + strconv.Itoa(i)
		ts.RegisterNormalizedPlan(key, value)
	}
	// collect
	var records []TopSQLRecord
	for i := 0; i < maxSQLNum; i++ {
		records = append(records, TopSQLRecord{
			SQLDigest:  "sqlDigest" + strconv.Itoa(i),
			PlanDigest: "planDigest" + strconv.Itoa(i),
			CPUTimeMs:  uint32(i + 1),
		})
	}
	ts.Collect(1, records)
	return ts
}

func TestTopSQL_CollectAndGet(t *testing.T) {
	maxSQLNum := 10
	ts := initializeCache(t, maxSQLNum)
	for i := 0; i < maxSQLNum; i++ {
		sqlDigest := "sqlDigest" + strconv.Itoa(i)
		planDigest := "planDigest" + strconv.Itoa(i)
		key := encodeCacheKey(sqlDigest, planDigest)
		entry := ts.topSQLCache.Get(key).(*TopSQLDataPoint)
		assert.Equal(t, uint32(i+1), entry.CPUTimeMsList[0])
		assert.Equal(t, uint64(1), entry.TimestampList[0])
	}
}

func TestTopSQL_CollectAndVerifyFrequency(t *testing.T) {
	maxSQLNum := 10
	ts := initializeCache(t, maxSQLNum)
	elem := ts.topSQLCache.freqList.Front()
	for i := 0; i < maxSQLNum; i++ {
		elem = elem.Next()
		entry := elem.Value.(*freqEntry)
		assert.Equal(t, uint64(i+1), entry.freq)
		assert.Equal(t, 1, len(entry.items))
		for item, _ := range entry.items {
			point := item.value.(*TopSQLDataPoint)
			assert.Equal(t, uint32(i+1), point.CPUTimeMsList[0])
			assert.Equal(t, uint64(1), point.TimestampList[0])
		}
	}
}
