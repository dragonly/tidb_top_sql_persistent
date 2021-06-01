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
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	pb "github.com/dragonly/tidb_top_sql_persistent/internal/app/protobuf"
	"github.com/wangjohn/quickselect"
	"google.golang.org/grpc"
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

// TopSQLDataPoints represents the cumulative SQL plan CPU time in current minute window
type TopSQLDataPoints struct {
	CPUTimeMsList  []uint32
	TimestampList  []uint64
	CPUTimeMsTotal uint64
}

type DigestAndCPUTime struct {
	Key            string
	CPUTimeMsTotal uint64
}
type DigestAndCPUTimeSlice []DigestAndCPUTime

func (t DigestAndCPUTimeSlice) Len() int {
	return len(t)
}

// We need find the kth largest value, so here should use >
func (t DigestAndCPUTimeSlice) Less(i, j int) bool {
	return t[i].CPUTimeMsTotal > t[j].CPUTimeMsTotal
}
func (t DigestAndCPUTimeSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type planRegisterJob struct {
	planDigest     string
	normalizedPlan string
}

type TopSQLCollector struct {
	// // calling this can take a while, so should not block critical paths
	// planBinaryDecoder planBinaryDecodeFunc
	mu sync.RWMutex

	// topSQLCache is an LFU cache, which stores top sql records in the next minute from the last send point
	topSQLCache *LFUCache
	topSQLMap   map[string]*TopSQLDataPoints
	maxSQLNum   int

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are normalized SQL strings
	normalizedSQLMap map[string]string

	// normalizedPlanMap is a plan version of normalizedSQLMap
	// this should only be set from the dedicated worker
	normalizedPlanMap map[string]string

	planRegisterChan chan *planRegisterJob

	// current tidb-server instance ID
	instanceID string

	agentGRPCAddress string

	quit chan struct{}
}

type TopSQLCollectorConfig struct {
	PlanBinaryDecoder   planBinaryDecodeFunc
	MaxSQLNum           int
	SendToAgentInterval time.Duration
	AgentGRPCAddress    string
	InstanceID          string
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
func topSQLEvictFuncGenerator(
	mu *sync.RWMutex,
	normalizedSQLMap map[string]string,
	normalizedPlanMap map[string]string) EvictedHookFunc {
	topSQLEvictFunc := func(key interface{}, value interface{}) {
		keyStr, ok := key.(string)
		if !ok {
			fmt.Printf("failed to convert key [%v] to string", key)
			return
		}
		sqlDigest, planDigest := decodeCacheKey(keyStr)
		mu.Lock()
		delete(normalizedSQLMap, sqlDigest)
		delete(normalizedPlanMap, planDigest)
		mu.Unlock()
	}
	return topSQLEvictFunc
}

func newAgentClient(addr string, sendingTimeout time.Duration) (*grpc.ClientConn, pb.TopSQLAgent_CollectCPUTimeClient, error) {
	dialCtx, _ := context.WithTimeout(context.TODO(), time.Second)
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := pb.NewTopSQLAgentClient(conn)
	ctx, _ := context.WithTimeout(dialCtx, sendingTimeout)
	stream, err := client.CollectCPUTime(ctx)
	if err != nil {
		return nil, nil, err
	}
	return conn, stream, nil
}

// NewTopSQLCollector creates a new TopSQL struct
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// maxSQLNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
// TODO: grpc stream
func NewTopSQLCollector(config *TopSQLCollectorConfig) *TopSQLCollector {
	normalizedSQLMap := make(map[string]string)
	normalizedPlanMap := make(map[string]string)
	planRegisterChan := make(chan *planRegisterJob, 10)
	topSQLCache := NewLFUCache(config.MaxSQLNum)
	topSQLMap := make(map[string]*TopSQLDataPoints)

	ts := &TopSQLCollector{
		topSQLCache:       topSQLCache,
		topSQLMap:         topSQLMap,
		maxSQLNum:         config.MaxSQLNum,
		normalizedSQLMap:  normalizedSQLMap,
		normalizedPlanMap: normalizedPlanMap,
		planRegisterChan:  planRegisterChan,
		agentGRPCAddress:  config.AgentGRPCAddress,
		instanceID:        config.InstanceID,
		quit:              make(chan struct{}),
	}

	go ts.registerNormalizedPlanWorker(config.PlanBinaryDecoder)

	go ts.sendToAgentWorker(config.SendToAgentInterval)

	ts.topSQLCache.EvictedHook = topSQLEvictFuncGenerator(
		&ts.mu,
		normalizedSQLMap,
		normalizedPlanMap,
	)
	return ts
}

// Collect collects a batch of cpu time records at timestamp.
// timestamp is the unix timestamp in second.
//
// This function is expected to return immediately in a non-blocking behavior.
// TODO: benchmark test concurrent performance
func (ts *TopSQLCollector) Collect(timestamp uint64, records []TopSQLRecord) {
	for _, record := range records {
		encodedKey := encodeCacheKey(record.SQLDigest, record.PlanDigest)
		value := ts.topSQLCache.Get(encodedKey)
		if value == nil {
			// not found, we should add a new entry for this SQL plan
			entry := &TopSQLDataPoints{
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
			// When gcache.Cache.serializeFunc is nil, we don't need to check error from `Set()`
			ts.topSQLCache.Set(encodedKey, entry)
		} else {
			// SQL plan entry exists, we should update it's CPU time and timestamp list
			entry, _ := value.(*TopSQLDataPoints)
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		// Finally, we should add the CPUTimeMS into the frequency of the SQL plan
		ts.topSQLCache.IncrementFrequency(encodedKey, uint64(record.CPUTimeMs))
	}
}

// Collect1 uses a hashmap to store records in every minute, and evict every minute.
// This function can be run in parallel with snapshot, so we should protect the map operations with a mutex.
func (ts *TopSQLCollector) Collect1(timestamp uint64, records []TopSQLRecord) {
	for _, record := range records {
		encodedKey := encodeCacheKey(record.SQLDigest, record.PlanDigest)
		entry, exist := ts.topSQLMap[encodedKey]
		if !exist {
			entry = &TopSQLDataPoints{
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
			ts.topSQLMap[encodedKey] = entry
		} else {
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		entry.CPUTimeMsTotal += uint64(record.CPUTimeMs)
	}

	if len(ts.topSQLMap) <= ts.maxSQLNum {
		return
	}

	// find the max CPUTimeMsTotal that should be evicted
	digestCPUTimeList := make([]DigestAndCPUTime, len(ts.topSQLMap))
	{
		i := 0
		for key, value := range ts.topSQLMap {
			data := DigestAndCPUTime{
				Key:            key,
				CPUTimeMsTotal: value.CPUTimeMsTotal,
			}
			digestCPUTimeList[i] = data
			i++
		}
	}
	quickselect.QuickSelect(DigestAndCPUTimeSlice(digestCPUTimeList), ts.maxSQLNum)
	shouldEvictList := digestCPUTimeList[ts.maxSQLNum:]
	for _, evict := range shouldEvictList {
		delete(ts.topSQLMap, evict.Key)
		sqlDigest, planDigest := decodeCacheKey(evict.Key)
		ts.mu.Lock()
		delete(ts.normalizedSQLMap, sqlDigest)
		delete(ts.normalizedPlanMap, planDigest)
		ts.mu.Unlock()
	}
}

// RegisterNormalizedSQL registers a normalized sql string to a sql digest, while the former can be of >1M long.
// The in-memory space for registered normalized sql are limited by TopSQL.normalizedSQLCapacity.
//
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
// TODO: benchmark test concurrent performance
func (ts *TopSQLCollector) RegisterNormalizedSQL(sqlDigest string, normalizedSQL string) {
	ts.mu.RLock()
	_, exist := ts.normalizedSQLMap[sqlDigest]
	ts.mu.RUnlock()
	if !exist {
		ts.mu.Lock()
		ts.normalizedSQLMap[sqlDigest] = normalizedSQL
		ts.mu.Unlock()
	}
}

// RegisterNormalizedPlan is like RegisterNormalizedSQL, but for normalized plan strings.
// TODO: benchmark test concurrent performance
func (ts *TopSQLCollector) RegisterNormalizedPlan(planDigest string, normalizedPlan string) {
	ts.planRegisterChan <- &planRegisterJob{
		planDigest:     planDigest,
		normalizedPlan: normalizedPlan,
	}
}

// this should be the only place where the normalizedPlanMap is set
func (ts *TopSQLCollector) registerNormalizedPlanWorker(planDecoder planBinaryDecodeFunc) {
	// NOTE: if use multiple worker goroutine, we should make sure that access to the digest map is thread-safe
	for {
		job := <-ts.planRegisterChan
		ts.mu.RLock()
		_, exist := ts.normalizedPlanMap[job.planDigest]
		ts.mu.RUnlock()
		if exist {
			continue
		}
		planDecoded, err := planDecoder(job.normalizedPlan)
		if err != nil {
			fmt.Printf("decode plan failed: %v\n", err)
			continue
		}
		ts.normalizedPlanMap[job.planDigest] = planDecoded
	}
}

// snapshot will collect the current snapshot of data for transmission
// This could run in parallel with `Collect()`, so we should guard it by a mutex.
func (ts *TopSQLCollector) snapshot() []*pb.CollectCPUTimeRequest {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	total := len(ts.topSQLMap)
	batch := make([]*pb.CollectCPUTimeRequest, total)
	i := 0
	for key, value := range ts.topSQLMap {
		sqlDigest, planDigest := decodeCacheKey(key)
		normalizedSQL := ts.normalizedSQLMap[sqlDigest]
		normalizedPlan := ts.normalizedPlanMap[planDigest]
		batch[i] = &pb.CollectCPUTimeRequest{
			TimestampList:  value.TimestampList,
			CpuTimeMsList:  value.CPUTimeMsList,
			SqlDigest:      sqlDigest,
			NormalizedSql:  normalizedSQL,
			PlanDigest:     planDigest,
			NormalizedPlan: normalizedPlan,
		}
		i++
	}
	return batch
}

func (ts *TopSQLCollector) sendBatch(stream pb.TopSQLAgent_CollectCPUTimeClient, batch []*pb.CollectCPUTimeRequest) error {
	for _, req := range batch {
		if err := stream.Send(req); err != nil {
			log.Printf("ERROR: send stream request failed, %v", err)
			return err
		}
	}
	// response is Empty, drop it for now
	_, err := stream.CloseAndRecv()
	if err != nil {
		log.Printf("ERROR: receive stream response failed, %v", err)
		return err
	}
	return nil
}

// sendToAgentWorker will send a snapshot to the gRPC endpoint every interval
func (ts *TopSQLCollector) sendToAgentWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			batch := ts.snapshot()
			sendingTimeout := interval - 10*time.Second
			if sendingTimeout < 0 {
				sendingTimeout = interval / 2
			}
			conn, stream, err := newAgentClient(ts.agentGRPCAddress, sendingTimeout)
			if err != nil {
				log.Printf("ERROR: failed to create agent client, %v\n", err)
				continue
			}
			if err := ts.sendBatch(stream, batch); err != nil {
				continue
			}
			if err := conn.Close(); err != nil {
				log.Printf("ERROR: failed to close connection, %v\n", err)
				continue
			}
		case <-ts.quit:
			ticker.Stop()
			return
		}
	}
}
