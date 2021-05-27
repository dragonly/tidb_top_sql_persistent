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
	"container/list"
	"sync"
)

type (
	EvictedHookFunc       func(interface{}, interface{})
	FrequencyEvaluateFunc func(interface{}, interface{}) uint64
)

// LFUCache is a cache that implements LFU eviction
// It supports customized frequency evaluation, which means you can replace "frequency" with
// any other number that is reasonable to your application requirements.
type LFUCache struct {
	mu       sync.RWMutex
	capacity int
	items    map[interface{}]*lfuItem
	freqList *list.List
	// EvictedHook is triggered whenever an item is evicted from the LFU cache.
	// This is a good point of time where you can do things when eviction happens.
	EvictedHook EvictedHookFunc
}

// lfuItem stores the actual key/value of the cache, which is used in `Get()/Set()` APIs
type lfuItem struct {
	key   interface{}
	value interface{}
	// freqElement is used to navigate in the list
	// When you want to do things related to frequency, you should always cast it to `freqEntry`
	freqElement *list.Element
}

// freqEntry is the internal frequency management unit, the first frequency is always 0.
// All items in a freqEntry has the same frequency in the LFU cache.
type freqEntry struct {
	freq  uint64
	items map[*lfuItem]struct{}
}

func NewLFUCache(capacity int) *LFUCache {
	c := &LFUCache{
		capacity: capacity,
	}
	c.init()
	return c
}

func (c *LFUCache) init() {
	c.freqList = list.New()
	c.items = make(map[interface{}]*lfuItem, c.capacity)
	c.freqList.PushFront(&freqEntry{
		freq:  0,
		items: make(map[*lfuItem]struct{}),
	})
}

func (c *LFUCache) Set(key, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set(key, value)
}

func (c *LFUCache) set(key, value interface{}) interface{} {
	// Check for existing item
	item, ok := c.items[key]
	if ok {
		item.value = value
	} else {
		// Verify capacity not exceeded
		if len(c.items) >= c.capacity {
			c.evict(1)
		}
		item = &lfuItem{
			key:         key,
			value:       value,
			freqElement: nil,
		}
		el := c.freqList.Front()
		fe := el.Value.(*freqEntry)
		fe.items[item] = struct{}{}

		item.freqElement = el
		c.items[key] = item
	}

	return item
}

func (c *LFUCache) Get(key interface{}) interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if item, ok := c.items[key]; ok {
		return item.value
	}
	return nil
}

func (c *LFUCache) IncrementFrequency(key interface{}, delta uint64) {
	if delta <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if item, ok := c.items[key]; ok {
		c.incrementFrequency(item, delta)
	}
}

// incrementFrequency add delta to the current frequency of the LFU key/value item
// It should be called with mutex protect.
func (c *LFUCache) incrementFrequency(item *lfuItem, delta uint64) {
	currentFreqElement := item.freqElement
	currentFreqEntry := currentFreqElement.Value.(*freqEntry)

	// remove item from the current frequency entry
	delete(currentFreqEntry.items, item)

	// a boolean whether reuse the empty current entry
	shouldRemoveCurrentFreqEntry := shouldRemoveFreqEntry(currentFreqEntry)

	// find the frequency list insert point
	nextFreq := currentFreqEntry.freq + delta
	nextFreqElement := currentFreqElement.Next()
	for nextFreqElement != nil && nextFreqElement.Value.(*freqEntry).freq < nextFreq {
		currentFreqElement = nextFreqElement
		nextFreqElement = nextFreqElement.Next()
	}
	if nextFreqElement == nil || nextFreqElement.Value.(*freqEntry).freq > nextFreq {
		// should insert new frequency entry to hold the item
		if shouldRemoveCurrentFreqEntry {
			// current frequency entry will be empty, reuse it
			currentFreqEntry.freq = nextFreq
			nextFreqElement = currentFreqElement
		} else {
			nextFreqElement = c.freqList.InsertAfter(&freqEntry{
				freq:  nextFreq,
				items: make(map[*lfuItem]struct{}),
			}, currentFreqElement)
		}
	} else {
		// add the item to the existing frequency entry
		if shouldRemoveCurrentFreqEntry {
			c.freqList.Remove(currentFreqElement)
		}
	}
	// after all, link the item and the frequency entry
	nextFreqElement.Value.(*freqEntry).items[item] = struct{}{}
	item.freqElement = nextFreqElement
}

// evict removes the least frequence item from the cache.
func (c *LFUCache) evict(count int) {
	entry := c.freqList.Front()
	for i := 0; i < count; {
		if entry == nil {
			return
		} else {
			for item := range entry.Value.(*freqEntry).items {
				if i >= count {
					return
				}
				c.removeItem(item)
				i++
			}
			entry = entry.Next()
		}
	}
}

// Remove removes the provided key from the cache.
func (c *LFUCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if item, ok := c.items[key]; ok {
		c.removeItem(item)
		return true
	}
	return false
}

// removeItem removes an LFU key/value item from the cache
func (c *LFUCache) removeItem(item *lfuItem) {
	entry := item.freqElement.Value.(*freqEntry)
	delete(c.items, item.key)
	delete(entry.items, item)
	if shouldRemoveFreqEntry(entry) {
		c.freqList.Remove(item.freqElement)
	}
	if c.EvictedHook != nil {
		c.EvictedHook(item.key, item.value)
	}
}

// Completely clear the cache
func (c *LFUCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
}

func shouldRemoveFreqEntry(entry *freqEntry) bool {
	return entry.freq != 0 && len(entry.items) == 0
}
