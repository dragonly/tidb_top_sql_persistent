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

type EvictedHookFunc func(interface{}, interface{})

type LFUCache struct {
	mu          sync.RWMutex
	capacity    int
	items       map[interface{}]*lfuItem
	freqList    *list.List
	evictedHook EvictedHookFunc
}

// lfuItem stores the actual key/value of the cache, which is used in `Get()/Set()` APIs
type lfuItem struct {
	key         interface{}
	value       interface{}
	freqElement *list.Element
}

// freqEntry is the internal frequency management unit, the first frequency is always 0.
// All items in a freqEntry has the same frequency in the LFU cache.
type freqEntry struct {
	freq  uint
	items map[*lfuItem]struct{}
}

func NewLFUCache(capacity int, evictedHook EvictedHookFunc) *LFUCache {
	c := &LFUCache{
		capacity:    capacity,
		evictedHook: evictedHook,
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
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if ok {
		c.increment(item)
		v := item.value
		return v
	}
	return nil
}

func (c *LFUCache) increment(item *lfuItem) {
	currentFreqElement := item.freqElement
	currentFreqEntry := currentFreqElement.Value.(*freqEntry)
	nextFreq := currentFreqEntry.freq + 1
	delete(currentFreqEntry.items, item)

	// a boolean whether reuse the empty current entry
	removable := isRemovableFreqEntry(currentFreqEntry)

	// insert item into a valid entry
	nextFreqElement := currentFreqElement.Next()
	switch {
	case nextFreqElement == nil || nextFreqElement.Value.(*freqEntry).freq > nextFreq:
		if removable {
			currentFreqEntry.freq = nextFreq
			nextFreqElement = currentFreqElement
		} else {
			nextFreqElement = c.freqList.InsertAfter(&freqEntry{
				freq:  nextFreq,
				items: make(map[*lfuItem]struct{}),
			}, currentFreqElement)
		}
	case nextFreqElement.Value.(*freqEntry).freq == nextFreq:
		if removable {
			c.freqList.Remove(currentFreqElement)
		}
	default:
		panic("unreachable")
	}
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

func (c *LFUCache) Has(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.items[key]; ok {
		return true
	}
	return false
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

// removeElement is used to remove a given list element from the cache
func (c *LFUCache) removeItem(item *lfuItem) {
	entry := item.freqElement.Value.(*freqEntry)
	delete(c.items, item.key)
	delete(entry.items, item)
	if isRemovableFreqEntry(entry) {
		c.freqList.Remove(item.freqElement)
	}
	if c.evictedHook != nil {
		c.evictedHook(item.key, item.value)
	}
}

// Completely clear the cache
func (c *LFUCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
}

func isRemovableFreqEntry(entry *freqEntry) bool {
	return entry.freq != 0 && len(entry.items) == 0
}
