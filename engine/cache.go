package engine

import (
	"container/list"
	"sync"
)

// blockCache is an LRU cache of decoded, checksum-verified data blocks,
// keyed by (table number, block offset). Hot blocks are served from memory
// instead of a pread + CRC check.
type blockCache struct {
	mu       sync.Mutex
	capacity int
	size     int
	ll       *list.List
	items    map[cacheKey]*list.Element
	stats    *Stats
}

type cacheKey struct {
	table  uint64
	offset uint64
}

type cacheEntry struct {
	key  cacheKey
	data []byte
}

func newBlockCache(capacity int, stats *Stats) *blockCache {
	return &blockCache{capacity: capacity, ll: list.New(), items: make(map[cacheKey]*list.Element), stats: stats}
}

func (c *blockCache) get(table, offset uint64) ([]byte, bool) {
	if c == nil || c.capacity <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[cacheKey{table, offset}]; ok {
		c.ll.MoveToFront(el)
		c.stats.CacheHits.Add(1)
		return el.Value.(*cacheEntry).data, true
	}
	c.stats.CacheMisses.Add(1)
	return nil, false
}

func (c *blockCache) put(table, offset uint64, data []byte) {
	if c == nil || c.capacity <= 0 || len(data) > c.capacity {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey{table, offset}
	if _, ok := c.items[k]; ok {
		return
	}
	c.items[k] = c.ll.PushFront(&cacheEntry{key: k, data: data})
	c.size += len(data)
	for c.size > c.capacity {
		el := c.ll.Back()
		e := el.Value.(*cacheEntry)
		c.ll.Remove(el)
		delete(c.items, e.key)
		c.size -= len(e.data)
	}
}

// evictTable drops every block of a deleted table.
func (c *blockCache) evictTable(table uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, el := range c.items {
		if k.table == table {
			c.ll.Remove(el)
			delete(c.items, k)
			c.size -= len(el.Value.(*cacheEntry).data)
		}
	}
}
