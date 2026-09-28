package engine

import (
	"bytes"
	"math/rand/v2"
	"sync/atomic"
)

const maxHeight = 12

// entry is the latest state of a key: a value or a tombstone (deletion
// marker). Tombstones must be stored, not just removed, because an older
// value for the key may still exist in an SSTable.
type entry struct {
	value     []byte
	tombstone bool
}

type node struct {
	key   []byte
	entry atomic.Pointer[entry]
	next  [maxHeight]atomic.Pointer[node]
}

// memtable is an in-memory sorted map implemented as a skiplist.
//
// Concurrency model (as in LevelDB): a single writer at a time (the DB write
// lock serialises Put) and any number of lock-free concurrent readers. A new
// node is fully initialised before it is published with an atomic store, so
// readers either see it completely or not at all.
type memtable struct {
	head   *node
	height atomic.Int32
	size   atomic.Int64 // approximate bytes
	count  atomic.Int64
	walNum uint64 // WAL file that holds this memtable's writes
}

func newMemtable(walNum uint64) *memtable {
	m := &memtable{head: &node{}, walNum: walNum}
	m.height.Store(1)
	return m
}

func randomHeight() int {
	h := 1
	// Each level is kept with probability 1/4 => expected O(log n) search.
	for h < maxHeight && rand.IntN(4) == 0 {
		h++
	}
	return h
}

// findGreaterOrEqual returns the first node with key >= key, filling prev
// with the rightmost node before it on every level (when prev != nil).
func (m *memtable) findGreaterOrEqual(key []byte, prev *[maxHeight]*node) *node {
	x := m.head
	level := int(m.height.Load()) - 1
	for {
		next := x.next[level].Load()
		if next != nil && bytes.Compare(next.key, key) < 0 {
			x = next
			continue
		}
		if prev != nil {
			prev[level] = x
		}
		if level == 0 {
			return next
		}
		level--
	}
}

// put inserts or overwrites key. Callers must serialise calls to put.
func (m *memtable) put(key, value []byte, tombstone bool) {
	e := &entry{value: value, tombstone: tombstone}
	var prev [maxHeight]*node
	x := m.findGreaterOrEqual(key, &prev)
	if x != nil && bytes.Equal(x.key, key) {
		old := x.entry.Swap(e)
		m.size.Add(int64(len(value) - len(old.value)))
		return
	}
	h := randomHeight()
	if cur := int(m.height.Load()); h > cur {
		for i := cur; i < h; i++ {
			prev[i] = m.head
		}
		m.height.Store(int32(h))
	}
	n := &node{key: key}
	n.entry.Store(e)
	for i := 0; i < h; i++ {
		n.next[i].Store(prev[i].next[i].Load())
		prev[i].next[i].Store(n) // publish
	}
	m.size.Add(int64(len(key) + len(value) + 48))
	m.count.Add(1)
}

// get returns the entry for key, if present.
func (m *memtable) get(key []byte) (*entry, bool) {
	x := m.findGreaterOrEqual(key, nil)
	if x != nil && bytes.Equal(x.key, key) {
		return x.entry.Load(), true
	}
	return nil, false
}

func (m *memtable) approximateSize() int64 { return m.size.Load() }
func (m *memtable) empty() bool            { return m.count.Load() == 0 }

// memIterator walks the skiplist in key order.
type memIterator struct {
	n   *node
	e   *entry
	end []byte
}

func (m *memtable) newIterator(start, end []byte) *memIterator {
	it := &memIterator{end: end}
	if start == nil {
		it.n = m.head.next[0].Load()
	} else {
		it.n = m.findGreaterOrEqual(start, nil)
	}
	it.load()
	return it
}

func (it *memIterator) load() {
	if it.n != nil && it.end != nil && bytes.Compare(it.n.key, it.end) >= 0 {
		it.n = nil
	}
	if it.n != nil {
		it.e = it.n.entry.Load()
	}
}

func (it *memIterator) Valid() bool     { return it.n != nil }
func (it *memIterator) Key() []byte     { return it.n.key }
func (it *memIterator) Value() []byte   { return it.e.value }
func (it *memIterator) Tombstone() bool { return it.e.tombstone }
func (it *memIterator) Next()           { it.n = it.n.next[0].Load(); it.load() }
func (it *memIterator) Err() error      { return nil }
func (it *memIterator) Close() error    { return nil }
