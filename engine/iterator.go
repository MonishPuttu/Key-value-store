package engine

import (
	"bytes"
	"container/heap"
)

// internalIterator is implemented by memtable, SSTable, level and merging
// iterators. Keys/values are only valid until the next call to Next.
type internalIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Tombstone() bool
	Next()
	Err() error
	Close() error
}

// mergingIterator merges several sorted iterators into one sorted stream.
// Sources are ordered by recency: index 0 is the newest. When several
// sources contain the same key, only the newest version is emitted — this
// is how an LSM tree resolves overwrites and deletes across levels.
type mergingIterator struct {
	iters []internalIterator
	h     iterHeap
	cur   int // index of the iterator positioned at the current key
	err   error
}

type iterHeap struct {
	idx   []int
	iters []internalIterator
}

func (h iterHeap) Len() int { return len(h.idx) }
func (h iterHeap) Less(i, j int) bool {
	a, b := h.iters[h.idx[i]], h.iters[h.idx[j]]
	if c := bytes.Compare(a.Key(), b.Key()); c != 0 {
		return c < 0
	}
	return h.idx[i] < h.idx[j] // same key: newer source first
}
func (h iterHeap) Swap(i, j int) { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *iterHeap) Push(x any)   { h.idx = append(h.idx, x.(int)) }
func (h *iterHeap) Pop() any {
	n := len(h.idx)
	x := h.idx[n-1]
	h.idx = h.idx[:n-1]
	return x
}

func newMergingIterator(iters []internalIterator) *mergingIterator {
	m := &mergingIterator{iters: iters, h: iterHeap{iters: iters}, cur: -1}
	for i, it := range iters {
		if it.Valid() {
			m.h.idx = append(m.h.idx, i)
		} else if err := it.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}
	heap.Init(&m.h)
	m.pick()
	return m
}

// pick pops the smallest (newest) entry and advances every other source
// that holds the same key, so duplicates are skipped.
func (m *mergingIterator) pick() {
	m.cur = -1
	if m.h.Len() == 0 {
		return
	}
	m.cur = heap.Pop(&m.h).(int)
	key := m.iters[m.cur].Key()
	for m.h.Len() > 0 {
		top := m.h.idx[0]
		if !bytes.Equal(m.iters[top].Key(), key) {
			break
		}
		heap.Pop(&m.h)
		m.advance(top)
	}
}

func (m *mergingIterator) advance(i int) {
	it := m.iters[i]
	it.Next()
	if it.Valid() {
		heap.Push(&m.h, i)
	} else if err := it.Err(); err != nil && m.err == nil {
		m.err = err
	}
}

func (m *mergingIterator) Valid() bool     { return m.cur >= 0 && m.err == nil }
func (m *mergingIterator) Key() []byte     { return m.iters[m.cur].Key() }
func (m *mergingIterator) Value() []byte   { return m.iters[m.cur].Value() }
func (m *mergingIterator) Tombstone() bool { return m.iters[m.cur].Tombstone() }
func (m *mergingIterator) Err() error      { return m.err }

func (m *mergingIterator) Next() {
	m.advance(m.cur)
	m.pick()
}

func (m *mergingIterator) Close() error {
	var err error
	for _, it := range m.iters {
		if e := it.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// levelIterator concatenates the non-overlapping, sorted tables of one level
// (L1+), opening each table's iterator lazily.
type levelIterator struct {
	tables []*tableHandle
	i      int
	cur    *sstIterator
	start  []byte
	end    []byte
}

func newLevelIterator(tables []*tableHandle, start, end []byte) *levelIterator {
	it := &levelIterator{tables: tables, start: start, end: end}
	// Skip tables that end before start.
	for it.i < len(tables) && start != nil && bytes.Compare(tables[it.i].meta.Largest, start) < 0 {
		it.i++
	}
	it.open()
	return it
}

func (it *levelIterator) open() {
	for it.i < len(it.tables) {
		t := it.tables[it.i]
		if it.end != nil && bytes.Compare(t.meta.Smallest, it.end) >= 0 {
			it.i = len(it.tables)
			break
		}
		it.cur = t.reader.newIterator(it.start, it.end)
		if it.cur.Valid() || it.cur.Err() != nil {
			return
		}
		it.i++
	}
	it.cur = nil
}

func (it *levelIterator) Valid() bool     { return it.cur != nil && it.cur.Valid() }
func (it *levelIterator) Key() []byte     { return it.cur.Key() }
func (it *levelIterator) Value() []byte   { return it.cur.Value() }
func (it *levelIterator) Tombstone() bool { return it.cur.Tombstone() }
func (it *levelIterator) Close() error    { return nil }
func (it *levelIterator) Err() error {
	if it.cur != nil {
		return it.cur.Err()
	}
	return nil
}

func (it *levelIterator) Next() {
	it.cur.Next()
	if !it.cur.Valid() && it.cur.Err() == nil {
		it.i++
		it.open()
	}
}
