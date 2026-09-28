package engine

import (
	"bytes"
	"fmt"
	"time"
)

// compaction describes one unit of compaction work: merge inputs from
// `level` and the overlapping tables of `level+1` into new tables in level+1.
type compaction struct {
	level   int
	inputs  []*tableHandle // from level (L0: newest first)
	overlap []*tableHandle // from level+1, sorted
	v       *version       // pinned version the inputs came from
}

func (db *DB) maxBytesForLevel(level int) uint64 {
	b := db.opts.BaseLevelSize
	for i := 1; i < level; i++ {
		b *= uint64(db.opts.LevelMultiplier)
	}
	return b
}

func (db *DB) maybeScheduleCompaction() {
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}

func (db *DB) compactionLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.stop:
			return
		case <-db.compactCh:
		}
		for {
			db.mu.Lock()
			var c *compaction
			if !db.closed && db.bgErr == nil {
				c = db.pickCompaction()
			}
			db.mu.Unlock()
			if c == nil {
				break
			}
			if err := db.runCompaction(c); err != nil {
				db.setBgErr(fmt.Errorf("compaction L%d: %w", c.level, err))
				c.v.unref()
				break
			}
			c.v.unref()
			select {
			case <-db.stop:
				return
			default:
			}
		}
	}
}

// pickCompaction chooses the level whose "score" is highest:
//
//	L0 score  = number of files / L0CompactionTrigger   (file count bounds read amplification)
//	Ln score  = level bytes / target bytes for level n  (size bounds space amplification)
//
// Caller holds db.mu.
func (db *DB) pickCompaction() *compaction {
	v := db.current
	bestLevel, bestScore := -1, 1.0
	if s := float64(len(v.levels[0])) / float64(db.opts.L0CompactionTrigger); s >= bestScore {
		bestLevel, bestScore = 0, s
	}
	for lvl := 1; lvl < numLevels-1; lvl++ {
		if s := float64(v.levelSize(lvl)) / float64(db.maxBytesForLevel(lvl)); s > bestScore {
			bestLevel, bestScore = lvl, s
		}
	}
	if bestLevel < 0 {
		return nil
	}
	c := &compaction{level: bestLevel, v: v}
	if bestLevel == 0 {
		c.inputs = append(c.inputs, v.levels[0]...)
	} else {
		// Round-robin through the key space so every range gets compacted.
		ts := v.levels[bestLevel]
		pick := ts[0]
		for _, t := range ts {
			if db.compPtr[bestLevel] == nil || bytes.Compare(t.meta.Smallest, db.compPtr[bestLevel]) > 0 {
				pick = t
				break
			}
		}
		c.inputs = []*tableHandle{pick}
	}
	start, end := keyRange(c.inputs)
	c.overlap = v.overlapping(bestLevel+1, start, end)
	v.ref()
	return c
}

func keyRange(ts []*tableHandle) (start, end []byte) {
	for _, t := range ts {
		if start == nil || bytes.Compare(t.meta.Smallest, start) < 0 {
			start = t.meta.Smallest
		}
		if end == nil || bytes.Compare(t.meta.Largest, end) > 0 {
			end = t.meta.Largest
		}
	}
	return start, end
}

// runCompaction merges the inputs and installs the result.
func (db *DB) runCompaction(c *compaction) error {
	start := time.Now()
	out := c.level + 1

	// Trivial move: a single table with nothing to merge with in the next
	// level can be re-assigned without rewriting a single byte.
	if c.level > 0 && len(c.inputs) == 1 && len(c.overlap) == 0 {
		t := c.inputs[0]
		db.mu.Lock()
		defer db.mu.Unlock()
		db.installCompaction(c, []tableMeta{t.meta}, []*tableHandle{t})
		db.stats.TrivialMoves.Add(1)
		return db.saveManifest()
	}

	var iters []internalIterator
	var bytesIn uint64
	for _, t := range c.inputs { // L0: newest first = highest priority
		iters = append(iters, t.reader.newIterator(nil, nil))
		bytesIn += t.meta.Size
	}
	if len(c.overlap) > 0 {
		iters = append(iters, newLevelIterator(c.overlap, nil, nil))
		for _, t := range c.overlap {
			bytesIn += t.meta.Size
		}
	}
	merged := newMergingIterator(iters)
	defer merged.Close()

	// Tombstones can be dropped only if no deeper level may still hold an
	// older value for the key; otherwise deleting the tombstone would
	// resurrect that value.
	start1, end1 := keyRange(append(append([]*tableHandle{}, c.inputs...), c.overlap...))
	dropTombstones := true
	for lvl := out + 1; lvl < numLevels; lvl++ {
		if len(c.v.overlapping(lvl, start1, end1)) > 0 {
			dropTombstones = false
			break
		}
	}

	var (
		outputs []tableMeta
		w       *sstWriter
		num     uint64
	)
	finish := func() error {
		if w == nil {
			return nil
		}
		meta, err := w.finish()
		if err != nil {
			return err
		}
		meta.Num = num
		outputs = append(outputs, meta)
		w = nil
		return nil
	}
	for ; merged.Valid(); merged.Next() {
		if dropTombstones && merged.Tombstone() {
			continue
		}
		if w == nil {
			db.mu.Lock()
			num = db.nextNum
			db.nextNum++
			db.mu.Unlock()
			var err error
			if w, err = newSSTWriter(db.tablePath(num), db.opts.BlockSize, db.opts.BloomBitsPerKey); err != nil {
				return err
			}
		}
		if err := w.add(merged.Key(), merged.Value(), merged.Tombstone()); err != nil {
			w.abort()
			return err
		}
		if w.estimatedSize() >= db.opts.TargetFileSize {
			if err := finish(); err != nil {
				return err
			}
		}
	}
	if err := merged.Err(); err != nil {
		if w != nil {
			w.abort()
		}
		return err
	}
	if err := finish(); err != nil {
		return err
	}

	handles := make([]*tableHandle, 0, len(outputs))
	var bytesOut uint64
	for _, meta := range outputs {
		r, err := openSSTable(db.tablePath(meta.Num), meta.Num, db.cache, &db.stats)
		if err != nil {
			return err
		}
		handles = append(handles, &tableHandle{meta: meta, reader: r, db: db})
		bytesOut += meta.Size
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.installCompaction(c, outputs, handles)
	if err := db.saveManifest(); err != nil {
		return err
	}
	db.stats.Compactions.Add(1)
	db.stats.CompactionBytesRead.Add(int64(bytesIn))
	db.stats.CompactionBytesWrite.Add(int64(bytesOut))
	db.log.Debug("compaction done", "from", c.level, "inputs", len(c.inputs)+len(c.overlap),
		"outputs", len(outputs), "in_bytes", bytesIn, "out_bytes", bytesOut, "took", time.Since(start))
	return nil
}

// installCompaction builds the next version from db.current (which may have
// gained new L0 tables from flushes meanwhile): remove inputs, add outputs.
// Caller holds db.mu.
func (db *DB) installCompaction(c *compaction, outputs []tableMeta, handles []*tableHandle) {
	removed := map[uint64]bool{}
	for _, t := range c.inputs {
		removed[t.meta.Num] = true
	}
	for _, t := range c.overlap {
		removed[t.meta.Num] = true
	}
	moved := map[uint64]bool{}
	for _, h := range handles {
		moved[h.meta.Num] = true
	}
	nv := &version{}
	nv.refs.Store(1)
	for lvl, ts := range db.current.levels {
		for _, t := range ts {
			if removed[t.meta.Num] {
				continue
			}
			t.ref()
			nv.levels[lvl] = append(nv.levels[lvl], t)
		}
	}
	out := c.level + 1
	for _, h := range handles {
		h.ref()
		nv.levels[out] = append(nv.levels[out], h)
	}
	sortLevel(nv.levels[out])
	for _, t := range append(append([]*tableHandle{}, c.inputs...), c.overlap...) {
		if !moved[t.meta.Num] {
			t.obsolete.Store(true) // file deleted when the last reader lets go
		}
	}
	_, end := keyRange(c.inputs)
	db.compPtr[c.level] = end
	old := db.current
	db.current = nv
	old.unref()
	db.cond.Broadcast() // may unblock stalled writers
	db.maybeScheduleCompaction()
}
