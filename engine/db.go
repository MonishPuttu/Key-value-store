// Package engine is an embedded, persistent key-value storage engine based
// on a Log-Structured Merge tree (LSM tree), in the family of LevelDB,
// RocksDB and Pebble.
//
// Write path: WAL append → memtable (skiplist). When the memtable is full it
// becomes immutable and a background goroutine flushes it to an SSTable in
// level 0. A second goroutine compacts tables down the levels (L0 → L1 → …)
// to bound read amplification and reclaim space from overwrites and deletes.
//
// Read path: memtable → immutable memtables → L0 tables (newest first) →
// one table per level L1..L6, using bloom filters to skip tables.
package engine

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// Options configures a DB. Zero values select defaults.
type Options struct {
	// MemtableSize is the size at which the memtable is flushed (default 4 MiB).
	MemtableSize int64
	// Sync fsyncs the WAL on every write. Without it, a write survives a
	// process crash but the last few may be lost on power failure.
	Sync bool
	// BlockSize is the target uncompressed data block size (default 4 KiB).
	BlockSize int
	// BloomBitsPerKey controls the bloom filter false-positive rate (default 10 ≈ 1%).
	BloomBitsPerKey int
	// BlockCacheSize is the LRU block cache capacity in bytes (default 32 MiB).
	BlockCacheSize int
	// L0CompactionTrigger: number of L0 files that triggers compaction (default 4).
	L0CompactionTrigger int
	// L0StopWritesTrigger: number of L0 files at which writes stall (default 12).
	L0StopWritesTrigger int
	// BaseLevelSize is the target size of L1 (default 10 MiB); each deeper
	// level is LevelMultiplier times larger (default 10).
	BaseLevelSize   uint64
	LevelMultiplier int
	// TargetFileSize is the size at which compaction output is split (default 2 MiB).
	TargetFileSize uint64
	// MaxImmutableMemtables waiting to be flushed before writes stall (default 2).
	MaxImmutableMemtables int
	Logger                *slog.Logger
}

func (o *Options) setDefaults() {
	if o.MemtableSize <= 0 {
		o.MemtableSize = 4 << 20
	}
	if o.BlockSize <= 0 {
		o.BlockSize = 4 << 10
	}
	if o.BloomBitsPerKey <= 0 {
		o.BloomBitsPerKey = 10
	}
	if o.BlockCacheSize == 0 {
		o.BlockCacheSize = 32 << 20
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = 4
	}
	if o.L0StopWritesTrigger <= 0 {
		o.L0StopWritesTrigger = 12
	}
	if o.BaseLevelSize == 0 {
		o.BaseLevelSize = 10 << 20
	}
	if o.LevelMultiplier <= 1 {
		o.LevelMultiplier = 10
	}
	if o.TargetFileSize == 0 {
		o.TargetFileSize = 2 << 20
	}
	if o.MaxImmutableMemtables <= 0 {
		o.MaxImmutableMemtables = 2
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Errors.
var (
	ErrNotFound = errors.New("engine: key not found")
	ErrClosed   = errors.New("engine: database closed")
	ErrEmptyKey = errors.New("engine: empty key")
)

// Stats holds engine counters (updated atomically).
type Stats struct {
	Puts, Deletes, Gets, GetHits              atomic.Int64
	BytesWritten                              atomic.Int64 // user bytes written
	Flushes, Compactions, TrivialMoves        atomic.Int64
	FlushBytes                                atomic.Int64
	CompactionBytesRead, CompactionBytesWrite atomic.Int64
	WriteStalls                               atomic.Int64
	BlockReads, CacheHits, CacheMisses        atomic.Int64
	BloomNegatives, BloomFalsePositives       atomic.Int64
}

// DB is an LSM-tree key-value store. It is safe for concurrent use.
type DB struct {
	dir  string
	opts Options
	log  *slog.Logger

	writeMu sync.Mutex // serialises writers

	mu      sync.Mutex // protects the fields below
	cond    *sync.Cond
	mem     *memtable
	imms    []*memtable // oldest first, waiting for flush
	wal     *walWriter
	current *version
	nextNum uint64
	logNum  uint64
	bgErr   error
	closed  bool
	compPtr [numLevels][]byte // round-robin compaction cursor per level

	flushCh   chan struct{}
	compactCh chan struct{}
	stop      chan struct{}
	wg        sync.WaitGroup
	lockFile  *os.File
	cache     *blockCache
	stats     Stats
}

func (db *DB) tablePath(num uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", num))
}
func (db *DB) walPath(num uint64) string { return filepath.Join(db.dir, fmt.Sprintf("%06d.wal", num)) }

// Open opens (or creates) a database in dir, recovering from the WAL.
func Open(dir string, opts Options) (*DB, error) {
	opts.setDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		dir: dir, opts: opts, log: opts.Logger,
		flushCh: make(chan struct{}, 1), compactCh: make(chan struct{}, 1), stop: make(chan struct{}),
	}
	db.cond = sync.NewCond(&db.mu)
	db.cache = newBlockCache(opts.BlockCacheSize, &db.stats)

	lf, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		return nil, fmt.Errorf("engine: %s is locked by another process: %w", dir, err)
	}
	db.lockFile = lf

	if err := db.recover(); err != nil {
		db.releaseLock()
		return nil, err
	}
	db.wg.Add(2)
	go db.flushLoop()
	go db.compactionLoop()
	db.maybeScheduleCompaction()
	return db, nil
}

func (db *DB) releaseLock() {
	if db.lockFile != nil {
		_ = syscall.Flock(int(db.lockFile.Fd()), syscall.LOCK_UN)
		_ = db.lockFile.Close()
	}
}

// recover loads the manifest, opens tables, replays WALs and deletes
// leftovers from interrupted flushes/compactions.
func (db *DB) recover() error {
	m, err := readManifest(db.dir)
	if err != nil {
		return err
	}
	if m == nil {
		m = &manifest{NextFileNum: 1}
	}
	db.nextNum = m.NextFileNum
	v := &version{}
	v.refs.Store(1)
	live := map[uint64]bool{}
	for lvl, metas := range m.Levels {
		for _, meta := range metas {
			r, err := openSSTable(db.tablePath(meta.Num), meta.Num, db.cache, &db.stats)
			if err != nil {
				return err
			}
			t := &tableHandle{meta: meta, reader: r, db: db}
			t.ref()
			v.levels[lvl] = append(v.levels[lvl], t)
			live[meta.Num] = true
		}
	}
	db.current = v

	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	var wals []uint64
	for _, e := range entries {
		name := e.Name()
		ext := filepath.Ext(name)
		num, err := strconv.ParseUint(strings.TrimSuffix(name, ext), 10, 64)
		if err != nil {
			continue
		}
		db.nextNum = max(db.nextNum, num+1)
		switch {
		case ext == ".wal" && num >= m.LogNum:
			wals = append(wals, num)
		case ext == ".wal":
			_ = os.Remove(filepath.Join(db.dir, name)) // already flushed
		case ext == ".sst" && !live[num]:
			_ = os.Remove(filepath.Join(db.dir, name)) // orphan from a crash
		}
	}
	sort.Slice(wals, func(i, j int) bool { return wals[i] < wals[j] })

	recovered := newMemtable(0)
	for _, num := range wals {
		n, torn, err := replayWAL(db.walPath(num), func(p []byte) error {
			b, err := decodeBatch(p)
			if err != nil {
				return err
			}
			for _, op := range b.ops {
				recovered.put(op.key, op.value, op.tombstone)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if torn {
			db.log.Warn("wal ended with a torn record (crash during write); ignoring the tail", "wal", num, "records", n)
		}
	}

	walNum := db.nextNum
	db.nextNum++
	w, err := createWAL(db.walPath(walNum), walNum)
	if err != nil {
		return err
	}
	db.wal = w
	db.mem = newMemtable(walNum)

	if !recovered.empty() {
		num := db.nextNum
		db.nextNum++
		meta, err := db.writeTable(num, recovered.newIterator(nil, nil), false)
		if err != nil {
			return err
		}
		if meta.Entries > 0 {
			if err := db.addL0(v, meta); err != nil {
				return err
			}
		}
		db.log.Info("recovered memtable from WAL", "wals", len(wals), "keys", recovered.count.Load())
	}
	db.logNum = walNum
	if err := db.saveManifest(); err != nil {
		return err
	}
	for _, num := range wals {
		_ = os.Remove(db.walPath(num))
	}
	return nil
}

func (db *DB) addL0(v *version, meta tableMeta) error {
	r, err := openSSTable(db.tablePath(meta.Num), meta.Num, db.cache, &db.stats)
	if err != nil {
		return err
	}
	t := &tableHandle{meta: meta, reader: r, db: db}
	t.ref()
	v.levels[0] = append([]*tableHandle{t}, v.levels[0]...) // newest first
	return nil
}

// saveManifest persists db.current. Caller holds db.mu (or is in Open).
func (db *DB) saveManifest() error {
	m := &manifest{NextFileNum: db.nextNum, LogNum: db.logNum}
	for i, lvl := range db.current.levels {
		for _, t := range lvl {
			m.Levels[i] = append(m.Levels[i], t.meta)
		}
	}
	return writeManifest(db.dir, m)
}

// writeTable writes the contents of it into SSTable num.
func (db *DB) writeTable(num uint64, it internalIterator, dropTombstones bool) (tableMeta, error) {
	w, err := newSSTWriter(db.tablePath(num), db.opts.BlockSize, db.opts.BloomBitsPerKey)
	if err != nil {
		return tableMeta{}, err
	}
	for ; it.Valid(); it.Next() {
		if dropTombstones && it.Tombstone() {
			continue
		}
		if err := w.add(it.Key(), it.Value(), it.Tombstone()); err != nil {
			w.abort()
			return tableMeta{}, err
		}
	}
	if err := it.Err(); err != nil {
		w.abort()
		return tableMeta{}, err
	}
	if w.count == 0 {
		w.abort()
		return tableMeta{Num: num}, nil
	}
	meta, err := w.finish()
	meta.Num = num
	return meta, err
}

// Put sets key to value.
func (db *DB) Put(key, value []byte) error {
	var b Batch
	b.Put(key, value)
	return db.Write(&b)
}

// Delete removes key (writes a tombstone).
func (db *DB) Delete(key []byte) error {
	var b Batch
	b.Delete(key)
	return db.Write(&b)
}

// Write applies a batch atomically.
func (db *DB) Write(b *Batch) error {
	if b.Len() == 0 {
		return nil
	}
	for _, op := range b.ops {
		if len(op.key) == 0 {
			return ErrEmptyKey
		}
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}
	payload := b.encode()
	if err := db.wal.append(payload, db.opts.Sync); err != nil {
		db.setBgErr(fmt.Errorf("wal append: %w", err))
		return err
	}
	for _, op := range b.ops {
		// Copy: callers may reuse their buffers after Write returns.
		k := append([]byte(nil), op.key...)
		var v []byte
		if !op.tombstone {
			v = append([]byte(nil), op.value...)
			db.stats.Puts.Add(1)
		} else {
			db.stats.Deletes.Add(1)
		}
		db.mem.put(k, v, op.tombstone)
	}
	db.stats.BytesWritten.Add(int64(b.size))
	return nil
}

func (db *DB) setBgErr(err error) {
	db.mu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
		db.log.Error("engine entered read-only mode after a background error", "err", err)
	}
	db.cond.Broadcast()
	db.mu.Unlock()
}

// makeRoomForWrite rotates a full memtable and applies backpressure
// (write stalls) when flushing or compaction cannot keep up.
func (db *DB) makeRoomForWrite() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for {
		switch {
		case db.closed:
			return ErrClosed
		case db.bgErr != nil:
			return db.bgErr
		case len(db.current.levels[0]) >= db.opts.L0StopWritesTrigger:
			db.stats.WriteStalls.Add(1)
			db.cond.Wait()
		case db.mem.approximateSize() < db.opts.MemtableSize:
			return nil
		case len(db.imms) >= db.opts.MaxImmutableMemtables:
			db.stats.WriteStalls.Add(1)
			db.cond.Wait()
		default:
			if err := db.rotateMemtable(); err != nil {
				return err
			}
			return nil
		}
	}
}

// rotateMemtable makes the current memtable immutable and starts a new one
// with a fresh WAL. Caller holds db.mu and db.writeMu.
func (db *DB) rotateMemtable() error {
	num := db.nextNum
	db.nextNum++
	w, err := createWAL(db.walPath(num), num)
	if err != nil {
		return err
	}
	if err := db.wal.close(); err != nil {
		w.close()
		return err
	}
	db.wal = w
	db.imms = append(db.imms, db.mem)
	db.mem = newMemtable(num)
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
	return nil
}

func (db *DB) flushLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.stop:
			return
		case <-db.flushCh:
		}
		for db.flushOne() {
		}
	}
}

// flushOne writes the oldest immutable memtable to an L0 SSTable.
func (db *DB) flushOne() bool {
	db.mu.Lock()
	if len(db.imms) == 0 || db.bgErr != nil || db.closed {
		db.mu.Unlock()
		return false
	}
	imm := db.imms[0]
	num := db.nextNum
	db.nextNum++
	db.mu.Unlock()

	meta, err := db.writeTable(num, imm.newIterator(nil, nil), false)
	if err != nil {
		db.setBgErr(fmt.Errorf("flush: %w", err))
		return false
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	nv := db.current.clone()
	if meta.Entries > 0 {
		if err := db.addL0(nv, meta); err != nil {
			nv.unref()
			db.bgErr = err
			db.cond.Broadcast()
			return false
		}
	}
	db.imms = db.imms[1:]
	old := db.current
	db.current = nv
	if len(db.imms) > 0 {
		db.logNum = db.imms[0].walNum
	} else {
		db.logNum = db.mem.walNum
	}
	if err := db.saveManifest(); err != nil {
		db.bgErr = fmt.Errorf("flush manifest: %w", err)
		db.cond.Broadcast()
		return false
	}
	old.unref()
	_ = os.Remove(db.walPath(imm.walNum))
	db.stats.Flushes.Add(1)
	db.stats.FlushBytes.Add(int64(meta.Size))
	db.cond.Broadcast()
	db.maybeScheduleCompaction()
	return true
}

// Get returns the value for key or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.stats.Gets.Add(1)
	mem, imms, v, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer v.unref()
	val, err := get(key, append([]*memtable{mem}, imms...), v)
	if err == nil {
		db.stats.GetHits.Add(1)
	}
	return val, err
}

// acquire pins the current read state. imms are returned newest first.
func (db *DB) acquire() (*memtable, []*memtable, *version, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, nil, nil, ErrClosed
	}
	imms := make([]*memtable, len(db.imms))
	for i, m := range db.imms {
		imms[len(imms)-1-i] = m
	}
	db.current.ref()
	return db.mem, imms, db.current, nil
}

// get searches memtables (newest first) and then the levels of v.
func get(key []byte, mems []*memtable, v *version) ([]byte, error) {
	for _, m := range mems {
		if e, ok := m.get(key); ok {
			if e.tombstone {
				return nil, ErrNotFound
			}
			return append([]byte(nil), e.value...), nil
		}
	}
	for _, t := range v.levels[0] { // newest first; may overlap
		if bytes.Compare(key, t.meta.Smallest) < 0 || bytes.Compare(key, t.meta.Largest) > 0 {
			continue
		}
		if e, found, err := t.reader.get(key); err != nil {
			return nil, err
		} else if found {
			if e.tombstone {
				return nil, ErrNotFound
			}
			return e.value, nil
		}
	}
	for lvl := 1; lvl < numLevels; lvl++ {
		ts := v.levels[lvl]
		// Non-overlapping and sorted: binary search the one candidate table.
		i := sort.Search(len(ts), func(i int) bool { return bytes.Compare(ts[i].meta.Largest, key) >= 0 })
		if i == len(ts) || bytes.Compare(key, ts[i].meta.Smallest) < 0 {
			continue
		}
		if e, found, err := ts[i].reader.get(key); err != nil {
			return nil, err
		} else if found {
			if e.tombstone {
				return nil, ErrNotFound
			}
			return e.value, nil
		}
	}
	return nil, ErrNotFound
}

// Iterator iterates over live keys in order. Close must be called.
type Iterator struct {
	m       *mergingIterator
	release func()
}

func newIterator(mems []*memtable, v *version, start, end []byte, release func()) *Iterator {
	var iters []internalIterator
	for _, m := range mems {
		iters = append(iters, m.newIterator(start, end))
	}
	for _, t := range v.levels[0] {
		iters = append(iters, t.reader.newIterator(start, end))
	}
	for lvl := 1; lvl < numLevels; lvl++ {
		if len(v.levels[lvl]) > 0 {
			iters = append(iters, newLevelIterator(v.levels[lvl], start, end))
		}
	}
	it := &Iterator{m: newMergingIterator(iters), release: release}
	it.skipTombstones()
	return it
}

// NewIterator returns an iterator over [start, end); nil bounds are open.
// It observes a consistent set of SSTables but may see concurrent writes
// to the active memtable; use Snapshot for a point-in-time view.
func (db *DB) NewIterator(start, end []byte) (*Iterator, error) {
	mem, imms, v, err := db.acquire()
	if err != nil {
		return nil, err
	}
	return newIterator(append([]*memtable{mem}, imms...), v, start, end, v.unref), nil
}

func (it *Iterator) skipTombstones() {
	for it.m.Valid() && it.m.Tombstone() {
		it.m.Next()
	}
}

func (it *Iterator) Valid() bool   { return it.m.Valid() }
func (it *Iterator) Key() []byte   { return it.m.Key() }
func (it *Iterator) Value() []byte { return it.m.Value() }
func (it *Iterator) Err() error    { return it.m.Err() }
func (it *Iterator) Next()         { it.m.Next(); it.skipTombstones() }

// Close releases the iterator's resources.
func (it *Iterator) Close() error {
	err := it.m.Close()
	if it.release != nil {
		it.release()
		it.release = nil
	}
	return err
}

// KV is a key-value pair returned by Scan.
type KV struct {
	Key   []byte
	Value []byte
}

// Scan returns up to limit live pairs in [start, end) (limit <= 0: no limit).
func (db *DB) Scan(start, end []byte, limit int) ([]KV, error) {
	it, err := db.NewIterator(start, end)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []KV
	for ; it.Valid() && (limit <= 0 || len(out) < limit); it.Next() {
		out = append(out, KV{Key: append([]byte(nil), it.Key()...), Value: append([]byte(nil), it.Value()...)})
	}
	return out, it.Err()
}

// Snapshot is a consistent, read-only, point-in-time view of the database.
type Snapshot struct {
	mems []*memtable
	v    *version
	once sync.Once
}

// Snapshot freezes the current state. The active memtable is rotated so
// that every memtable in the snapshot is immutable.
func (db *DB) Snapshot() (*Snapshot, error) {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	if !db.mem.empty() {
		if err := db.rotateMemtable(); err != nil {
			return nil, err
		}
	}
	s := &Snapshot{v: db.current}
	for i := len(db.imms) - 1; i >= 0; i-- {
		s.mems = append(s.mems, db.imms[i])
	}
	db.current.ref()
	return s, nil
}

// Get reads key as of the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) { return get(key, s.mems, s.v) }

// NewIterator iterates the snapshot over [start, end).
func (s *Snapshot) NewIterator(start, end []byte) *Iterator {
	s.v.ref()
	return newIterator(s.mems, s.v, start, end, s.v.unref)
}

// Release frees the snapshot. It is safe to call more than once.
func (s *Snapshot) Release() { s.once.Do(s.v.unref) }

// Flush forces the memtable to disk and waits for it (useful before backups
// and in tests).
func (db *DB) Flush() error {
	db.writeMu.Lock()
	db.mu.Lock()
	if !db.mem.empty() {
		if err := db.rotateMemtable(); err != nil {
			db.mu.Unlock()
			db.writeMu.Unlock()
			return err
		}
	}
	db.writeMu.Unlock()
	for len(db.imms) > 0 && db.bgErr == nil && !db.closed {
		db.cond.Wait()
	}
	err := db.bgErr
	db.mu.Unlock()
	return err
}

// Close flushes buffers and releases all resources. Unflushed memtables are
// recovered from the WAL on the next Open.
func (db *DB) Close() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.cond.Broadcast()
	db.mu.Unlock()

	close(db.stop)
	db.wg.Wait()

	err := db.wal.close()
	db.mu.Lock()
	for _, lvl := range db.current.levels {
		for _, t := range lvl {
			_ = t.reader.close()
		}
	}
	db.mu.Unlock()
	db.releaseLock()
	return err
}

// LevelInfo describes one level of the tree.
type LevelInfo struct {
	Level int    `json:"level"`
	Files int    `json:"files"`
	Bytes uint64 `json:"bytes"`
}

// Metrics is a snapshot of engine statistics.
type Metrics struct {
	Levels               []LevelInfo `json:"levels"`
	MemtableBytes        int64       `json:"memtable_bytes"`
	ImmutableMemtables   int         `json:"immutable_memtables"`
	Puts                 int64       `json:"puts"`
	Deletes              int64       `json:"deletes"`
	Gets                 int64       `json:"gets"`
	GetHits              int64       `json:"get_hits"`
	UserBytesWritten     int64       `json:"user_bytes_written"`
	Flushes              int64       `json:"flushes"`
	Compactions          int64       `json:"compactions"`
	TrivialMoves         int64       `json:"trivial_moves"`
	FlushBytes           int64       `json:"flush_bytes"`
	CompactionBytesRead  int64       `json:"compaction_bytes_read"`
	CompactionBytesWrite int64       `json:"compaction_bytes_written"`
	WriteAmplification   float64     `json:"write_amplification"`
	WriteStalls          int64       `json:"write_stalls"`
	BlockReads           int64       `json:"block_reads"`
	CacheHits            int64       `json:"cache_hits"`
	CacheMisses          int64       `json:"cache_misses"`
	BloomNegatives       int64       `json:"bloom_negatives"`
	BloomFalsePositives  int64       `json:"bloom_false_positives"`
}

// Metrics returns current statistics.
func (db *DB) Metrics() Metrics {
	db.mu.Lock()
	m := Metrics{MemtableBytes: db.mem.approximateSize(), ImmutableMemtables: len(db.imms)}
	for i := range db.current.levels {
		m.Levels = append(m.Levels, LevelInfo{Level: i, Files: len(db.current.levels[i]), Bytes: db.current.levelSize(i)})
	}
	db.mu.Unlock()
	s := &db.stats
	m.Puts, m.Deletes, m.Gets, m.GetHits = s.Puts.Load(), s.Deletes.Load(), s.Gets.Load(), s.GetHits.Load()
	m.UserBytesWritten = s.BytesWritten.Load()
	m.Flushes, m.Compactions, m.TrivialMoves = s.Flushes.Load(), s.Compactions.Load(), s.TrivialMoves.Load()
	m.FlushBytes = s.FlushBytes.Load()
	m.CompactionBytesRead, m.CompactionBytesWrite = s.CompactionBytesRead.Load(), s.CompactionBytesWrite.Load()
	if m.UserBytesWritten > 0 {
		// Bytes written to disk by the tree (WAL + flushes + compactions) per user byte.
		m.WriteAmplification = float64(m.UserBytesWritten+m.FlushBytes+m.CompactionBytesWrite) / float64(m.UserBytesWritten)
	}
	m.WriteStalls = s.WriteStalls.Load()
	m.BlockReads, m.CacheHits, m.CacheMisses = s.BlockReads.Load(), s.CacheHits.Load(), s.CacheMisses.Load()
	m.BloomNegatives, m.BloomFalsePositives = s.BloomNegatives.Load(), s.BloomFalsePositives.Load()
	return m
}
