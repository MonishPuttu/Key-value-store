package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

// SSTable (Sorted String Table) file layout:
//
//	+-----------------+
//	| data block 0    |  entries sorted by key, then crc32 (4 bytes)
//	| data block 1    |
//	| ...             |
//	+-----------------+
//	| index block     |  per data block: last key, offset, length
//	+-----------------+
//	| bloom filter    |  bits + k
//	+-----------------+
//	| footer (40 B)   |  index off/len, bloom off/len, magic
//	+-----------------+
//
// Entry: kind byte | klen uvarint | vlen uvarint | key | value
//
// Files are immutable once written. The index and bloom filter are loaded in
// memory when the table is opened; a point lookup costs at most one data
// block read (usually zero for absent keys thanks to the bloom filter).

const (
	footerSize = 40
	sstMagic   = 0x4b56535354424c31 // "KVSSTBL1"
)

var errCorruptTable = errors.New("sstable: corrupt")

type sstWriter struct {
	f          *os.File
	bw         *bufio.Writer
	offset     uint64
	blockSize  int
	bitsPerKey int

	block     []byte
	blockLast []byte
	index     []byte
	keys      [][]byte // for the bloom filter
	smallest  []byte
	largest   []byte
	count     int
}

func newSSTWriter(path string, blockSize, bitsPerKey int) (*sstWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &sstWriter{f: f, bw: bufio.NewWriterSize(f, 256<<10), blockSize: blockSize, bitsPerKey: bitsPerKey}, nil
}

// add appends an entry; keys must be added in strictly increasing order.
func (w *sstWriter) add(key, value []byte, tombstone bool) error {
	if w.largest != nil && bytes.Compare(key, w.largest) <= 0 {
		return fmt.Errorf("sstable: keys out of order: %q after %q", key, w.largest)
	}
	kind := kindValue
	if tombstone {
		kind = kindTombstone
		value = nil
	}
	w.block = append(w.block, kind)
	w.block = binary.AppendUvarint(w.block, uint64(len(key)))
	w.block = binary.AppendUvarint(w.block, uint64(len(value)))
	w.block = append(w.block, key...)
	w.block = append(w.block, value...)
	k := append([]byte(nil), key...)
	if w.smallest == nil {
		w.smallest = k
	}
	w.largest = k
	w.blockLast = k
	w.keys = append(w.keys, k)
	w.count++
	if len(w.block) >= w.blockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *sstWriter) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	w.block = binary.LittleEndian.AppendUint32(w.block, crc32.Checksum(w.block, crcTable))
	if _, err := w.bw.Write(w.block); err != nil {
		return err
	}
	w.index = binary.AppendUvarint(w.index, uint64(len(w.blockLast)))
	w.index = append(w.index, w.blockLast...)
	w.index = binary.AppendUvarint(w.index, w.offset)
	w.index = binary.AppendUvarint(w.index, uint64(len(w.block)))
	w.offset += uint64(len(w.block))
	w.block = w.block[:0]
	return nil
}

// estimatedSize is the number of bytes written so far plus the open block.
func (w *sstWriter) estimatedSize() uint64 { return w.offset + uint64(len(w.block)) }

// finish writes index, bloom and footer and fsyncs the file.
func (w *sstWriter) finish() (tableMeta, error) {
	if err := w.flushBlock(); err != nil {
		return tableMeta{}, err
	}
	indexOff := w.offset
	if _, err := w.bw.Write(w.index); err != nil {
		return tableMeta{}, err
	}
	bloom := newBloomFilter(w.keys, w.bitsPerKey).encode()
	bloomOff := indexOff + uint64(len(w.index))
	if _, err := w.bw.Write(bloom); err != nil {
		return tableMeta{}, err
	}
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:], indexOff)
	binary.LittleEndian.PutUint64(footer[8:], uint64(len(w.index)))
	binary.LittleEndian.PutUint64(footer[16:], bloomOff)
	binary.LittleEndian.PutUint64(footer[24:], uint64(len(bloom)))
	binary.LittleEndian.PutUint64(footer[32:], sstMagic)
	if _, err := w.bw.Write(footer[:]); err != nil {
		return tableMeta{}, err
	}
	if err := w.bw.Flush(); err != nil {
		return tableMeta{}, err
	}
	if err := w.f.Sync(); err != nil {
		return tableMeta{}, err
	}
	size := bloomOff + uint64(len(bloom)) + footerSize
	return tableMeta{Smallest: w.smallest, Largest: w.largest, Size: size, Entries: w.count}, w.f.Close()
}

func (w *sstWriter) abort() {
	_ = w.f.Close()
	_ = os.Remove(w.f.Name())
}

// indexEntry locates one data block.
type indexEntry struct {
	lastKey []byte
	offset  uint64
	length  uint64
}

// sstReader serves reads from one immutable table file.
type sstReader struct {
	f     *os.File
	num   uint64
	index []indexEntry
	bloom bloomFilter
	cache *blockCache
	stats *Stats
}

func openSSTable(path string, num uint64, cache *blockCache, stats *Stats) (*sstReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := loadSSTable(f, num, cache, stats)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

func loadSSTable(f *os.File, num uint64, cache *blockCache, stats *Stats) (*sstReader, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < footerSize {
		return nil, errCorruptTable
	}
	var footer [footerSize]byte
	if _, err := f.ReadAt(footer[:], st.Size()-footerSize); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint64(footer[32:]) != sstMagic {
		return nil, fmt.Errorf("%w: bad magic", errCorruptTable)
	}
	indexOff := binary.LittleEndian.Uint64(footer[0:])
	indexLen := binary.LittleEndian.Uint64(footer[8:])
	bloomOff := binary.LittleEndian.Uint64(footer[16:])
	bloomLen := binary.LittleEndian.Uint64(footer[24:])
	if bloomOff+bloomLen+footerSize != uint64(st.Size()) || indexOff+indexLen != bloomOff {
		return nil, fmt.Errorf("%w: bad footer", errCorruptTable)
	}
	buf := make([]byte, indexLen+bloomLen)
	if _, err := f.ReadAt(buf, int64(indexOff)); err != nil {
		return nil, err
	}
	r := &sstReader{f: f, num: num, cache: cache, stats: stats, bloom: decodeBloom(buf[indexLen:])}
	p := buf[:indexLen]
	for len(p) > 0 {
		key, rest, ok := readBytes(p)
		if !ok {
			return nil, fmt.Errorf("%w: index", errCorruptTable)
		}
		off, n1 := binary.Uvarint(rest)
		if n1 <= 0 {
			return nil, fmt.Errorf("%w: index", errCorruptTable)
		}
		ln, n2 := binary.Uvarint(rest[n1:])
		if n2 <= 0 {
			return nil, fmt.Errorf("%w: index", errCorruptTable)
		}
		r.index = append(r.index, indexEntry{lastKey: key, offset: off, length: ln})
		p = rest[n1+n2:]
	}
	return r, nil
}

func (r *sstReader) close() error { return r.f.Close() }

// readBlock returns the verified contents of block i (without the CRC).
func (r *sstReader) readBlock(i int) ([]byte, error) {
	ie := r.index[i]
	if b, ok := r.cache.get(r.num, ie.offset); ok {
		return b, nil
	}
	buf := make([]byte, ie.length)
	if _, err := r.f.ReadAt(buf, int64(ie.offset)); err != nil {
		return nil, err
	}
	if len(buf) < 4 {
		return nil, errCorruptTable
	}
	data, sum := buf[:len(buf)-4], binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if crc32.Checksum(data, crcTable) != sum {
		return nil, fmt.Errorf("%w: block checksum mismatch in table %d", errCorruptTable, r.num)
	}
	r.stats.BlockReads.Add(1)
	r.cache.put(r.num, ie.offset, data)
	return data, nil
}

// blockFor returns the index of the first block that may contain key.
func (r *sstReader) blockFor(key []byte) int {
	return sort.Search(len(r.index), func(i int) bool { return bytes.Compare(r.index[i].lastKey, key) >= 0 })
}

// get looks up key. found reports whether the table has an entry for it
// (which may be a tombstone).
func (r *sstReader) get(key []byte) (e entry, found bool, err error) {
	if !r.bloom.MayContain(key) {
		r.stats.BloomNegatives.Add(1)
		return entry{}, false, nil
	}
	i := r.blockFor(key)
	if i >= len(r.index) {
		return entry{}, false, nil
	}
	block, err := r.readBlock(i)
	if err != nil {
		return entry{}, false, err
	}
	for p := block; len(p) > 0; {
		k, v, tomb, rest, err := decodeEntry(p)
		if err != nil {
			return entry{}, false, err
		}
		switch c := bytes.Compare(k, key); {
		case c == 0:
			return entry{value: append([]byte(nil), v...), tombstone: tomb}, true, nil
		case c > 0:
			r.stats.BloomFalsePositives.Add(1)
			return entry{}, false, nil
		}
		p = rest
	}
	r.stats.BloomFalsePositives.Add(1)
	return entry{}, false, nil
}

func decodeEntry(p []byte) (key, value []byte, tombstone bool, rest []byte, err error) {
	if len(p) < 3 {
		return nil, nil, false, nil, errCorruptTable
	}
	kind := p[0]
	klen, n1 := binary.Uvarint(p[1:])
	if n1 <= 0 {
		return nil, nil, false, nil, errCorruptTable
	}
	vlen, n2 := binary.Uvarint(p[1+n1:])
	if n2 <= 0 {
		return nil, nil, false, nil, errCorruptTable
	}
	off := 1 + n1 + n2
	if uint64(len(p)-off) < klen+vlen {
		return nil, nil, false, nil, errCorruptTable
	}
	key = p[off : off+int(klen)]
	value = p[off+int(klen) : off+int(klen)+int(vlen)]
	return key, value, kind == kindTombstone, p[off+int(klen)+int(vlen):], nil
}

// sstIterator walks a table in key order within [start, end).
type sstIterator struct {
	r          *sstReader
	blockIdx   int
	block      []byte
	key, value []byte
	tombstone  bool
	valid      bool
	end        []byte
	err        error
}

func (r *sstReader) newIterator(start, end []byte) *sstIterator {
	it := &sstIterator{r: r, end: end}
	if start == nil {
		it.blockIdx = 0
	} else {
		it.blockIdx = r.blockFor(start)
	}
	if it.blockIdx >= len(r.index) {
		return it
	}
	it.block, it.err = r.readBlock(it.blockIdx)
	if it.err != nil {
		return it
	}
	it.advance()
	for it.valid && start != nil && bytes.Compare(it.key, start) < 0 {
		it.advance()
	}
	return it
}

func (it *sstIterator) advance() {
	for len(it.block) == 0 {
		it.blockIdx++
		if it.blockIdx >= len(it.r.index) {
			it.valid = false
			return
		}
		it.block, it.err = it.r.readBlock(it.blockIdx)
		if it.err != nil {
			it.valid = false
			return
		}
	}
	k, v, tomb, rest, err := decodeEntry(it.block)
	if err != nil {
		it.err, it.valid = err, false
		return
	}
	it.block = rest
	if it.end != nil && bytes.Compare(k, it.end) >= 0 {
		it.valid = false
		return
	}
	it.key, it.value, it.tombstone, it.valid = k, v, tomb, true
}

func (it *sstIterator) Valid() bool     { return it.valid }
func (it *sstIterator) Key() []byte     { return it.key }
func (it *sstIterator) Value() []byte   { return it.value }
func (it *sstIterator) Tombstone() bool { return it.tombstone }
func (it *sstIterator) Next()           { it.advance() }
func (it *sstIterator) Err() error      { return it.err }
func (it *sstIterator) Close() error    { return nil }
