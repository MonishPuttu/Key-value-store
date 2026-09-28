package engine

import (
	"encoding/binary"
	"errors"
)

// Batch is a set of writes applied atomically: after a crash either all of
// them are visible or none are (they share one WAL record).
type Batch struct {
	ops  []batchOp
	size int
}

type batchOp struct {
	key, value []byte
	tombstone  bool
}

const (
	kindValue     byte = 0
	kindTombstone byte = 1
)

// Put adds a set operation. The batch keeps references to key and value;
// do not modify them until the batch has been written.
func (b *Batch) Put(key, value []byte) {
	b.ops = append(b.ops, batchOp{key: key, value: value})
	b.size += len(key) + len(value)
}

// Delete adds a delete operation.
func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{key: key, tombstone: true})
	b.size += len(key)
}

// Len returns the number of operations.
func (b *Batch) Len() int { return len(b.ops) }

// Reset empties the batch for reuse.
func (b *Batch) Reset() { b.ops, b.size = b.ops[:0], 0 }

// encode serialises the batch as a WAL payload:
//
//	count uvarint, then per op: kind byte, klen uvarint, key, vlen uvarint, value
func (b *Batch) encode() []byte {
	buf := make([]byte, 0, b.size+len(b.ops)*12+binary.MaxVarintLen64)
	buf = binary.AppendUvarint(buf, uint64(len(b.ops)))
	for _, op := range b.ops {
		kind := kindValue
		if op.tombstone {
			kind = kindTombstone
		}
		buf = append(buf, kind)
		buf = binary.AppendUvarint(buf, uint64(len(op.key)))
		buf = append(buf, op.key...)
		buf = binary.AppendUvarint(buf, uint64(len(op.value)))
		buf = append(buf, op.value...)
	}
	return buf
}

var errCorruptBatch = errors.New("corrupt batch")

func decodeBatch(p []byte) (*Batch, error) {
	count, n := binary.Uvarint(p)
	if n <= 0 {
		return nil, errCorruptBatch
	}
	p = p[n:]
	b := &Batch{ops: make([]batchOp, 0, count)}
	for i := uint64(0); i < count; i++ {
		if len(p) < 1 {
			return nil, errCorruptBatch
		}
		kind := p[0]
		p = p[1:]
		key, rest, ok := readBytes(p)
		if !ok {
			return nil, errCorruptBatch
		}
		val, rest, ok := readBytes(rest)
		if !ok {
			return nil, errCorruptBatch
		}
		p = rest
		b.ops = append(b.ops, batchOp{key: key, value: val, tombstone: kind == kindTombstone})
	}
	return b, nil
}

// readBytes decodes a uvarint length-prefixed byte slice (copied).
func readBytes(p []byte) ([]byte, []byte, bool) {
	l, n := binary.Uvarint(p)
	if n <= 0 || uint64(len(p)-n) < l {
		return nil, nil, false
	}
	out := make([]byte, l)
	copy(out, p[n:n+int(l)])
	return out, p[n+int(l):], true
}
