package cluster

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"

	"github.com/hashicorp/raft"

	"github.com/MonishPuttu/Key-value-store/engine"
)

// Key namespaces inside the engine. User data and cluster metadata share
// one replicated state machine but never collide.
var (
	userPrefix   = []byte("u/")
	metaPrefix   = []byte("m/")
	appliedKey   = []byte("m/applied_index")
	nodeKeyStart = []byte("m/node/")
)

func userKey(k []byte) []byte  { return append(append([]byte{}, userPrefix...), k...) }
func nodeKey(id string) []byte { return append(append([]byte{}, nodeKeyStart...), id...) }

// Op is a replicated command type.
type Op string

const (
	OpSet    Op = "set"
	OpDelete Op = "delete"
	OpCAS    Op = "cas"
	OpBatch  Op = "batch"
	OpMeta   Op = "meta" // set/delete cluster metadata (node addresses)
)

// Command is the payload of a Raft log entry.
type Command struct {
	Op             Op        `json:"op"`
	Key            []byte    `json:"key,omitempty"`
	Value          []byte    `json:"value,omitempty"`
	Expected       []byte    `json:"expected,omitempty"`
	ExpectedAbsent bool      `json:"expected_absent,omitempty"`
	Delete         bool      `json:"delete,omitempty"` // for OpMeta
	Ops            []Command `json:"ops,omitempty"`
}

// ApplyResult is returned (via raft.ApplyFuture.Response) for each command.
type ApplyResult struct {
	Err     error
	Swapped bool   // CAS outcome
	Current []byte // CAS: current value when not swapped
	Exists  bool   // CAS: whether the key existed when not swapped
}

// fsm applies committed Raft log entries to the LSM engine.
//
// Every batch of entries is written as ONE engine batch that also records
// the index of the last applied entry (m/applied_index), with fsync. After a
// crash the engine therefore knows exactly which entries it has applied and
// Raft replays only the rest — no double application, no full restore.
type fsm struct {
	mu      sync.RWMutex // guards db (swapped wholesale by Restore)
	db      *engine.DB
	dir     string
	opts    engine.Options
	applied uint64
	log     *slog.Logger
}

func openFSM(dir string, opts engine.Options, log *slog.Logger) (*fsm, error) {
	opts.Sync = true
	db, err := engine.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	f := &fsm{db: db, dir: dir, opts: opts, log: log}
	if v, err := db.Get(appliedKey); err == nil {
		f.applied, _ = strconv.ParseUint(string(v), 10, 64)
	}
	return f, nil
}

func (f *fsm) appliedIndex() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

// Apply implements raft.FSM (single entry).
func (f *fsm) Apply(l *raft.Log) any { return f.ApplyBatch([]*raft.Log{l})[0] }

// ApplyBatch implements raft.BatchingFSM.
func (f *fsm) ApplyBatch(logs []*raft.Log) []any {
	f.mu.Lock()
	defer f.mu.Unlock()
	results := make([]any, len(logs))
	var b engine.Batch
	// pending shadows the engine with writes earlier in this batch so a CAS
	// sees the effect of a Set that precedes it in the same batch.
	pending := map[string]*[]byte{}
	get := func(k []byte) ([]byte, bool, error) {
		if v, ok := pending[string(k)]; ok {
			if v == nil {
				return nil, false, nil
			}
			return *v, true, nil
		}
		v, err := f.db.Get(k)
		if errors.Is(err, engine.ErrNotFound) {
			return nil, false, nil
		}
		return v, err == nil, err
	}
	set := func(k, v []byte) {
		b.Put(k, v)
		vv := append([]byte{}, v...)
		pending[string(k)] = &vv
	}
	del := func(k []byte) {
		b.Delete(k)
		pending[string(k)] = nil
	}

	var last uint64
	for i, l := range logs {
		last = l.Index
		if l.Type != raft.LogCommand || l.Index <= f.applied {
			results[i] = &ApplyResult{}
			continue
		}
		var c Command
		if err := json.Unmarshal(l.Data, &c); err != nil {
			results[i] = &ApplyResult{Err: fmt.Errorf("decode command: %w", err)}
			continue
		}
		results[i] = f.apply(c, get, set, del)
	}
	if last > f.applied {
		b.Put(appliedKey, []byte(strconv.FormatUint(last, 10)))
	}
	if b.Len() == 0 {
		return results
	}
	if err := f.db.Write(&b); err != nil {
		// A storage failure on a replica is fatal for correctness: stop
		// rather than silently diverge from the log.
		f.log.Error("fsm: engine write failed", "err", err)
		panic(fmt.Sprintf("fsm: engine write failed: %v", err))
	}
	f.applied = max(f.applied, last)
	return results
}

func (f *fsm) apply(c Command, get func([]byte) ([]byte, bool, error), set func(k, v []byte), del func([]byte)) *ApplyResult {
	switch c.Op {
	case OpSet:
		set(userKey(c.Key), c.Value)
	case OpDelete:
		del(userKey(c.Key))
	case OpCAS:
		k := userKey(c.Key)
		cur, exists, err := get(k)
		if err != nil {
			return &ApplyResult{Err: err}
		}
		match := (c.ExpectedAbsent && !exists) || (!c.ExpectedAbsent && exists && bytes.Equal(cur, c.Expected))
		if !match {
			return &ApplyResult{Current: cur, Exists: exists}
		}
		set(k, c.Value)
		return &ApplyResult{Swapped: true}
	case OpBatch:
		for _, op := range c.Ops {
			switch op.Op {
			case OpSet:
				set(userKey(op.Key), op.Value)
			case OpDelete:
				del(userKey(op.Key))
			}
		}
	case OpMeta:
		k := append(append([]byte{}, metaPrefix...), c.Key...)
		if c.Delete {
			del(k)
		} else {
			set(k, c.Value)
		}
	default:
		return &ApplyResult{Err: fmt.Errorf("unknown op %q", c.Op)}
	}
	return &ApplyResult{}
}

// get reads a user key from the local state machine.
func (f *fsm) get(key []byte) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.db.Get(userKey(key))
}

// scan returns user keys in [start, end) (end nil = unbounded).
func (f *fsm) scan(start, end []byte, limit int) ([]engine.KV, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := userKey(start)
	var e []byte
	if end != nil {
		e = userKey(end)
	} else {
		e = prefixEnd(userPrefix)
	}
	kvs, err := f.db.Scan(s, e, limit)
	for i := range kvs {
		kvs[i].Key = kvs[i].Key[len(userPrefix):]
	}
	return kvs, err
}

// nodeAddrs returns the HTTP address of every known node.
func (f *fsm) nodeAddrs() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	kvs, _ := f.db.Scan(nodeKeyStart, prefixEnd(nodeKeyStart), 0)
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key[len(nodeKeyStart):])] = string(kv.Value)
	}
	return out
}

func (f *fsm) metrics() engine.Metrics {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.db.Metrics()
}

// prefixEnd returns the smallest key greater than every key with prefix p.
func prefixEnd(p []byte) []byte {
	end := append([]byte{}, p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// Snapshot implements raft.FSM. Raft guarantees Apply is not called
// concurrently with Snapshot, so the engine snapshot is exactly the state
// at the last applied index. Persist then streams it while Apply continues.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, err := f.db.Snapshot()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{snap: s}, nil
}

type fsmSnapshot struct{ snap *engine.Snapshot }

// Persist writes every key/value (user data and metadata, including the
// applied index) as length-prefixed frames.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	w := bufio.NewWriterSize(sink, 1<<20)
	it := s.snap.NewIterator(nil, nil)
	defer it.Close()
	var buf []byte
	for ; it.Valid(); it.Next() {
		buf = binary.AppendUvarint(buf[:0], uint64(len(it.Key())))
		buf = append(buf, it.Key()...)
		buf = binary.AppendUvarint(buf, uint64(len(it.Value())))
		buf = append(buf, it.Value()...)
		if _, err := w.Write(buf); err != nil {
			_ = sink.Cancel()
			return err
		}
	}
	if err := it.Err(); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := w.Flush(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() { s.snap.Release() }

// Restore replaces the entire state with a snapshot (used when a follower
// is too far behind and the leader sends it a snapshot).
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.db.Close(); err != nil && !errors.Is(err, engine.ErrClosed) {
		return err
	}
	if err := os.RemoveAll(f.dir); err != nil {
		return err
	}
	db, err := engine.Open(f.dir, f.opts)
	if err != nil {
		return err
	}
	f.db = db
	r := bufio.NewReaderSize(rc, 1<<20)
	var b engine.Batch
	n := 0
	for {
		k, err := readFrame(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("restore: %w", err)
		}
		v, err := readFrame(r)
		if err != nil {
			return fmt.Errorf("restore: %w", err)
		}
		b.Put(k, v)
		if n++; b.Len() >= 1000 {
			if err := db.Write(&b); err != nil {
				return err
			}
			b = engine.Batch{}
		}
	}
	if err := db.Write(&b); err != nil {
		return err
	}
	f.applied = 0
	if v, err := db.Get(appliedKey); err == nil {
		f.applied, _ = strconv.ParseUint(string(v), 10, 64)
	}
	f.log.Info("restored state from snapshot", "keys", n, "applied_index", f.applied)
	return nil
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}

func (f *fsm) close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.db.Close()
}
