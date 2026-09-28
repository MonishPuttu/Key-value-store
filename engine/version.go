package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
)

const numLevels = 7

// tableMeta is the persistent description of an SSTable.
type tableMeta struct {
	Num      uint64 `json:"num"`
	Smallest []byte `json:"smallest"`
	Largest  []byte `json:"largest"`
	Size     uint64 `json:"size"`
	Entries  int    `json:"entries"`
}

func (m tableMeta) overlaps(start, end []byte) bool {
	return bytes.Compare(m.Largest, start) >= 0 && bytes.Compare(m.Smallest, end) <= 0
}

// tableHandle is an open SSTable shared by every version that contains it.
// It is closed and its file deleted once no version references it and it
// has been compacted away.
type tableHandle struct {
	meta     tableMeta
	reader   *sstReader
	refs     atomic.Int32
	obsolete atomic.Bool
	db       *DB
}

func (t *tableHandle) ref() { t.refs.Add(1) }

func (t *tableHandle) unref() {
	if t.refs.Add(-1) == 0 && t.obsolete.Load() {
		_ = t.reader.close()
		t.db.cache.evictTable(t.meta.Num)
		_ = os.Remove(t.db.tablePath(t.meta.Num))
	}
}

// version is an immutable snapshot of which tables make up each level.
// Readers pin a version (ref count) so compaction can replace it without
// deleting files they are still reading.
//
//	L0: tables flushed from memtables; may overlap; newest first.
//	L1..L6: non-overlapping, sorted by key; each level ~10x the previous.
type version struct {
	levels [numLevels][]*tableHandle
	refs   atomic.Int32
}

func (v *version) ref() { v.refs.Add(1) }

func (v *version) unref() {
	if v.refs.Add(-1) == 0 {
		for _, lvl := range v.levels {
			for _, t := range lvl {
				t.unref()
			}
		}
	}
}

// clone returns a new version with the same tables (each ref'ed once more).
func (v *version) clone() *version {
	nv := &version{}
	for i, lvl := range v.levels {
		nv.levels[i] = append([]*tableHandle(nil), lvl...)
		for _, t := range lvl {
			t.ref()
		}
	}
	nv.refs.Store(1)
	return nv
}

// overlapping returns tables in level that overlap [start, end].
func (v *version) overlapping(level int, start, end []byte) []*tableHandle {
	var out []*tableHandle
	for _, t := range v.levels[level] {
		if t.meta.overlaps(start, end) {
			out = append(out, t)
		}
	}
	return out
}

func (v *version) levelSize(level int) uint64 {
	var s uint64
	for _, t := range v.levels[level] {
		s += t.meta.Size
	}
	return s
}

func sortLevel(ts []*tableHandle) {
	sort.Slice(ts, func(i, j int) bool { return bytes.Compare(ts[i].meta.Smallest, ts[j].meta.Smallest) < 0 })
}

// manifest is the durable record of the current version. It is rewritten
// atomically (write temp file, fsync, rename, fsync dir) on every change.
type manifest struct {
	NextFileNum uint64                 `json:"next_file_num"`
	LogNum      uint64                 `json:"log_num"` // WALs >= LogNum must be replayed
	Levels      [numLevels][]tableMeta `json:"levels"`
}

const manifestName = "MANIFEST"

func readManifest(dir string) (*manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, manifestName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	return &m, nil
}

func writeManifest(dir string, m *manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, manifestName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		return err
	}
	return syncDir(dir)
}

// syncDir makes a rename/create durable by fsyncing the directory entry.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
