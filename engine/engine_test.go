package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// smallOpts forces frequent flushes and compactions.
func smallOpts() Options {
	return Options{
		MemtableSize:        32 << 10,
		BlockSize:           512,
		TargetFileSize:      16 << 10,
		BaseLevelSize:       64 << 10,
		L0CompactionTrigger: 2,
		BlockCacheSize:      64 << 10,
		Logger:              quiet,
	}
}

func openDB(t testing.TB, dir string, o Options) *DB {
	t.Helper()
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func mustGet(t *testing.T, db interface{ Get([]byte) ([]byte, error) }, k, want string) {
	t.Helper()
	v, err := db.Get([]byte(k))
	if want == "" {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%q) = %q, %v; want not found", k, v, err)
		}
		return
	}
	if err != nil || string(v) != want {
		t.Fatalf("Get(%q) = %q, %v; want %q", k, v, err, want)
	}
}

func TestBasicOperations(t *testing.T) {
	db := openDB(t, t.TempDir(), Options{Logger: quiet})
	defer db.Close()
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	_ = db.Put([]byte("b"), []byte("2"))
	mustGet(t, db, "a", "1")
	_ = db.Put([]byte("a"), []byte("1b"))
	mustGet(t, db, "a", "1b")
	_ = db.Delete([]byte("a"))
	mustGet(t, db, "a", "")
	mustGet(t, db, "missing", "")
	if err := db.Put(nil, []byte("x")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: %v", err)
	}
	// Empty values are legal and distinct from "not found".
	_ = db.Put([]byte("empty"), nil)
	if v, err := db.Get([]byte("empty")); err != nil || len(v) != 0 {
		t.Fatalf("empty value: %q %v", v, err)
	}
}

func TestBatchIsAtomicAndCopied(t *testing.T) {
	db := openDB(t, t.TempDir(), Options{Logger: quiet})
	defer db.Close()
	key := []byte("k1")
	var b Batch
	b.Put(key, []byte("v1"))
	b.Put([]byte("k2"), []byte("v2"))
	b.Delete([]byte("k3"))
	if err := db.Write(&b); err != nil {
		t.Fatal(err)
	}
	key[0] = 'X' // mutating caller buffers must not affect stored data
	mustGet(t, db, "k1", "v1")
	mustGet(t, db, "k2", "v2")
}

// model is a reference implementation used to check the engine.
type model map[string]string

func (m model) sortedKeys(start, end string) []string {
	var ks []string
	for k := range m {
		if k >= start && (end == "" || k < end) {
			ks = append(ks, k)
		}
	}
	sort.Strings(ks)
	return ks
}

func randomWorkload(t *testing.T, db *DB, m model, n int, rng *rand.Rand) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%05d", rng.IntN(3000))
		switch r := rng.IntN(10); {
		case r < 7:
			v := fmt.Sprintf("val-%d-%s", i, bytes.Repeat([]byte("x"), rng.IntN(100)))
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			m[k] = v
		default:
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
			delete(m, k)
		}
	}
}

func verify(t *testing.T, db *DB, m model) {
	t.Helper()
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("key-%05d", i)
		mustGet(t, db, k, m[k])
	}
	kvs, err := db.Scan(nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := m.sortedKeys("", "")
	if len(kvs) != len(want) {
		t.Fatalf("scan returned %d keys, want %d", len(kvs), len(want))
	}
	for i, kv := range kvs {
		if string(kv.Key) != want[i] || string(kv.Value) != m[want[i]] {
			t.Fatalf("scan[%d] = %q=%q, want %q", i, kv.Key, kv.Value, want[i])
		}
	}
}

// The core correctness test: a random mix of puts/overwrites/deletes that
// forces many flushes and compactions must always agree with a map.
func TestRandomizedAgainstModelWithCompaction(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, smallOpts())
	m := model{}
	rng := rand.New(rand.NewPCG(1, 2))
	randomWorkload(t, db, m, 30000, rng)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	verify(t, db, m)
	met := db.Metrics()
	if met.Flushes == 0 || met.Compactions == 0 {
		t.Fatalf("expected flushes and compactions, got %+v", met)
	}
	deep := 0
	for _, l := range met.Levels[1:] {
		deep += l.Files
	}
	if deep == 0 {
		t.Fatalf("expected data below L0: %+v", met.Levels)
	}
	// Reopen: everything must be recovered from manifest + WAL.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, dir, smallOpts())
	defer db.Close()
	verify(t, db, m)
	randomWorkload(t, db, m, 5000, rng)
	verify(t, db, m)
}

func TestRecoveryFromWALWithoutClose(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, Options{Logger: quiet})
	for i := 0; i < 100; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	_ = db.Delete([]byte("k050"))
	// "Crash": copy the directory as it is on disk right now, without Close.
	crash := copyDir(t, dir)
	db.Close()

	db2 := openDB(t, crash, Options{Logger: quiet})
	defer db2.Close()
	mustGet(t, db2, "k000", "v0")
	mustGet(t, db2, "k099", "v99")
	mustGet(t, db2, "k050", "")
}

func TestTornWALTailIsIgnored(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, Options{Logger: quiet})
	_ = db.Put([]byte("good"), []byte("1"))
	crash := copyDir(t, dir)
	db.Close()

	wals, _ := filepath.Glob(filepath.Join(crash, "*.wal"))
	if len(wals) == 0 {
		t.Fatal("no wal")
	}
	// Simulate a crash in the middle of appending a record.
	f, _ := os.OpenFile(wals[len(wals)-1], os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x10, 0, 0, 0, 1, 2})
	f.Close()

	db2 := openDB(t, crash, Options{Logger: quiet})
	defer db2.Close()
	mustGet(t, db2, "good", "1")
}

func TestCorruptBlockDetected(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, Options{Logger: quiet, BlockCacheSize: -1})
	_ = db.Put([]byte("key"), []byte("value-value-value"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	b, _ := os.ReadFile(ssts[0])
	b[5] ^= 0xff // flip a byte inside the first data block
	_ = os.WriteFile(ssts[0], b, 0o644)

	db = openDB(t, dir, Options{Logger: quiet, BlockCacheSize: -1})
	defer db.Close()
	if _, err := db.Get([]byte("key")); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("expected checksum error, got %v", err)
	}
}

func TestScanRanges(t *testing.T) {
	db := openDB(t, t.TempDir(), smallOpts())
	defer db.Close()
	m := model{}
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("user:%04d", i)
		v := fmt.Sprint(i)
		_ = db.Put([]byte(k), []byte(v))
		m[k] = v
	}
	for i := 0; i < 2000; i += 3 {
		k := fmt.Sprintf("user:%04d", i)
		_ = db.Delete([]byte(k))
		delete(m, k)
	}
	_ = db.Flush()
	kvs, err := db.Scan([]byte("user:0100"), []byte("user:0200"), 0)
	if err != nil {
		t.Fatal(err)
	}
	want := m.sortedKeys("user:0100", "user:0200")
	if len(kvs) != len(want) {
		t.Fatalf("got %d want %d", len(kvs), len(want))
	}
	for i := range want {
		if string(kvs[i].Key) != want[i] {
			t.Fatalf("kvs[%d]=%s want %s", i, kvs[i].Key, want[i])
		}
	}
	limited, _ := db.Scan([]byte("user:1"), nil, 5)
	if len(limited) != 5 || string(limited[0].Key) != "user:1000" {
		t.Fatalf("limit scan: %v", limited)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	db := openDB(t, t.TempDir(), smallOpts())
	defer db.Close()
	for i := 0; i < 500; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("old"))
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()
	for i := 0; i < 500; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new"))
	}
	_ = db.Delete([]byte("k007"))
	_ = db.Flush() // flushes + compactions must not disturb the snapshot

	mustGet(t, snap, "k007", "old")
	it := snap.NewIterator(nil, nil)
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		if string(it.Value()) != "old" {
			t.Fatalf("snapshot saw %q", it.Value())
		}
		n++
	}
	if n != 500 {
		t.Fatalf("snapshot has %d keys", n)
	}
	mustGet(t, db, "k001", "new")
	mustGet(t, db, "k007", "")
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	db := openDB(t, t.TempDir(), smallOpts())
	defer db.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := []byte(fmt.Sprintf("w%d-%04d", w, i))
				if err := db.Put(k, k); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := []byte(fmt.Sprintf("w%d-%04d", i%4, i))
				if v, err := db.Get(k); err == nil && !bytes.Equal(v, k) {
					t.Errorf("corrupt read %q=%q", k, v)
					return
				}
				if i%200 == 0 {
					if _, err := db.Scan([]byte("w1-"), []byte("w2-"), 50); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	for w := 0; w < 4; w++ {
		for i := 0; i < 2000; i += 97 {
			k := fmt.Sprintf("w%d-%04d", w, i)
			mustGet(t, db, k, k)
		}
	}
}

func TestDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, Options{Logger: quiet})
	defer db.Close()
	if _, err := Open(dir, Options{Logger: quiet}); err == nil {
		t.Fatal("second Open of the same directory must fail")
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	var keys [][]byte
	for i := 0; i < 10000; i++ {
		keys = append(keys, []byte(fmt.Sprintf("present-%d", i)))
	}
	f := decodeBloom(newBloomFilter(keys, 10).encode())
	for _, k := range keys {
		if !f.MayContain(k) {
			t.Fatalf("false negative for %s", k)
		}
	}
	fp := 0
	for i := 0; i < 10000; i++ {
		if f.MayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			fp++
		}
	}
	if rate := float64(fp) / 10000; rate > 0.02 {
		t.Fatalf("false positive rate %.3f, want ~0.01", rate)
	}
}

func TestSkiplistOrder(t *testing.T) {
	m := newMemtable(0)
	rng := rand.New(rand.NewPCG(3, 4))
	want := map[string]bool{}
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("%08d", rng.IntN(100000))
		m.put([]byte(k), []byte(k), false)
		want[k] = true
	}
	var prev []byte
	n := 0
	for it := m.newIterator(nil, nil); it.Valid(); it.Next() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("out of order: %s then %s", prev, it.Key())
		}
		prev = append(prev[:0], it.Key()...)
		n++
	}
	if n != len(want) {
		t.Fatalf("iterated %d keys, want %d", n, len(want))
	}
}

func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, _ := os.ReadDir(src)
	for _, e := range entries {
		if e.Name() == "LOCK" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		_ = os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644)
	}
	return dst
}

func BenchmarkPut(b *testing.B) {
	db := openDB(b, b.TempDir(), Options{Logger: quiet})
	defer db.Close()
	val := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Put([]byte(fmt.Sprintf("key-%010d", i)), val)
	}
}

func BenchmarkGetRandom(b *testing.B) {
	db := openDB(b, b.TempDir(), Options{Logger: quiet})
	defer db.Close()
	val := bytes.Repeat([]byte("v"), 100)
	const n = 200000
	for i := 0; i < n; i++ {
		_ = db.Put([]byte(fmt.Sprintf("key-%010d", i)), val)
	}
	_ = db.Flush()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get([]byte(fmt.Sprintf("key-%010d", rand.IntN(n))))
	}
}
