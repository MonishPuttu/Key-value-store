// Command kvbench benchmarks the storage engine directly (no network, no
// Raft), in the spirit of LevelDB's db_bench. Use it to observe how the LSM
// tree behaves: flushes, compactions, level shape and write amplification.
//
//	go run ./cmd/kvbench -n 1000000 -value 100
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/MonishPuttu/Key-value-store/engine"
)

func main() {
	dir := flag.String("dir", "", "database directory (default: temporary)")
	n := flag.Int("n", 500000, "number of keys")
	valueSize := flag.Int("value", 100, "value size in bytes")
	sync := flag.Bool("sync", false, "fsync every write")
	benchmarks := flag.String("benchmarks", "fillseq,fillrandom,overwrite,readrandom,readmissing,scan", "comma-separated list")
	flag.Parse()

	if *dir == "" {
		d, err := os.MkdirTemp("", "kvbench-*")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(d)
		*dir = d
	}
	db, err := engine.Open(*dir, engine.Options{Sync: *sync})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	val := make([]byte, *valueSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	key := func(i int) []byte { return []byte(fmt.Sprintf("key%016d", i)) }

	fmt.Printf("keys=%d value=%dB sync=%v dir=%s\n\n", *n, *valueSize, *sync, *dir)
	for _, b := range strings.Split(*benchmarks, ",") {
		start := time.Now()
		ops := *n
		switch b {
		case "fillseq":
			for i := 0; i < *n; i++ {
				must(db.Put(key(i), val))
			}
		case "fillrandom", "overwrite":
			for i := 0; i < *n; i++ {
				must(db.Put(key(rand.IntN(*n)), val))
			}
		case "readrandom":
			found := 0
			for i := 0; i < *n; i++ {
				if _, err := db.Get(key(rand.IntN(*n))); err == nil {
					found++
				}
			}
			fmt.Printf("  (%d of %d found)\n", found, *n)
		case "readmissing":
			for i := 0; i < *n; i++ {
				_, _ = db.Get([]byte(fmt.Sprintf("missing%016d", i)))
			}
		case "scan":
			it, err := db.NewIterator(nil, nil)
			must(err)
			ops = 0
			for ; it.Valid(); it.Next() {
				ops++
			}
			must(it.Close())
		default:
			fmt.Fprintf(os.Stderr, "unknown benchmark %q\n", b)
			continue
		}
		d := time.Since(start)
		fmt.Printf("%-12s %10.0f ops/s  %8.2f µs/op\n", b, float64(ops)/d.Seconds(), float64(d.Microseconds())/float64(max(ops, 1)))
	}
	must(db.Flush())
	m := db.Metrics()
	fmt.Printf("\nLSM shape:\n")
	for _, l := range m.Levels {
		if l.Files > 0 {
			fmt.Printf("  L%d: %4d files %10.1f MiB\n", l.Level, l.Files, float64(l.Bytes)/(1<<20))
		}
	}
	fmt.Printf("\nflushes=%d compactions=%d trivial_moves=%d write_stalls=%d\n", m.Flushes, m.Compactions, m.TrivialMoves, m.WriteStalls)
	fmt.Printf("write amplification=%.2f  block cache hit rate=%.1f%%  bloom filter skips=%d (false positives %d)\n",
		m.WriteAmplification, 100*float64(m.CacheHits)/float64(max(1, m.CacheHits+m.CacheMisses)), m.BloomNegatives, m.BloomFalsePositives)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
