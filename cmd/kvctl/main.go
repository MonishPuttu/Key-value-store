// Command kvctl is a command-line client for kvstore.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const usage = `kvctl — kvstore client

  kvctl [-addr URL] get <key> [-c strong|leader|stale]
  kvctl put <key> <value>            (value "-" reads stdin)
  kvctl del <key>
  kvctl cas <key> <expected> <new>   (expected "-" means "must not exist")
  kvctl scan [-prefix p] [-start s] [-end e] [-limit n] [-c level]
  kvctl status | stats
  kvctl snapshot
  kvctl remove <node-id>
  kvctl bench [-n 20000] [-c 32] [-size 100] [-read-ratio 0.8]

  KV_ADDR sets the default address (http://127.0.0.1:8080).
`

var (
	addr   = envOr("KV_ADDR", "http://127.0.0.1:8080")
	client = &http.Client{Timeout: 15 * time.Second}
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	args := os.Args[1:]
	if len(args) >= 2 && args[0] == "-addr" {
		addr, args = args[1], args[2:]
	}
	if len(args) == 0 {
		fmt.Print(usage)
		os.Exit(2)
	}
	var err error
	switch cmd, rest := args[0], args[1:]; cmd {
	case "get":
		err = get(rest)
	case "put":
		err = put(rest)
	case "del":
		err = need(rest, 1, func() error { return show(do("DELETE", "/v1/kv/"+escKey(rest[0]), nil)) })
	case "cas":
		err = cas(rest)
	case "scan":
		err = scan(rest)
	case "status":
		err = show(do("GET", "/cluster/status", nil))
	case "stats":
		err = show(do("GET", "/v1/stats", nil))
	case "snapshot":
		err = show(do("POST", "/cluster/snapshot", nil))
	case "remove":
		err = need(rest, 1, func() error { return show(do("DELETE", "/cluster/nodes/"+url.PathEscape(rest[0]), nil)) })
	case "bench":
		err = bench(rest)
	default:
		fmt.Print(usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func need(a []string, n int, f func() error) error {
	if len(a) < n {
		return fmt.Errorf("expected %d argument(s); see kvctl help", n)
	}
	return f()
}

// escKey escapes each path segment but keeps "/" so hierarchical keys read naturally.
func escKey(k string) string {
	parts := strings.Split(k, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func do(method, path string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequest(method, strings.TrimRight(addr, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return b, resp.StatusCode, err
}

func show(b []byte, code int, err error) error {
	if err != nil {
		return err
	}
	if code >= 400 {
		return fmt.Errorf("HTTP %d: %s", code, strings.TrimSpace(string(b)))
	}
	if len(b) == 0 {
		fmt.Println("OK")
		return nil
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, b, "", "  ") == nil {
		b = pretty.Bytes()
	}
	fmt.Println(strings.TrimSpace(string(b)))
	return nil
}

func split(args []string) (pos, flags []string) {
	for i, a := range args {
		if strings.HasPrefix(a, "-") && a != "-" {
			return pos, args[i:]
		}
		pos = append(pos, a)
	}
	return pos, nil
}

func get(args []string) error {
	pos, rest := split(args)
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	c := fs.String("c", "strong", "consistency")
	_ = fs.Parse(rest)
	if len(pos) < 1 {
		return fmt.Errorf("usage: kvctl get <key>")
	}
	b, code, err := do("GET", "/v1/kv/"+escKey(pos[0])+"?consistency="+*c, nil)
	if err != nil {
		return err
	}
	if code >= 400 {
		return fmt.Errorf("HTTP %d: %s", code, strings.TrimSpace(string(b)))
	}
	fmt.Println(string(b))
	return nil
}

func put(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: kvctl put <key> <value>")
	}
	v := []byte(args[1])
	if args[1] == "-" {
		var err error
		if v, err = io.ReadAll(os.Stdin); err != nil {
			return err
		}
	}
	return show(do("PUT", "/v1/kv/"+escKey(args[0]), v))
}

func cas(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: kvctl cas <key> <expected|-> <new>")
	}
	body := map[string]any{"value": args[2]}
	if args[1] == "-" {
		body["expected_absent"] = true
	} else {
		body["expected"] = args[1]
	}
	b, _ := json.Marshal(body)
	return show(do("POST", "/v1/cas/"+escKey(args[0]), b))
}

func scan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	prefix := fs.String("prefix", "", "key prefix")
	start := fs.String("start", "", "start key (inclusive)")
	end := fs.String("end", "", "end key (exclusive)")
	limit := fs.Int("limit", 100, "max items")
	c := fs.String("c", "strong", "consistency")
	_ = fs.Parse(args)
	q := url.Values{"prefix": {*prefix}, "start": {*start}, "end": {*end}, "limit": {fmt.Sprint(*limit)}, "consistency": {*c}}
	return show(do("GET", "/v1/scan?"+q.Encode(), nil))
}

// bench runs a mixed read/write HTTP workload and reports throughput and
// latency percentiles.
func bench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("n", 20000, "total operations")
	conc := fs.Int("c", 32, "concurrent clients")
	size := fs.Int("size", 100, "value size in bytes")
	readRatio := fs.Float64("read-ratio", 0.8, "fraction of reads")
	keys := fs.Int("keys", 10000, "key space")
	_ = fs.Parse(args)
	val := bytes.Repeat([]byte("x"), *size)

	fmt.Printf("preloading %d keys...\n", *keys)
	var wg sync.WaitGroup
	var next atomic.Int64
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := next.Add(1) - 1; i < int64(*keys); i = next.Add(1) - 1 {
				_, _, _ = do("PUT", fmt.Sprintf("/v1/kv/bench/%08d", i), val)
			}
		}()
	}
	wg.Wait()

	type sample struct {
		d    time.Duration
		read bool
		err  bool
	}
	samples := make([]sample, *n)
	next.Store(0)
	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := next.Add(1) - 1; i < int64(*n); i = next.Add(1) - 1 {
				key := fmt.Sprintf("/v1/kv/bench/%08d", (i*7919)%int64(*keys))
				read := float64(i%100)/100 < *readRatio
				t := time.Now()
				var code int
				var err error
				if read {
					_, code, err = do("GET", key, nil)
				} else {
					_, code, err = do("PUT", key, val)
				}
				samples[i] = sample{time.Since(t), read, err != nil || code >= 400}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	report := func(name string, filter func(sample) bool) {
		var ds []time.Duration
		errs := 0
		for _, s := range samples {
			if filter(s) {
				ds = append(ds, s.d)
				if s.err {
					errs++
				}
			}
		}
		if len(ds) == 0 {
			return
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		p := func(q float64) time.Duration { return ds[int(float64(len(ds)-1)*q)].Round(10 * time.Microsecond) }
		fmt.Printf("%-6s %7d ops  %8.0f ops/s  p50=%-9v p99=%-9v max=%-9v errors=%d\n",
			name, len(ds), float64(len(ds))/elapsed.Seconds(), p(.5), p(.99), ds[len(ds)-1].Round(10*time.Microsecond), errs)
	}
	report("reads", func(s sample) bool { return s.read })
	report("writes", func(s sample) bool { return !s.read })
	report("total", func(sample) bool { return true })
	return nil
}
