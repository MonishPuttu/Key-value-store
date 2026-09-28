// Command kvstore runs one node of the replicated key-value store.
//
// Single node:
//
//	kvstore -id n1 -data ./data/n1 -http :8080 -raft 127.0.0.1:7000 -bootstrap
//
// Three-node cluster:
//
//	kvstore -id n1 -data ./data/n1 -http :8081 -raft 127.0.0.1:7001 -bootstrap
//	kvstore -id n2 -data ./data/n2 -http :8082 -raft 127.0.0.1:7002 -join http://127.0.0.1:8081
//	kvstore -id n3 -data ./data/n3 -http :8083 -raft 127.0.0.1:7003 -join http://127.0.0.1:8081
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/MonishPuttu/Key-value-store/engine"
	"github.com/MonishPuttu/Key-value-store/internal/cluster"
	"github.com/MonishPuttu/Key-value-store/internal/server"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	id := flag.String("id", env("KV_NODE_ID", "n1"), "unique node id")
	dataDir := flag.String("data", env("KV_DATA_DIR", "./data/n1"), "data directory")
	httpAddr := flag.String("http", env("KV_HTTP_ADDR", ":8080"), "HTTP listen address")
	httpAdv := flag.String("http-advertise", env("KV_HTTP_ADVERTISE", ""), "HTTP URL other nodes use to reach this node (default derived from -http)")
	raftAddr := flag.String("raft", env("KV_RAFT_ADDR", "127.0.0.1:7000"), "Raft bind address")
	raftAdv := flag.String("raft-advertise", env("KV_RAFT_ADVERTISE", ""), "Raft address advertised to peers (required when binding 0.0.0.0)")
	bootstrap := flag.Bool("bootstrap", env("KV_BOOTSTRAP", "") == "true", "bootstrap a new cluster if no state exists")
	join := flag.String("join", env("KV_JOIN", ""), "HTTP address of an existing node to join")
	memtable := flag.Int64("memtable-mb", 16, "memtable size in MiB")
	cache := flag.Int("cache-mb", 64, "block cache size in MiB")
	logLevel := flag.String("log-level", env("KV_LOG_LEVEL", "info"), "debug|info|warn|error")
	flag.Parse()

	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(*logLevel))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)

	if *httpAdv == "" {
		host, port, _ := net.SplitHostPort(*httpAddr)
		if host == "" || host == "0.0.0.0" {
			host = "127.0.0.1"
		}
		*httpAdv = "http://" + net.JoinHostPort(host, port)
	}
	if err := run(log, *id, *dataDir, *httpAddr, *httpAdv, *raftAddr, *raftAdv, *bootstrap, *join, *memtable, *cache); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, id, dataDir, httpAddr, httpAdv, raftAddr, raftAdv string, bootstrap bool, join string, memMB int64, cacheMB int) error {
	store, err := cluster.Open(cluster.Config{
		NodeID: id, DataDir: dataDir, RaftBind: raftAddr, RaftAdvertise: raftAdv,
		HTTPAdvertise: httpAdv, Bootstrap: bootstrap, Logger: log,
		Engine: engine.Options{MemtableSize: memMB << 20, BlockCacheSize: cacheMB << 20},
	})
	if err != nil {
		return err
	}
	defer store.Close()

	srv := &http.Server{
		Addr: httpAddr, Handler: server.New(server.Options{Store: store, Logger: log}),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() {
		log.Info("kvstore node started", "id", id, "http", httpAddr, "raft", raftAddr, "data", dataDir)
		errc <- srv.ListenAndServe()
	}()
	if join != "" {
		go joinCluster(ctx, log, join, id, store.RaftAddr(), httpAdv)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	return nil
}

// joinCluster asks an existing node to add us, retrying until it succeeds
// (the target may still be starting or electing a leader).
func joinCluster(ctx context.Context, log *slog.Logger, target, id, raftAddr, httpAddr string) {
	body, _ := json.Marshal(server.JoinRequest{ID: id, RaftAddr: raftAddr, HTTPAddr: httpAddr})
	url := strings.TrimRight(target, "/") + "/cluster/join"
	for attempt := 1; ctx.Err() == nil; attempt++ {
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Info("joined cluster", "via", target)
				return
			}
			err = fmt.Errorf("status %s", resp.Status)
		}
		log.Warn("join failed; retrying", "via", target, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(min(attempt, 5)) * time.Second):
		}
	}
}
