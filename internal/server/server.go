// Package server exposes a cluster.Store over HTTP. Any node accepts any
// request: writes and strongly consistent reads that arrive at a follower
// are transparently forwarded to the leader.
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/MonishPuttu/Key-value-store/engine"
	"github.com/MonishPuttu/Key-value-store/internal/cluster"
)

const forwardedHeader = "X-Kv-Forwarded"

// Options configures the server.
type Options struct {
	Store        *cluster.Store
	Logger       *slog.Logger
	MaxValueSize int64 // default 1 MiB
	MaxKeySize   int   // default 4 KiB
}

// Server is the HTTP handler.
type Server struct {
	s       *cluster.Store
	log     *slog.Logger
	mux     *http.ServeMux
	opts    Options
	reqs    *prometheus.CounterVec
	latency *prometheus.HistogramVec
	reg     *prometheus.Registry
}

// New builds the handler.
func New(o Options) *Server {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxValueSize <= 0 {
		o.MaxValueSize = 1 << 20
	}
	if o.MaxKeySize <= 0 {
		o.MaxKeySize = 4 << 10
	}
	srv := &Server{s: o.Store, log: o.Logger, mux: http.NewServeMux(), opts: o, reg: prometheus.NewRegistry()}
	srv.reqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_http_requests_total", Help: "HTTP requests by route and status.",
	}, []string{"route", "code"})
	srv.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kv_http_request_duration_seconds", Help: "HTTP latency by route.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 3, 12),
	}, []string{"route"})
	srv.reg.MustRegister(srv.reqs, srv.latency, newCollector(o.Store),
		prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	srv.route("GET /v1/kv/{key...}", "get", srv.get)
	srv.route("PUT /v1/kv/{key...}", "put", srv.put)
	srv.route("DELETE /v1/kv/{key...}", "delete", srv.del)
	srv.route("POST /v1/cas/{key...}", "cas", srv.cas)
	srv.route("POST /v1/batch", "batch", srv.batch)
	srv.route("GET /v1/scan", "scan", srv.scan)
	srv.route("GET /v1/stats", "stats", srv.stats)
	srv.route("GET /cluster/status", "status", srv.status)
	srv.route("POST /cluster/join", "join", srv.join)
	srv.route("DELETE /cluster/nodes/{id}", "remove", srv.remove)
	srv.route("POST /cluster/snapshot", "snapshot", srv.snapshot)
	srv.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	srv.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if _, _, err := o.Store.LeaderHTTP(); err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	srv.mux.Handle("GET /metrics", promhttp.HandlerFor(srv.reg, promhttp.HandlerOpts{}))
	return srv
}

// ServeHTTP implements http.Handler.
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { srv.mux.ServeHTTP(w, r) }

type recorder struct {
	http.ResponseWriter
	code int
}

func (r *recorder) WriteHeader(c int) { r.code = c; r.ResponseWriter.WriteHeader(c) }

func (srv *Server) route(pattern, name string, h http.HandlerFunc) {
	srv.mux.Handle(pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w, code: http.StatusOK}
		h(rec, r)
		srv.reqs.WithLabelValues(name, strconv.Itoa(rec.code)).Inc()
		srv.latency.WithLabelValues(name).Observe(time.Since(start).Seconds())
	}))
}

// forward proxies the request to the leader.
func (srv *Server) forward(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(forwardedHeader) != "" {
		// Already forwarded once: leadership is changing; let the client retry.
		writeError(w, http.StatusServiceUnavailable, "leadership changed during request; retry")
		return
	}
	_, addr, err := srv.s.LeaderHTTP()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	target, err := url.Parse(addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "bad leader address")
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		writeError(w, http.StatusBadGateway, "forward to leader: "+err.Error())
	}
	r.Header.Set(forwardedHeader, "1")
	w.Header().Set("X-Kv-Leader", addr)
	proxy.ServeHTTP(w, r)
}

func (srv *Server) key(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	k := r.PathValue("key")
	if k == "" || len(k) > srv.opts.MaxKeySize {
		writeError(w, http.StatusBadRequest, "key must be 1.."+strconv.Itoa(srv.opts.MaxKeySize)+" bytes")
		return nil, false
	}
	return []byte(k), true
}

func consistency(r *http.Request) cluster.Consistency {
	switch c := cluster.Consistency(r.URL.Query().Get("consistency")); c {
	case cluster.Stale, cluster.Leader:
		return c
	default:
		return cluster.Strong
	}
}

// handleErr maps store errors to responses, forwarding to the leader when
// this node is a follower.
func (srv *Server) handleErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, cluster.ErrNotLeader):
		srv.forward(w, r)
	case errors.Is(err, engine.ErrNotFound):
		writeError(w, http.StatusNotFound, "key not found")
	case errors.Is(err, cluster.ErrNoLeader):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		srv.log.Error("request failed", "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func (srv *Server) get(w http.ResponseWriter, r *http.Request) {
	k, ok := srv.key(w, r)
	if !ok {
		return
	}
	v, err := srv.s.Get(k, consistency(r))
	if err != nil {
		srv.handleErr(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(v)
}

// readBody reads the request body for forwarding-safe reuse.
func (srv *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, srv.opts.MaxValueSize))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "value too large")
		return nil, false
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	r.ContentLength = int64(len(b))
	return b, true
}

func (srv *Server) put(w http.ResponseWriter, r *http.Request) {
	k, ok := srv.key(w, r)
	if !ok {
		return
	}
	v, ok := srv.readBody(w, r)
	if !ok {
		return
	}
	if err := srv.s.Set(k, v); err != nil {
		srv.handleErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (srv *Server) del(w http.ResponseWriter, r *http.Request) {
	k, ok := srv.key(w, r)
	if !ok {
		return
	}
	if err := srv.s.Delete(k); err != nil {
		srv.handleErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// CASRequest is the body of POST /v1/cas/{key}.
type CASRequest struct {
	Expected       *string `json:"expected"`        // required unless expected_absent
	ExpectedAbsent bool    `json:"expected_absent"` // create only if missing
	Value          string  `json:"value"`
}

func (srv *Server) cas(w http.ResponseWriter, r *http.Request) {
	k, ok := srv.key(w, r)
	if !ok {
		return
	}
	body, ok := srv.readBody(w, r)
	if !ok {
		return
	}
	var req CASRequest
	if err := json.Unmarshal(body, &req); err != nil || (req.Expected == nil && !req.ExpectedAbsent) {
		writeError(w, http.StatusBadRequest, `body must be {"expected": "...", "value": "..."} or {"expected_absent": true, "value": "..."}`)
		return
	}
	var expected []byte
	if req.Expected != nil {
		expected = []byte(*req.Expected)
	}
	res, err := srv.s.CAS(k, expected, []byte(req.Value), req.ExpectedAbsent)
	if err != nil {
		srv.handleErr(w, r, err)
		return
	}
	if !res.Swapped {
		out := map[string]any{"swapped": false, "exists": res.Exists}
		if res.Exists {
			out["current"] = string(res.Current)
		}
		writeJSON(w, http.StatusConflict, out)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"swapped": true})
}

// BatchRequest is the body of POST /v1/batch.
type BatchRequest struct {
	Ops []struct {
		Op    string `json:"op"` // "set" or "delete"
		Key   string `json:"key"`
		Value string `json:"value,omitempty"`
	} `json:"ops"`
}

func (srv *Server) batch(w http.ResponseWriter, r *http.Request) {
	body, ok := srv.readBody(w, r)
	if !ok {
		return
	}
	var req BatchRequest
	if err := json.Unmarshal(body, &req); err != nil || len(req.Ops) == 0 || len(req.Ops) > 10000 {
		writeError(w, http.StatusBadRequest, "body must be {\"ops\": [...]} with 1..10000 operations")
		return
	}
	ops := make([]cluster.Command, 0, len(req.Ops))
	for _, op := range req.Ops {
		if op.Key == "" || (op.Op != "set" && op.Op != "delete") {
			writeError(w, http.StatusBadRequest, "each op needs op=set|delete and a key")
			return
		}
		ops = append(ops, cluster.Command{Op: cluster.Op(op.Op), Key: []byte(op.Key), Value: []byte(op.Value)})
	}
	if err := srv.s.Batch(ops); err != nil {
		srv.handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"applied": len(ops)})
}

// Item is one scan result.
type Item struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (srv *Server) scan(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var start, end []byte
	if p := q.Get("prefix"); p != "" {
		start = []byte(p)
		end = prefixEnd([]byte(p))
	}
	if s := q.Get("start"); s != "" {
		start = []byte(s)
	}
	if e := q.Get("end"); e != "" {
		end = []byte(e)
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	// Fetch one extra to know whether there is a next page.
	kvs, err := srv.s.Scan(start, end, limit+1, consistency(r))
	if err != nil {
		srv.handleErr(w, r, err)
		return
	}
	out := struct {
		Items []Item `json:"items"`
		Next  string `json:"next,omitempty"`
	}{Items: []Item{}}
	for i, kv := range kvs {
		if i == limit {
			out.Next = string(kv.Key)
			break
		}
		out.Items = append(out.Items, Item{Key: string(kv.Key), Value: string(kv.Value)})
	}
	writeJSON(w, http.StatusOK, out)
}

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

func (srv *Server) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, srv.s.EngineMetrics())
}

func (srv *Server) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, srv.s.Status())
}

// JoinRequest is the body of POST /cluster/join.
type JoinRequest struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
}

func (srv *Server) join(w http.ResponseWriter, r *http.Request) {
	body, ok := srv.readBody(w, r)
	if !ok {
		return
	}
	var req JoinRequest
	if err := json.Unmarshal(body, &req); err != nil || req.ID == "" || req.RaftAddr == "" || req.HTTPAddr == "" {
		writeError(w, http.StatusBadRequest, "id, raft_addr and http_addr are required")
		return
	}
	if err := srv.s.Join(req.ID, req.RaftAddr, req.HTTPAddr); err != nil {
		srv.handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"joined": req.ID})
}

func (srv *Server) remove(w http.ResponseWriter, r *http.Request) {
	if err := srv.s.Remove(r.PathValue("id")); err != nil {
		srv.handleErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (srv *Server) snapshot(w http.ResponseWriter, _ *http.Request) {
	if err := srv.s.Snapshot(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
