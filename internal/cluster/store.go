// Package cluster replicates the LSM engine across nodes with Raft
// (hashicorp/raft). Every write is a Raft log entry; once a majority has
// persisted it, each node applies it to its own engine in log order, so all
// replicas converge to the same state.
package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/MonishPuttu/Key-value-store/engine"
)

// Consistency selects the read guarantee.
type Consistency string

const (
	// Strong (linearizable): the leader confirms it is still leader with a
	// quorum and waits until it has applied everything committed before the
	// read began (the Raft "ReadIndex" technique).
	Strong Consistency = "strong"
	// Leader reads the leader's local state without the quorum check. Fast,
	// but a deposed leader that has not noticed yet may return stale data.
	Leader Consistency = "leader"
	// Stale reads any node's local state. Fastest; may lag the leader.
	Stale Consistency = "stale"
)

// Errors.
var (
	ErrNotLeader = errors.New("cluster: not the leader")
	ErrNoLeader  = errors.New("cluster: no leader elected")
)

// Config configures a Store.
type Config struct {
	NodeID        string
	DataDir       string
	RaftBind      string // e.g. 0.0.0.0:7000
	RaftAdvertise string // address other nodes use to reach us; defaults to RaftBind
	HTTPAdvertise string // this node's HTTP API address, shared with the cluster
	Bootstrap     bool   // create a new single-node cluster if no state exists
	// SnapshotThreshold is the number of log entries between snapshots.
	SnapshotThreshold uint64
	// ApplyTimeout bounds how long a write waits for commit.
	ApplyTimeout time.Duration
	Engine       engine.Options
	Logger       *slog.Logger
	// RaftLogLevel for hashicorp/raft's own logging (default "warn").
	RaftLogLevel string
	// Tune, if set, may adjust the Raft configuration (timeouts in tests).
	Tune func(*raft.Config)
}

// Store is a replicated key-value store node.
type Store struct {
	cfg   Config
	raft  *raft.Raft
	fsm   *fsm
	trans *raft.NetworkTransport
	logs  *raftboltdb.BoltStore
	log   *slog.Logger
	stop  chan struct{}
}

// Open starts the node. With Bootstrap on a fresh data directory it forms a
// one-node cluster; otherwise it rejoins its existing cluster (or waits to
// be added by a leader).
func Open(cfg Config) (*Store, error) {
	if cfg.NodeID == "" || cfg.DataDir == "" || cfg.RaftBind == "" {
		return nil, errors.New("cluster: NodeID, DataDir and RaftBind are required")
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 8192
	}
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RaftLogLevel == "" {
		cfg.RaftLogLevel = "warn"
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	cfg.Engine.Logger = cfg.Logger
	f, err := openFSM(filepath.Join(cfg.DataDir, "data"), cfg.Engine, cfg.Logger)
	if err != nil {
		return nil, err
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.SnapshotThreshold = cfg.SnapshotThreshold
	// Our FSM is durable and records its applied index, so it does not need
	// to be rebuilt from the snapshot on every restart.
	rc.NoSnapshotRestoreOnStart = true
	rc.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.LevelFromString(cfg.RaftLogLevel), Output: os.Stderr})
	if cfg.Tune != nil {
		cfg.Tune(rc)
	}

	// With no advertise address the transport advertises its listener
	// address (fine for 127.0.0.1 or a concrete IP; not for 0.0.0.0).
	var advertise net.Addr
	if cfg.RaftAdvertise != "" {
		a, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertise)
		if err != nil {
			f.close()
			return nil, fmt.Errorf("resolve raft advertise address: %w", err)
		}
		advertise = a
	}
	trans, err := raft.NewTCPTransport(cfg.RaftBind, advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		f.close()
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		f.close()
		trans.Close()
		return nil, err
	}
	logs, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.DataDir, "raft.db")})
	if err != nil {
		f.close()
		trans.Close()
		return nil, err
	}
	r, err := raft.NewRaft(rc, f, logs, logs, snaps, trans)
	if err != nil {
		f.close()
		trans.Close()
		logs.Close()
		return nil, err
	}
	s := &Store{cfg: cfg, raft: r, fsm: f, trans: trans, logs: logs, log: cfg.Logger, stop: make(chan struct{})}

	if cfg.Bootstrap {
		has, err := raft.HasExistingState(logs, logs, snaps)
		if err != nil {
			return nil, err
		}
		if !has {
			s.log.Info("bootstrapping new cluster", "node", cfg.NodeID)
			fut := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
				{ID: rc.LocalID, Address: trans.LocalAddr()},
			}})
			if err := fut.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
				return nil, err
			}
		}
	}
	go s.watchLeadership()
	return s, nil
}

// watchLeadership registers this node's HTTP address whenever it becomes
// leader, so every node can find (and forward to) the leader's API.
func (s *Store) watchLeadership() {
	for {
		select {
		case <-s.stop:
			return
		case isLeader := <-s.raft.LeaderCh():
			if !isLeader {
				continue
			}
			s.log.Info("became leader", "node", s.cfg.NodeID)
			if s.cfg.HTTPAdvertise != "" {
				if err := s.SetNodeAddr(s.cfg.NodeID, s.cfg.HTTPAdvertise); err != nil {
					s.log.Warn("could not register http address", "err", err)
				}
			}
		}
	}
}

// Close shuts the node down.
func (s *Store) Close() error {
	close(s.stop)
	err := s.raft.Shutdown().Error()
	_ = s.trans.Close()
	_ = s.logs.Close()
	if e := s.fsm.close(); err == nil && !errors.Is(e, engine.ErrClosed) {
		err = e
	}
	return err
}

// RaftAddr is the address other nodes use to reach this node's Raft transport.
func (s *Store) RaftAddr() string { return string(s.trans.LocalAddr()) }

// IsLeader reports whether this node is the leader.
func (s *Store) IsLeader() bool { return s.raft.State() == raft.Leader }

// LeaderHTTP returns the leader's node id and HTTP address.
func (s *Store) LeaderHTTP() (id, addr string, err error) {
	_, lid := s.raft.LeaderWithID()
	if lid == "" {
		return "", "", ErrNoLeader
	}
	addr, ok := s.fsm.nodeAddrs()[string(lid)]
	if !ok {
		return string(lid), "", fmt.Errorf("cluster: leader %s has not registered its address yet", lid)
	}
	return string(lid), addr, nil
}

// apply replicates a command through Raft (leader only).
func (s *Store) apply(c Command) (*ApplyResult, error) {
	if !s.IsLeader() {
		return nil, ErrNotLeader
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	fut := s.raft.Apply(b, s.cfg.ApplyTimeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return nil, ErrNotLeader
		}
		return nil, err
	}
	res, _ := fut.Response().(*ApplyResult)
	if res == nil {
		return &ApplyResult{}, nil
	}
	return res, res.Err
}

// Set writes key=value.
func (s *Store) Set(key, value []byte) error {
	_, err := s.apply(Command{Op: OpSet, Key: key, Value: value})
	return err
}

// Delete removes key.
func (s *Store) Delete(key []byte) error {
	_, err := s.apply(Command{Op: OpDelete, Key: key})
	return err
}

// CAS sets key to value only if its current value equals expected (or, with
// expectAbsent, if it does not exist). It is linearizable because the check
// happens when the entry is applied, in log order, on every replica.
func (s *Store) CAS(key, expected, value []byte, expectAbsent bool) (*ApplyResult, error) {
	return s.apply(Command{Op: OpCAS, Key: key, Expected: expected, ExpectedAbsent: expectAbsent, Value: value})
}

// Batch applies several set/delete operations atomically.
func (s *Store) Batch(ops []Command) error {
	_, err := s.apply(Command{Op: OpBatch, Ops: ops})
	return err
}

// SetNodeAddr records a node's HTTP address in replicated metadata.
func (s *Store) SetNodeAddr(id, httpAddr string) error {
	_, err := s.apply(Command{Op: OpMeta, Key: []byte("node/" + id), Value: []byte(httpAddr)})
	return err
}

// Get reads key with the requested consistency.
func (s *Store) Get(key []byte, c Consistency) ([]byte, error) {
	if err := s.readBarrier(c); err != nil {
		return nil, err
	}
	return s.fsm.get(key)
}

// Scan reads a key range with the requested consistency.
func (s *Store) Scan(start, end []byte, limit int, c Consistency) ([]engine.KV, error) {
	if err := s.readBarrier(c); err != nil {
		return nil, err
	}
	return s.fsm.scan(start, end, limit)
}

// readBarrier implements the consistency levels.
func (s *Store) readBarrier(c Consistency) error {
	switch c {
	case Stale:
		return nil
	case Leader:
		if !s.IsLeader() {
			return ErrNotLeader
		}
		return nil
	default: // Strong: ReadIndex
		if !s.IsLeader() {
			return ErrNotLeader
		}
		readIndex := s.raft.CommitIndex()
		if err := s.raft.VerifyLeader().Error(); err != nil {
			return ErrNotLeader
		}
		deadline := time.Now().Add(s.cfg.ApplyTimeout)
		for s.fsm.appliedIndex() < readIndex {
			if time.Now().After(deadline) {
				return errors.New("cluster: timed out waiting for state machine to catch up")
			}
			time.Sleep(time.Millisecond)
		}
		return nil
	}
}

// Join adds a node as a voter (leader only). Idempotent.
func (s *Store) Join(id, raftAddr, httpAddr string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}
	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		return err
	}
	for _, srv := range cf.Configuration().Servers {
		if srv.ID == raft.ServerID(id) && srv.Address == raft.ServerAddress(raftAddr) {
			return s.SetNodeAddr(id, httpAddr) // already a member
		}
		if srv.ID == raft.ServerID(id) || srv.Address == raft.ServerAddress(raftAddr) {
			if err := s.raft.RemoveServer(srv.ID, 0, 0).Error(); err != nil {
				return fmt.Errorf("remove stale member %s: %w", srv.ID, err)
			}
		}
	}
	if err := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(raftAddr), 0, 0).Error(); err != nil {
		return err
	}
	s.log.Info("node joined", "id", id, "raft", raftAddr, "http", httpAddr)
	return s.SetNodeAddr(id, httpAddr)
}

// Remove removes a node from the cluster (leader only).
func (s *Store) Remove(id string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}
	if err := s.raft.RemoveServer(raft.ServerID(id), 0, 0).Error(); err != nil {
		return err
	}
	_, err := s.apply(Command{Op: OpMeta, Key: []byte("node/" + id), Delete: true})
	return err
}

// Node describes a cluster member.
type Node struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr,omitempty"`
	Suffrage string `json:"suffrage"`
	Leader   bool   `json:"leader"`
}

// Status describes this node's view of the cluster.
type Status struct {
	NodeID       string            `json:"node_id"`
	State        string            `json:"state"`
	LeaderID     string            `json:"leader_id"`
	Term         string            `json:"term"`
	CommitIndex  uint64            `json:"commit_index"`
	AppliedIndex uint64            `json:"applied_index"`
	LastContact  string            `json:"last_contact"`
	Nodes        []Node            `json:"nodes"`
	Raft         map[string]string `json:"raft"`
}

// Status returns cluster status.
func (s *Store) Status() Status {
	_, lid := s.raft.LeaderWithID()
	st := s.raft.Stats()
	out := Status{
		NodeID: s.cfg.NodeID, State: s.raft.State().String(), LeaderID: string(lid),
		Term: st["term"], CommitIndex: s.raft.CommitIndex(), AppliedIndex: s.fsm.appliedIndex(),
		LastContact: st["last_contact"], Raft: st,
	}
	addrs := s.fsm.nodeAddrs()
	if cf := s.raft.GetConfiguration(); cf.Error() == nil {
		for _, srv := range cf.Configuration().Servers {
			out.Nodes = append(out.Nodes, Node{
				ID: string(srv.ID), RaftAddr: string(srv.Address), HTTPAddr: addrs[string(srv.ID)],
				Suffrage: srv.Suffrage.String(), Leader: srv.ID == lid,
			})
		}
	}
	return out
}

// EngineMetrics exposes the local engine's statistics.
func (s *Store) EngineMetrics() engine.Metrics { return s.fsm.metrics() }

// Snapshot forces a Raft snapshot (and log truncation).
func (s *Store) Snapshot() error { return s.raft.Snapshot().Error() }

// WaitForLeader blocks until a leader is known or the timeout elapses.
func (s *Store) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, id := s.raft.LeaderWithID(); id != "" {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ErrNoLeader
}

var _ raft.BatchingFSM = (*fsm)(nil)
var _ io.Closer = (*Store)(nil)
