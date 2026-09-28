package cluster

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/MonishPuttu/Key-value-store/engine"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func fastRaft(c *raft.Config) {
	c.HeartbeatTimeout = 150 * time.Millisecond
	c.ElectionTimeout = 150 * time.Millisecond
	c.LeaderLeaseTimeout = 100 * time.Millisecond
	c.CommitTimeout = 5 * time.Millisecond
}

type testCluster struct {
	t     *testing.T
	root  string
	nodes map[string]*Store
}

func (tc *testCluster) open(id string, bootstrap bool, tune func(*raft.Config)) *Store {
	return tc.openAt(id, "127.0.0.1:0", bootstrap, tune)
}

func (tc *testCluster) openAt(id, bind string, bootstrap bool, tune func(*raft.Config)) *Store {
	tc.t.Helper()
	s, err := Open(Config{
		NodeID: id, DataDir: filepath.Join(tc.root, id), RaftBind: bind,
		HTTPAdvertise: "http://" + id + ".test", Bootstrap: bootstrap, Logger: quiet,
		RaftLogLevel: "error", Tune: tune, Engine: engine.Options{Logger: quiet},
	})
	if err != nil {
		tc.t.Fatal(err)
	}
	tc.nodes[id] = s
	return s
}

func newCluster(t *testing.T, n int) *testCluster {
	tc := &testCluster{t: t, root: t.TempDir(), nodes: map[string]*Store{}}
	t.Cleanup(func() {
		for _, s := range tc.nodes {
			_ = s.Close()
		}
	})
	n1 := tc.open("n1", true, fastRaft)
	waitUntil(t, 5*time.Second, n1.IsLeader, "n1 leader")
	for i := 2; i <= n; i++ {
		s := tc.open(fmt.Sprintf("n%d", i), false, fastRaft)
		if err := n1.Join(s.cfg.NodeID, s.RaftAddr(), s.cfg.HTTPAdvertise); err != nil {
			t.Fatal(err)
		}
	}
	return tc
}

func (tc *testCluster) leader() *Store {
	tc.t.Helper()
	var l *Store
	waitUntil(tc.t, 5*time.Second, func() bool {
		for _, s := range tc.nodes {
			if s.IsLeader() {
				l = s
				return true
			}
		}
		return false
	}, "a leader")
	return l
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func eventually(t *testing.T, s *Store, key, want string) {
	t.Helper()
	waitUntil(t, 5*time.Second, func() bool {
		v, err := s.Get([]byte(key), Stale)
		if want == "" {
			return errors.Is(err, engine.ErrNotFound)
		}
		return err == nil && string(v) == want
	}, fmt.Sprintf("%s: %s=%q", s.cfg.NodeID, key, want))
}

func TestReplicationAndConsistencyLevels(t *testing.T) {
	tc := newCluster(t, 3)
	l := tc.leader()
	if err := l.Set([]byte("color"), []byte("blue")); err != nil {
		t.Fatal(err)
	}
	for _, s := range tc.nodes {
		eventually(t, s, "color", "blue")
	}
	v, err := l.Get([]byte("color"), Strong)
	if err != nil || string(v) != "blue" {
		t.Fatalf("strong read on leader: %q %v", v, err)
	}
	for id, s := range tc.nodes {
		if s == l {
			continue
		}
		if _, err := s.Get([]byte("color"), Strong); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("%s: strong read on follower should fail with ErrNotLeader, got %v", id, err)
		}
		if err := s.Set([]byte("x"), []byte("y")); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("%s: write on follower should fail with ErrNotLeader, got %v", id, err)
		}
	}
	if err := l.Delete([]byte("color")); err != nil {
		t.Fatal(err)
	}
	for _, s := range tc.nodes {
		eventually(t, s, "color", "")
	}
	st := l.Status()
	if len(st.Nodes) != 3 || st.State != "Leader" {
		t.Fatalf("status %+v", st)
	}
	if _, addr, err := tc.nodes["n2"].LeaderHTTP(); err != nil || addr == "" {
		t.Fatalf("followers must know the leader's HTTP address: %q %v", addr, err)
	}
}

func TestCASAndBatch(t *testing.T) {
	tc := newCluster(t, 1)
	l := tc.leader()
	res, err := l.CAS([]byte("lock"), nil, []byte("owner-a"), true)
	if err != nil || !res.Swapped {
		t.Fatalf("create-if-absent: %+v %v", res, err)
	}
	res, _ = l.CAS([]byte("lock"), nil, []byte("owner-b"), true)
	if res.Swapped || string(res.Current) != "owner-a" {
		t.Fatalf("second create must fail: %+v", res)
	}
	res, _ = l.CAS([]byte("lock"), []byte("owner-a"), []byte("owner-b"), false)
	if !res.Swapped {
		t.Fatalf("swap with correct expected value: %+v", res)
	}
	err = l.Batch([]Command{
		{Op: OpSet, Key: []byte("a"), Value: []byte("1")},
		{Op: OpSet, Key: []byte("b"), Value: []byte("2")},
		{Op: OpDelete, Key: []byte("lock")},
	})
	if err != nil {
		t.Fatal(err)
	}
	kvs, _ := l.Scan(nil, nil, 0, Strong)
	if len(kvs) != 2 || string(kvs[0].Key) != "a" || string(kvs[1].Value) != "2" {
		t.Fatalf("scan after batch: %v", kvs)
	}
}

func TestLeaderFailover(t *testing.T) {
	tc := newCluster(t, 3)
	old := tc.leader()
	for i := 0; i < 50; i++ {
		if err := old.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	oldID, oldAddr := old.cfg.NodeID, old.RaftAddr()
	_ = old.Close()
	delete(tc.nodes, oldID)

	nl := tc.leader()
	if nl.cfg.NodeID == oldID {
		t.Fatal("old leader still reported")
	}
	// Committed data survives; the cluster (2 of 3) accepts new writes.
	for i := 0; i < 50; i++ {
		if v, err := nl.Get([]byte(fmt.Sprintf("k%02d", i)), Strong); err != nil || string(v) != "v" {
			t.Fatalf("lost k%02d after failover: %v", i, err)
		}
	}
	if err := nl.Set([]byte("after"), []byte("failover")); err != nil {
		t.Fatal(err)
	}

	// The old node restarts from its data dir and catches up.
	back := tc.openAt(oldID, oldAddr, false, fastRaft) // same identity and address
	eventually(t, back, "after", "failover")
	eventually(t, back, "k49", "v")
}

// A node that joins after the log was truncated must be brought up to date
// with an InstallSnapshot → FSM.Restore.
func TestNewNodeCatchesUpFromSnapshot(t *testing.T) {
	tc := &testCluster{t: t, root: t.TempDir(), nodes: map[string]*Store{}}
	t.Cleanup(func() {
		for _, s := range tc.nodes {
			_ = s.Close()
		}
	})
	tune := func(c *raft.Config) {
		fastRaft(c)
		c.TrailingLogs = 5
		c.SnapshotThreshold = 20
	}
	n1 := tc.open("n1", true, tune)
	waitUntil(t, 5*time.Second, n1.IsLeader, "leader")
	for i := 0; i < 300; i++ {
		if err := n1.Set([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := n1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	n2 := tc.open("n2", false, tune)
	if err := n1.Join("n2", n2.RaftAddr(), "http://n2.test"); err != nil {
		t.Fatal(err)
	}
	eventually(t, n2, "key-000", "0")
	eventually(t, n2, "key-299", "299")
	kvs, _ := n2.Scan(nil, nil, 0, Stale)
	if len(kvs) != 300 {
		t.Fatalf("n2 has %d keys after snapshot install, want 300", len(kvs))
	}
}

// Restarting a node must not re-apply entries it already applied.
func TestRestartDoesNotReapply(t *testing.T) {
	tc := newCluster(t, 1)
	l := tc.leader()
	_ = l.Set([]byte("n"), []byte("1"))
	applied, addr := l.fsm.appliedIndex(), l.RaftAddr()
	_ = l.Close()
	delete(tc.nodes, "n1")
	s := tc.openAt("n1", addr, true, fastRaft)
	if got := s.fsm.appliedIndex(); got != applied {
		t.Fatalf("applied index after restart %d, want %d", got, applied)
	}
	waitUntil(t, 5*time.Second, s.IsLeader, "leader after restart")
	if v, err := s.Get([]byte("n"), Strong); err != nil || string(v) != "1" {
		t.Fatalf("%q %v", v, err)
	}
}
