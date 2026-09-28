package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/MonishPuttu/Key-value-store/engine"
	"github.com/MonishPuttu/Key-value-store/internal/cluster"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type node struct {
	store *cluster.Store
	ts    *httptest.Server
}

func startNode(t *testing.T, root, id string, bootstrap bool) *node {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + ln.Addr().String()
	st, err := cluster.Open(cluster.Config{
		NodeID: id, DataDir: filepath.Join(root, id), RaftBind: "127.0.0.1:0", HTTPAdvertise: url,
		Bootstrap: bootstrap, Logger: quiet, RaftLogLevel: "error", Engine: engine.Options{Logger: quiet},
		Tune: func(c *raft.Config) {
			c.HeartbeatTimeout, c.ElectionTimeout, c.LeaderLeaseTimeout = 150*time.Millisecond, 150*time.Millisecond, 100*time.Millisecond
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(New(Options{Store: st, Logger: quiet}))
	ts.Listener.Close()
	ts.Listener = ln
	ts.Start()
	t.Cleanup(func() { ts.Close(); _ = st.Close() })
	return &node{store: st, ts: ts}
}

func req(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	r, _ := http.NewRequest(method, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func waitReady(t *testing.T, n *node) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if code, _ := req(t, "GET", n.ts.URL+"/readyz", ""); code == 200 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("node not ready")
}

func TestKVEndpoints(t *testing.T) {
	n := startNode(t, t.TempDir(), "n1", true)
	waitReady(t, n)
	u := n.ts.URL

	if code, _ := req(t, "PUT", u+"/v1/kv/users/42", `{"name":"ada"}`); code != 204 {
		t.Fatalf("put: %d", code)
	}
	if code, body := req(t, "GET", u+"/v1/kv/users/42", ""); code != 200 || body != `{"name":"ada"}` {
		t.Fatalf("get: %d %q", code, body)
	}
	if code, _ := req(t, "GET", u+"/v1/kv/nope", ""); code != 404 {
		t.Fatalf("missing: %d", code)
	}
	if code, body := req(t, "POST", u+"/v1/cas/lock", `{"expected_absent":true,"value":"a"}`); code != 200 {
		t.Fatalf("cas create: %d %s", code, body)
	}
	if code, body := req(t, "POST", u+"/v1/cas/lock", `{"expected":"zzz","value":"b"}`); code != 409 || !strings.Contains(body, `"current":"a"`) {
		t.Fatalf("cas conflict: %d %s", code, body)
	}
	if code, body := req(t, "POST", u+"/v1/batch", `{"ops":[{"op":"set","key":"users/1","value":"x"},{"op":"set","key":"users/2","value":"y"},{"op":"delete","key":"lock"}]}`); code != 200 {
		t.Fatalf("batch: %d %s", code, body)
	}
	code, body := req(t, "GET", u+"/v1/scan?prefix=users/&limit=2", "")
	var out struct {
		Items []Item
		Next  string
	}
	_ = json.Unmarshal([]byte(body), &out)
	if code != 200 || len(out.Items) != 2 || out.Items[0].Key != "users/1" || out.Next != "users/42" {
		t.Fatalf("scan: %d %s", code, body)
	}
	if code, _ := req(t, "DELETE", u+"/v1/kv/users/42", ""); code != 204 {
		t.Fatalf("delete: %d", code)
	}
	for _, p := range []string{"/cluster/status", "/v1/stats", "/metrics", "/healthz"} {
		if code, _ := req(t, "GET", u+p, ""); code != 200 {
			t.Fatalf("%s: %d", p, code)
		}
	}
	if code, _ := req(t, "POST", u+"/v1/cas/k", `{"value":"x"}`); code != 400 {
		t.Fatalf("cas without expected should be 400, got %d", code)
	}
}

// Writes and strong reads sent to a follower are forwarded to the leader.
func TestFollowerForwardsToLeader(t *testing.T) {
	root := t.TempDir()
	n1 := startNode(t, root, "n1", true)
	waitReady(t, n1)
	n2 := startNode(t, root, "n2", false)
	body := fmt.Sprintf(`{"id":"n2","raft_addr":%q,"http_addr":%q}`, n2.store.RaftAddr(), n2.ts.URL)
	// Join via the follower-to-be's peer: any node accepts /cluster/join.
	if code, b := req(t, "POST", n1.ts.URL+"/cluster/join", body); code != 200 {
		t.Fatalf("join: %d %s", code, b)
	}
	waitReady(t, n2)

	if code, b := req(t, "PUT", n2.ts.URL+"/v1/kv/greeting", "hello"); code != 204 {
		t.Fatalf("write via follower: %d %s", code, b)
	}
	if code, b := req(t, "GET", n2.ts.URL+"/v1/kv/greeting", ""); code != 200 || b != "hello" {
		t.Fatalf("strong read via follower: %d %q", code, b)
	}
	// A stale read is served locally by the follower (eventually consistent).
	deadline := time.Now().Add(3 * time.Second)
	for {
		code, b := req(t, "GET", n2.ts.URL+"/v1/kv/greeting?consistency=stale", "")
		if code == 200 && b == "hello" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale read never converged: %d %q", code, b)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var st cluster.Status
	_, b := req(t, "GET", n2.ts.URL+"/cluster/status", "")
	_ = json.Unmarshal([]byte(b), &st)
	if len(st.Nodes) != 2 || st.LeaderID != "n1" {
		t.Fatalf("status: %s", b)
	}
}
