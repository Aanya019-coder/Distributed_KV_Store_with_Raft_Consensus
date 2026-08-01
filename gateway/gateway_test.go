package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- Mock node helpers ----

type mockNode struct {
	server  *httptest.Server
	status  NodeStatus
	metrics string
	mu      sync.RWMutex
}

func newMockNode(status NodeStatus) *mockNode {
	n := &mockNode{status: status, metrics: "raft_current_term 1\nraft_role 2\n"}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		n.mu.RLock()
		s := n.status
		n.mu.RUnlock()
		json.NewEncoder(w).Encode(s)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		n.mu.RLock()
		m := n.metrics
		n.mu.RUnlock()
		w.Write([]byte(m))
	})
	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		n.mu.RLock()
		role := n.status.Role
		n.mu.RUnlock()
		if role != "leader" {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"not the leader"}`))
			return
		}
		if r.Method == http.MethodGet {
			w.Write([]byte(`{"key":"testkey","value":"testvalue"}`))
		} else {
			w.Write([]byte(`{"status":"ok"}`))
		}
	})
	n.server = httptest.NewServer(mux)
	return n
}

func (n *mockNode) addr() string {
	return n.server.Listener.Addr().String()
}

func (n *mockNode) setRole(role string) {
	n.mu.Lock()
	n.status.Role = role
	n.mu.Unlock()
}

func (n *mockNode) close() {
	n.server.Close()
}

// ---- Tests ----

func TestLeaderTracking(t *testing.T) {
	t.Parallel()

	node1 := newMockNode(NodeStatus{NodeID: "node1", Role: "leader", CurrentTerm: 1})
	node2 := newMockNode(NodeStatus{NodeID: "node2", Role: "follower", CurrentTerm: 1})
	node3 := newMockNode(NodeStatus{NodeID: "node3", Role: "follower", CurrentTerm: 1})
	defer node1.close()
	defer node2.close()
	defer node3.close()

	gw := New(Config{
		NodeAddrs:    []string{node1.addr(), node2.addr(), node3.addr()},
		PollInterval: 100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gw.Start(ctx)

	// Wait for initial poll
	time.Sleep(200 * time.Millisecond)

	gw.mu.RLock()
	leader := gw.leaderID
	leaderAddr := gw.leaderAddr
	gw.mu.RUnlock()

	if leader != "node1" {
		t.Errorf("expected leader node1, got %s", leader)
	}
	if leaderAddr != node1.addr() {
		t.Errorf("expected leaderAddr %s, got %s", node1.addr(), leaderAddr)
	}

	// Switch leader: node1 becomes follower, node2 becomes leader
	node1.setRole("follower")
	node2.mu.Lock()
	node2.status.Role = "leader"
	node2.status.CurrentTerm = 2
	node2.mu.Unlock()

	// Wait for re-election to be detected
	time.Sleep(400 * time.Millisecond)

	gw.mu.RLock()
	newLeader := gw.leaderID
	gw.mu.RUnlock()

	if newLeader != "node2" {
		t.Errorf("expected leader to switch to node2, got %s", newLeader)
	}
}

func TestKVProxy_WritesToLeader(t *testing.T) {
	t.Parallel()

	var leaderHit atomic.Int32

	// Leader mock
	leaderMux := http.NewServeMux()
	leaderMux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(NodeStatus{NodeID: "node1", Role: "leader", CurrentTerm: 1})
	})
	leaderMux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		leaderHit.Add(1)
		w.Write([]byte(`{"status":"ok"}`))
	})
	leaderServer := httptest.NewServer(leaderMux)
	defer leaderServer.Close()

	// Follower mock
	followerMux := http.NewServeMux()
	followerMux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(NodeStatus{NodeID: "node2", Role: "follower", CurrentTerm: 1})
	})
	followerServer := httptest.NewServer(followerMux)
	defer followerServer.Close()

	gw := New(Config{
		NodeAddrs:    []string{leaderServer.Listener.Addr().String(), followerServer.Listener.Addr().String()},
		PollInterval: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gw.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	gwServer := httptest.NewServer(gw.Handler())
	defer gwServer.Close()

	req, _ := http.NewRequest(http.MethodPut, gwServer.URL+"/kv/mykey", strings.NewReader("myvalue"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if leaderHit.Load() == 0 {
		t.Error("expected PUT to reach the leader node")
	}
}

func TestSSEPush(t *testing.T) {
	t.Parallel()

	node1 := newMockNode(NodeStatus{NodeID: "node1", Role: "leader", CurrentTerm: 1})
	defer node1.close()

	gw := New(Config{
		NodeAddrs:    []string{node1.addr()},
		PollInterval: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gw.Start(ctx)

	gwServer := httptest.NewServer(gw.Handler())
	defer gwServer.Close()

	sseResp, err := http.Get(gwServer.URL + "/cluster/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("SSE endpoint returned %d", sseResp.StatusCode)
	}
	ct := sseResp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/event-stream") {
		t.Errorf("expected text/event-stream, got %s", ct)
	}

	buf := make([]byte, 4096)
	done := make(chan string, 1)
	go func() {
		n, _ := sseResp.Body.Read(buf)
		done <- string(buf[:n])
	}()

	select {
	case data := <-done:
		if !strings.Contains(data, "state_update") {
			t.Errorf("expected state_update event, got: %s", data)
		}
	case <-time.After(2 * time.Second):
		t.Error("SSE: timed out waiting for initial state_update event")
	}
}

func TestOverviewEndpoint(t *testing.T) {
	t.Parallel()

	node1 := newMockNode(NodeStatus{NodeID: "node1", Role: "leader", CurrentTerm: 5, CommitIndex: 42})
	node2 := newMockNode(NodeStatus{NodeID: "node2", Role: "follower", CurrentTerm: 5})
	defer node1.close()
	defer node2.close()

	gw := New(Config{
		NodeAddrs:    []string{node1.addr(), node2.addr()},
		PollInterval: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gw.Start(ctx)
	time.Sleep(250 * time.Millisecond)

	gwServer := httptest.NewServer(gw.Handler())
	defer gwServer.Close()

	resp, err := http.Get(gwServer.URL + "/cluster/overview")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var overview ClusterOverview
	if err := json.NewDecoder(resp.Body).Decode(&overview); err != nil {
		t.Fatal(err)
	}

	if overview.LeaderID != "node1" {
		t.Errorf("expected LeaderID=node1, got %s", overview.LeaderID)
	}
	if len(overview.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(overview.Nodes))
	}
}

func TestAuth_RejectsUnauthorized(t *testing.T) {
	t.Parallel()

	node1 := newMockNode(NodeStatus{NodeID: "node1", Role: "leader"})
	defer node1.close()

	gw := New(Config{
		NodeAddrs:    []string{node1.addr()},
		GatewayToken: "secret-token",
		PollInterval: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gw.Start(ctx)
	time.Sleep(150 * time.Millisecond)

	gwServer := httptest.NewServer(gw.Handler())
	defer gwServer.Close()

	// PUT without token — should be 401
	req, _ := http.NewRequest(http.MethodPut, gwServer.URL+"/kv/key", strings.NewReader("val"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}

	// PUT with correct token — should succeed
	req2, _ := http.NewRequest(http.MethodPut, gwServer.URL+"/kv/key", strings.NewReader("val"))
	req2.Header.Set("Authorization", "Bearer secret-token")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode == http.StatusUnauthorized {
		t.Error("expected authorized request to succeed")
	}
}

// Ensure unused imports from fmt/io/net don't cause compile errors
var (
	_ = fmt.Sprintf
	_ = io.Discard
	_ = net.Listen
)
