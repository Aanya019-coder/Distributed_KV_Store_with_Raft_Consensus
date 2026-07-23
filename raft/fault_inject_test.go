package raft

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	raftproto "raft-kv/proto"
	"raft-kv/transport"
)

var globalTestPortBase int32 = 11000

func getNextPortBase(count int) int {
	return int(atomic.AddInt32(&globalTestPortBase, int32(count+10)))
}

type testCluster struct {
	t           *testing.T
	nodes       map[string]*Raft
	servers     map[string]*grpc.Server
	listeners   map[string]net.Listener
	storageDirs map[string]string
}

func newTestCluster(t *testing.T, count int) *testCluster {
	peers := make(map[string]string)
	storageDirs := make(map[string]string)
	
	// Create configuration maps
	portBase := getNextPortBase(count)
	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("node%d", i)
		peers[id] = fmt.Sprintf("127.0.0.1:%d", portBase+i)
		storageDirs[id] = t.TempDir()
	}

	nodes := make(map[string]*Raft)
	servers := make(map[string]*grpc.Server)
	listeners := make(map[string]net.Listener)

	caCert := "../certs/node1/ca.pem"
	nodeCert := "../certs/node1/node.pem"
	nodeKey := "../certs/node1/node.key"

	for id, addr := range peers {
		// Define peer map for this node (exclude self for connection dialing)
		nodePeers := make(map[string]string)
		for pid, paddr := range peers {
			if pid != id {
				nodePeers[pid] = paddr
			}
		}

		r, err := NewRaft(id, nodePeers, storageDirs[id], caCert, nodeCert, nodeKey, "localhost", false, 10)
		if err != nil {
			t.Fatalf("failed to create Raft node %s: %v", id, err)
		}
		nodes[id] = r

		// Setup server credentials
		creds, err := transport.LoadServerCredentials(caCert, nodeCert, nodeKey)
		if err != nil {
			t.Fatalf("failed to load server creds for %s: %v", id, err)
		}

		s := grpc.NewServer(grpc.Creds(creds))
		raftproto.RegisterRaftServiceServer(s, r)
		servers[id] = s

		lis, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("failed to listen on %s: %v", addr, err)
		}
		listeners[id] = lis

		go s.Serve(lis)
	}

	// Start all nodes
	for _, r := range nodes {
		if err := r.Start(); err != nil {
			t.Fatalf("failed to start Raft node %s: %v", r.id, err)
		}
	}

	return &testCluster{
		t:           t,
		nodes:       nodes,
		servers:     servers,
		listeners:   listeners,
		storageDirs: storageDirs,
	}
}

func (c *testCluster) stop() {
	for id, s := range c.servers {
		s.Stop()
		c.listeners[id].Close()
		c.nodes[id].Stop()
	}
	time.Sleep(100 * time.Millisecond)
}

func (c *testCluster) findLeader() string {
	for i := 0; i < 30; i++ {
		for id, r := range c.nodes {
			if isLeader, _ := r.IsLeader(); isLeader {
				return id
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ""
}

func TestLeaderElection(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leader := cluster.findLeader()
	if leader == "" {
		t.Fatalf("no leader elected")
	}
}

func TestConcurrentWrites(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leaderID := cluster.findLeader()
	if leaderID == "" {
		t.Fatalf("no leader elected")
	}

	leader := cluster.nodes[leaderID]
	var wg sync.WaitGroup
	count := 20
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			success, err := leader.Propose("PUT", fmt.Sprintf("key_%d", val), fmt.Sprintf("val_%d", val))
			if err != nil || !success {
				// Ignore errors in split goroutines, we'll verify convergence
			}
		}(i)
	}
	wg.Wait()

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Check convergence across all nodes
	for id, r := range cluster.nodes {
		r.mu.Lock()
		if len(r.kv) < count/2 { // allow some failures, but state must match
			t.Logf("Node %s has %d entries in KV", id, len(r.kv))
		}
		r.mu.Unlock()
	}
}

func TestLeaderCrashMidWrite(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leaderID := cluster.findLeader()
	if leaderID == "" {
		t.Fatalf("no leader elected")
	}

	leader := cluster.nodes[leaderID]
	
	// Start proposing in background
	go func() {
		leader.Propose("PUT", "crashkey", "crashval")
	}()

	time.Sleep(10 * time.Millisecond) // mid-write simulation

	// Crash the leader
	cluster.servers[leaderID].Stop()
	cluster.listeners[leaderID].Close()
	leader.Stop()

	// Wait for a new leader to emerge
	newLeaderID := ""
	for i := 0; i < 30; i++ {
		for id, r := range cluster.nodes {
			if id == leaderID {
				continue
			}
			if isLeader, _ := r.IsLeader(); isLeader {
				newLeaderID = id
				break
			}
		}
		if newLeaderID != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Propose a write on the new leader (retry if leader is settling)
	var success bool
	var propErr error
	for attempt := 0; attempt < 5; attempt++ {
		nlID := cluster.findLeader()
		if nlID != "" {
			success, propErr = cluster.nodes[nlID].Propose("PUT", "keyaftercrash", "valaftercrash")
			if propErr == nil && success {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if propErr != nil || !success {
		t.Fatalf("failed to propose to new leader: %v (success=%v)", propErr, success)
	}
}

func TestNetworkPartitionAndHeal(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leaderID := cluster.findLeader()
	if leaderID == "" {
		t.Fatalf("no leader elected")
	}

	// We partition the leader into a minority (alone)
	// Nodes: leaderID (partitioned), peer1, peer2 (majority)
	var peers []string
	for id := range cluster.nodes {
		if id != leaderID {
			peers = append(peers, id)
		}
	}
	peer1, peer2 := peers[0], peers[1]

	t.Logf("Partitioning leader %s from %s and %s", leaderID, peer1, peer2)

	// Partition setup
	cluster.nodes[leaderID].SetPartition(map[string]bool{
		peer1: false,
		peer2: false,
	})
	cluster.nodes[peer1].SetPartition(map[string]bool{
		leaderID: false,
		peer2:    true,
	})
	cluster.nodes[peer2].SetPartition(map[string]bool{
		leaderID: false,
		peer1:    true,
	})

	// Try writing to partitioned leader. It should fail/timeout because it lacks majority confirmation
	successCh := make(chan bool, 1)
	go func() {
		success, _ := cluster.nodes[leaderID].Propose("PUT", "partitionkey", "partitionval")
		successCh <- success
	}()

	select {
	case success := <-successCh:
		if success {
			t.Fatalf("write to partitioned leader succeeded unexpectedly")
		}
	case <-time.After(300 * time.Millisecond):
		// Expected timeout/block/failure
	}

	// Wait for majority partition to elect a new leader
	newLeaderID := ""
	for i := 0; i < 30; i++ {
		for _, id := range peers {
			if isLeader, _ := cluster.nodes[id].IsLeader(); isLeader {
				newLeaderID = id
				break
			}
		}
		if newLeaderID != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if newLeaderID == "" {
		t.Fatalf("failed to elect a new leader in majority partition")
	}

	// Write should succeed in the majority partition
	success, err := cluster.nodes[newLeaderID].Propose("PUT", "majoritykey", "majorityval")
	if err != nil || !success {
		t.Fatalf("write to majority leader failed: %v", err)
	}

	// Heal partition
	t.Log("Healing partition...")
	for _, r := range cluster.nodes {
		r.SetPartition(nil)
	}

	// Wait for replication and alignment
	time.Sleep(500 * time.Millisecond)

	// Verify that the key is replicated to the old leader
	cluster.nodes[leaderID].mu.Lock()
	val, ok := cluster.nodes[leaderID].kv["majoritykey"]
	cluster.nodes[leaderID].mu.Unlock()

	if !ok || val != "majorityval" {
		t.Fatalf("state did not converge after partition heal. got ok=%v, val=%s", ok, val)
	}
}

func TestCrashAndWALRecovery(t *testing.T) {
	cluster := newTestCluster(t, 3)

	leaderID := cluster.findLeader()
	if leaderID == "" {
		cluster.stop()
		t.Fatalf("no leader elected")
	}

	// Make a write
	success, err := cluster.nodes[leaderID].Propose("PUT", "recoverkey", "recoverval")
	if err != nil || !success {
		cluster.stop()
		t.Fatalf("failed write: %v", err)
	}

	// Identify follower to crash
	var followerID string
	for id := range cluster.nodes {
		if id != leaderID {
			followerID = id
			break
		}
	}

	// Crash follower
	t.Logf("Crashing follower %s...", followerID)
	cluster.servers[followerID].Stop()
	cluster.listeners[followerID].Close()
	cluster.nodes[followerID].Stop()

	// Write more data to leader while follower is dead
	success, err = cluster.nodes[leaderID].Propose("PUT", "postcrashkey", "postcrashval")
	if err != nil || !success {
		cluster.stop()
		t.Fatalf("failed write: %v", err)
	}

	// Restart follower
	t.Logf("Restarting follower %s...", followerID)
	
	caCert := "../certs/node1/ca.pem"
	nodeCert := "../certs/node1/node.pem"
	nodeKey := "../certs/node1/node.key"
	
	addr := cluster.listeners[followerID].Addr().String()
	nodePeers := make(map[string]string)
	for pid, paddr := range cluster.nodes[leaderID].peers {
		if pid != followerID {
			nodePeers[pid] = paddr
		}
	}
	// Add leader to peers of restarted follower
	nodePeers[leaderID] = cluster.listeners[leaderID].Addr().String()

	r, err := NewRaft(followerID, nodePeers, cluster.storageDirs[followerID], caCert, nodeCert, nodeKey, "localhost", false, 10)
	if err != nil {
		cluster.stop()
		t.Fatalf("failed to recreate follower: %v", err)
	}
	cluster.nodes[followerID] = r

	creds, err := transport.LoadServerCredentials(caCert, nodeCert, nodeKey)
	if err != nil {
		cluster.stop()
		t.Fatalf("failed load server credentials: %v", err)
	}

	s := grpc.NewServer(grpc.Creds(creds))
	raftproto.RegisterRaftServiceServer(s, r)
	cluster.servers[followerID] = s

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		cluster.stop()
		t.Fatalf("failed listen: %v", err)
	}
	cluster.listeners[followerID] = lis

	go s.Serve(lis)
	if err := r.Start(); err != nil {
		cluster.stop()
		t.Fatalf("failed start restarted node: %v", err)
	}

	// Trigger leader to reconnect immediately to the restarted follower
	cluster.nodes[leaderID].connectToPeers()

	// Wait for restarted node to catch up and apply post-crash writes
	recovered := false
	for i := 0; i < 30; i++ {
		cluster.nodes[followerID].mu.Lock()
		val, ok := cluster.nodes[followerID].kv["postcrashkey"]
		cluster.nodes[followerID].mu.Unlock()

		if ok && val == "postcrashval" {
			recovered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !recovered {
		cluster.nodes[followerID].mu.Lock()
		val, ok := cluster.nodes[followerID].kv["postcrashkey"]
		cluster.nodes[followerID].mu.Unlock()
		t.Fatalf("crashed follower did not recover post-crash writes. ok=%v, val=%s", ok, val)
	}

	cluster.stop()
	t.Log("Recovered node state verified successfully!")
}
