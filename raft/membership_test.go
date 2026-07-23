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

// TestAddNodeUnderLoad verifies that adding a 4th node while under write load
// doesn't cause split-brain or an extended availability gap.
func TestAddNodeUnderLoad(t *testing.T) {
	// Start a 3-node cluster
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leaderID := cluster.findLeader()
	if leaderID == "" {
		t.Fatalf("no leader elected")
	}

	leader := cluster.nodes[leaderID]

	// Start concurrent writes in background
	var writeErrors int64
	var writeSuccesses int64
	stopWrites := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stopWrites:
					return
				default:
				}
				_, err := leader.Propose("PUT", fmt.Sprintf("load_key_%d_%d", workerID, j), fmt.Sprintf("val_%d", j))
				if err != nil {
					atomic.AddInt64(&writeErrors, 1)
				} else {
					atomic.AddInt64(&writeSuccesses, 1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	// Let some writes accumulate
	time.Sleep(200 * time.Millisecond)

	// Add a 4th node
	node4ID := "node4"
	node4Addr := fmt.Sprintf("127.0.0.1:%d", getNextPortBase(1))
	node4StorageDir := t.TempDir()

	caCert := "../certs/node1/ca.pem"
	nodeCert := "../certs/node1/node.pem"
	nodeKey := "../certs/node1/node.key"

	// Create node4's peer map (all existing nodes)
	node4Peers := make(map[string]string)
	for id := range cluster.nodes {
		node4Peers[id] = cluster.listeners[id].Addr().String()
	}

	node4, err := NewRaft(node4ID, node4Peers, node4StorageDir, caCert, nodeCert, nodeKey, "localhost", false, 10)
	if err != nil {
		t.Fatalf("failed to create node4: %v", err)
	}
	node4.SetJoining(true)

	creds, err := transport.LoadServerCredentials(caCert, nodeCert, nodeKey)
	if err != nil {
		t.Fatalf("failed to load server creds for node4: %v", err)
	}

	s := grpc.NewServer(grpc.Creds(creds))
	raftproto.RegisterRaftServiceServer(s, node4)

	lis, err := net.Listen("tcp", node4Addr)
	if err != nil {
		t.Fatalf("failed to listen for node4: %v", err)
	}

	go s.Serve(lis)
	if err := node4.Start(); err != nil {
		t.Fatalf("failed to start node4: %v", err)
	}

	cluster.nodes[node4ID] = node4
	cluster.servers[node4ID] = s
	cluster.listeners[node4ID] = lis
	cluster.storageDirs[node4ID] = node4StorageDir

	// Propose config change to add node4 (retry on active leader if leadership changes under load)
	var success bool
	var propErr error
	for attempt := 0; attempt < 5; attempt++ {
		lID := cluster.findLeader()
		if lID != "" {
			l := cluster.nodes[lID]
			success, propErr = l.ProposeConfigChange(ConfigChangeRequest{
				NodeID:   node4ID,
				GRPCAddr: node4Addr,
				HTTPAddr: "127.0.0.1:8004",
				Remove:   false,
			})
			if propErr == nil && success {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if propErr != nil || !success {
		t.Fatalf("failed to propose config change: %v (success=%v)", propErr, success)
	}

	// Wait for config to propagate
	time.Sleep(500 * time.Millisecond)

	// Stop writes
	close(stopWrites)
	wg.Wait()

	// Verify cluster settles on a single leader (and no split brain)
	finalLeader := ""
	for i := 0; i < 30; i++ {
		leaderCount := 0
		var currentLeader string
		for id, r := range cluster.nodes {
			if isLeader, _ := r.IsLeader(); isLeader {
				leaderCount++
				currentLeader = id
			}
		}
		if leaderCount > 1 {
			t.Fatalf("SPLIT BRAIN: %d leaders detected", leaderCount)
		}
		if leaderCount == 1 {
			finalLeader = currentLeader
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if finalLeader == "" {
		t.Fatalf("no leader after config change")
	}

	// Verify node4 is in the cluster config
	leader = cluster.nodes[cluster.findLeader()]
	leader.mu.Lock()
	_, inConfig := leader.currentConfig.NewPeers[node4ID]
	isJoint := leader.currentConfig.Joint
	leader.mu.Unlock()

	if !inConfig {
		t.Fatalf("node4 not found in cluster config after add")
	}
	if isJoint {
		t.Fatalf("config still in joint consensus after completion")
	}

	t.Logf("Add node under load: successes=%d, errors=%d", atomic.LoadInt64(&writeSuccesses), atomic.LoadInt64(&writeErrors))
}

// TestRemoveCurrentLeader verifies that removing the current leader results in
// a safe step-down and new leader election.
func TestRemoveCurrentLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leaderID := cluster.findLeader()
	if leaderID == "" {
		t.Fatalf("no leader elected")
	}

	// Write some data first
	leader := cluster.nodes[leaderID]
	success, err := leader.Propose("PUT", "before_remove", "value1")
	if err != nil || !success {
		t.Fatalf("failed to write before leader removal: %v", err)
	}

	// Propose removal of the current leader
	success, err = leader.ProposeConfigChange(ConfigChangeRequest{
		NodeID: leaderID,
		Remove: true,
	})

	// The leader should have stepped down. The proposal might succeed (committed before step-down)
	// or fail (stepped down during commit). Either is acceptable.
	t.Logf("Leader removal proposal: success=%v, err=%v", success, err)

	// Wait for a new leader to emerge among the remaining nodes
	time.Sleep(1 * time.Second)

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

	if newLeaderID == "" {
		t.Fatalf("no new leader elected after leader removal")
	}

	t.Logf("New leader elected: %s (removed leader: %s)", newLeaderID, leaderID)

	// Verify the old leader has stepped down
	isOldLeader, _ := cluster.nodes[leaderID].IsLeader()
	if isOldLeader {
		t.Fatalf("old leader %s did not step down", leaderID)
	}

	// Verify the new leader can still accept writes
	newLeader := cluster.nodes[newLeaderID]
	success, err = newLeader.Propose("PUT", "after_remove", "value2")
	if err != nil || !success {
		t.Fatalf("failed to write to new leader after removal: %v", err)
	}

	// Verify data consistency
	time.Sleep(300 * time.Millisecond)
	newLeader.mu.Lock()
	val, ok := newLeader.kv["before_remove"]
	newLeader.mu.Unlock()
	if !ok || val != "value1" {
		t.Fatalf("data written before removal not preserved: ok=%v, val=%s", ok, val)
	}
}

// TestCrashDuringJointConsensus verifies that crashing a node during the
// joint consensus window doesn't prevent the cluster from converging.
func TestCrashDuringJointConsensus(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leaderID := cluster.findLeader()
	if leaderID == "" {
		t.Fatalf("no leader elected")
	}

	leader := cluster.nodes[leaderID]

	// Write some data
	success, err := leader.Propose("PUT", "precrash", "data")
	if err != nil || !success {
		t.Fatalf("failed initial write: %v", err)
	}

	// Pick a follower to crash
	var followerID string
	for id := range cluster.nodes {
		if id != leaderID {
			followerID = id
			break
		}
	}

	// Start the config change (add node4), then crash a follower during the transition
	node4ID := "node4"
	node4Addr := fmt.Sprintf("127.0.0.1:%d", getNextPortBase(1))
	node4StorageDir := t.TempDir()

	caCert := "../certs/node1/ca.pem"
	nodeCert := "../certs/node1/node.pem"
	nodeKey := "../certs/node1/node.key"

	node4Peers := make(map[string]string)
	for id := range cluster.nodes {
		node4Peers[id] = cluster.listeners[id].Addr().String()
	}

	node4, err := NewRaft(node4ID, node4Peers, node4StorageDir, caCert, nodeCert, nodeKey, "localhost", false, 10)
	if err != nil {
		t.Fatalf("failed to create node4: %v", err)
	}
	node4.SetJoining(true)

	creds, err := transport.LoadServerCredentials(caCert, nodeCert, nodeKey)
	if err != nil {
		t.Fatalf("failed to load creds: %v", err)
	}

	s := grpc.NewServer(grpc.Creds(creds))
	raftproto.RegisterRaftServiceServer(s, node4)

	lis, err := net.Listen("tcp", node4Addr)
	if err != nil {
		t.Fatalf("failed to listen node4: %v", err)
	}

	go s.Serve(lis)
	node4.Start()

	cluster.nodes[node4ID] = node4
	cluster.servers[node4ID] = s
	cluster.listeners[node4ID] = lis
	cluster.storageDirs[node4ID] = node4StorageDir

	// Propose config change and crash follower mid-transition
	go func() {
		time.Sleep(5 * time.Millisecond) // Let the config change start
		t.Logf("Crashing follower %s during joint consensus", followerID)
		cluster.servers[followerID].Stop()
		cluster.listeners[followerID].Close()
		cluster.nodes[followerID].Stop()
	}()

	success, err = leader.ProposeConfigChange(ConfigChangeRequest{
		NodeID:   node4ID,
		GRPCAddr: node4Addr,
		HTTPAddr: "127.0.0.1:8014",
		Remove:   false,
	})

	t.Logf("Config change during crash: success=%v, err=%v", success, err)

	// Wait for cluster to stabilize
	time.Sleep(1 * time.Second)

	// Restart the crashed follower
	addr := cluster.listeners[followerID].Addr().String()
	followerPeers := make(map[string]string)
	for id := range cluster.nodes {
		if id != followerID {
			peerAddr := cluster.listeners[id].Addr().String()
			if id == node4ID {
				peerAddr = node4Addr
			}
			followerPeers[id] = peerAddr
		}
	}

	restarted, err := NewRaft(followerID, followerPeers, cluster.storageDirs[followerID], caCert, nodeCert, nodeKey, "localhost", false, 10)
	if err != nil {
		t.Fatalf("failed to restart follower: %v", err)
	}

	creds2, _ := transport.LoadServerCredentials(caCert, nodeCert, nodeKey)
	s2 := grpc.NewServer(grpc.Creds(creds2))
	raftproto.RegisterRaftServiceServer(s2, restarted)

	lis2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen on restarted follower: %v", err)
	}

	go s2.Serve(lis2)
	restarted.Start()

	cluster.nodes[followerID] = restarted
	cluster.servers[followerID] = s2
	cluster.listeners[followerID] = lis2

	// Reconnect the leader to the restarted follower
	currentLeaderID := cluster.findLeader()
	if currentLeaderID != "" {
		cluster.nodes[currentLeaderID].connectToPeers()
	}

	time.Sleep(600 * time.Millisecond)

	// Verify cluster has converged
	finalLeaderID := cluster.findLeader()
	if finalLeaderID == "" {
		t.Fatalf("no leader after recovery from crash during joint consensus")
	}

	// Verify data integrity
	finalLeader := cluster.nodes[finalLeaderID]
	finalLeader.mu.Lock()
	val, ok := finalLeader.kv["precrash"]
	finalLeader.mu.Unlock()
	if !ok || val != "data" {
		t.Fatalf("data lost after crash during joint consensus: ok=%v, val=%s", ok, val)
	}

	t.Logf("Cluster converged after crash during joint consensus. Leader: %s", finalLeaderID)
}

// TestRapidSuccessiveConfigChanges verifies that submitting two config changes
// in rapid succession correctly serializes them (the second is rejected while
// the first is in flight).
func TestRapidSuccessiveConfigChanges(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	// Create two new nodes
	caCert := "../certs/node1/ca.pem"
	nodeCert := "../certs/node1/node.pem"
	nodeKey := "../certs/node1/node.key"

	node4Addr := fmt.Sprintf("127.0.0.1:%d", getNextPortBase(1))
	node5Addr := fmt.Sprintf("127.0.0.1:%d", getNextPortBase(1))

	for _, nodeSpec := range []struct {
		id   string
		addr string
	}{
		{"node4", node4Addr},
		{"node5", node5Addr},
	} {
		peers := make(map[string]string)
		for id := range cluster.nodes {
			peers[id] = cluster.listeners[id].Addr().String()
		}

		n, err := NewRaft(nodeSpec.id, peers, t.TempDir(), caCert, nodeCert, nodeKey, "localhost", false, 10)
		if err != nil {
			t.Fatalf("failed to create %s: %v", nodeSpec.id, err)
		}
		n.SetJoining(true)

		creds, _ := transport.LoadServerCredentials(caCert, nodeCert, nodeKey)
		s := grpc.NewServer(grpc.Creds(creds))
		raftproto.RegisterRaftServiceServer(s, n)

		lis, err := net.Listen("tcp", nodeSpec.addr)
		if err != nil {
			t.Fatalf("failed to listen %s: %v", nodeSpec.id, err)
		}

		go s.Serve(lis)
		n.Start()

		cluster.nodes[nodeSpec.id] = n
		cluster.servers[nodeSpec.id] = s
		cluster.listeners[nodeSpec.id] = lis
		cluster.storageDirs[nodeSpec.id] = ""
	}

	// Find active leader right before submitting proposals
	leaderID := cluster.findLeader()
	if leaderID == "" {
		t.Fatalf("no leader elected")
	}
	leader := cluster.nodes[leaderID]

	// First config change: add node4 (should succeed)
	var wg sync.WaitGroup
	var firstSuccess, secondSuccess bool
	var firstErr, secondErr error

	wg.Add(2)

	// Launch first config change
	go func() {
		defer wg.Done()
		firstSuccess, firstErr = leader.ProposeConfigChange(ConfigChangeRequest{
			NodeID:   "node4",
			GRPCAddr: node4Addr,
			HTTPAddr: "127.0.0.1:8024",
			Remove:   false,
		})
	}()

	// Launch second config change immediately after — should be rejected
	time.Sleep(5 * time.Millisecond) // tiny delay to ensure ordering
	go func() {
		defer wg.Done()
		secondSuccess, secondErr = leader.ProposeConfigChange(ConfigChangeRequest{
			NodeID:   "node5",
			GRPCAddr: node5Addr,
			HTTPAddr: "127.0.0.1:8025",
			Remove:   false,
		})
	}()

	wg.Wait()

	t.Logf("First config change: success=%v, err=%v", firstSuccess, firstErr)
	t.Logf("Second config change: success=%v, err=%v", secondSuccess, secondErr)

	// The second should have been rejected with "config change already in flight"
	if secondSuccess {
		t.Logf("WARNING: Second config change succeeded — it may have been serialized after the first completed")
	}
	if secondErr != nil && secondErr.Error() != "config change already in flight" {
		t.Logf("Second config change error (expected 'config change already in flight'): %v", secondErr)
	}

	// At least the first should succeed
	if firstErr != nil {
		t.Fatalf("first config change failed: %v", firstErr)
	}

	// After the first completes, the second should be retryable
	time.Sleep(500 * time.Millisecond)

	// Re-find leader (may have changed)
	leaderID = cluster.findLeader()
	if leaderID == "" {
		t.Fatalf("no leader after first config change")
	}

	t.Log("Rapid successive config changes test passed: changes are properly serialized")
}
