package raft

import (
	"testing"
	"time"
)

func TestReadIndexStaleReadPrevention(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.stop()

	// Wait for leader election
	var leaderID string
	var leader *Raft
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		for id, node := range tc.nodes {
			if isLeader, _ := node.IsLeader(); isLeader {
				leaderID = id
				leader = node
				break
			}
		}
		if leader != nil {
			break
		}
	}

	if leader == nil {
		t.Fatalf("failed to elect initial leader")
	}

	// Write initial KV pair
	ok, err := leader.Propose("PUT", "stale_test_key", "valid_value")
	if err != nil || !ok {
		t.Fatalf("failed to propose initial key: %v", err)
	}

	// Confirm read works when leader is healthy
	val, ok, isLeader := leader.Get("stale_test_key")
	if !isLeader || !ok || val != "valid_value" {
		t.Fatalf("expected valid read on healthy leader, got val=%s ok=%v isLeader=%v", val, ok, isLeader)
	}

	// Partition leader away from majority peers
	partitionMap := make(map[string]bool)
	for id := range tc.nodes {
		partitionMap[id] = false // isolated
	}
	partitionMap[leaderID] = true // isolated leader can only talk to self
	leader.SetPartition(partitionMap)

	// Now issue GET to isolated leader with ReadIndex enabled (safe mode)
	leader.SetReadSafety("safe")

	// Read should fail or return isLeader=false because majority quorum cannot be confirmed
	valStale, okStale, isLeaderStale := leader.Get("stale_test_key")

	if isLeaderStale && okStale && valStale == "valid_value" {
		t.Fatalf("stale read flaw! Isolated leader returned data without majority heartbeat confirmation")
	}

	t.Logf("ReadIndex successfully prevented stale read on isolated leader (val=%s ok=%v isLeader=%v)", valStale, okStale, isLeaderStale)
}
