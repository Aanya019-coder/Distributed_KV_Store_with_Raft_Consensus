package raft

import (
	"testing"
	"time"
)

func TestPreVoteDisruption(t *testing.T) {
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

	initialTerm := leader.currentTerm
	t.Logf("Initial leader: %s (term %d)", leaderID, initialTerm)

	// Pick a follower to isolate
	var isolatedID string
	for id := range tc.nodes {
		if id != leaderID {
			isolatedID = id
			break
		}
	}

	isolatedNode := tc.nodes[isolatedID]

	// Partition follower away from all peers
	pMap := make(map[string]bool)
	for id := range tc.nodes {
		pMap[id] = false
	}
	pMap[isolatedID] = true
	isolatedNode.SetPartition(pMap)

	// Wait for several election timeouts so isolated follower triggers Pre-Vote attempts
	time.Sleep(2500 * time.Millisecond)

	// Verify isolated follower DID NOT advance term endlessly because Pre-Vote was rejected by peers
	termWhileIsolated := isolatedNode.currentTerm
	t.Logf("Isolated follower %s term during isolation: %d (initial term: %d)", isolatedID, termWhileIsolated, initialTerm)

	if termWhileIsolated > initialTerm+1 {
		t.Fatalf("Pre-Vote failure! Isolated node term climbed to %d while isolated", termWhileIsolated)
	}

	// Heal the partition
	for id := range tc.nodes {
		pMap[id] = true
	}
	isolatedNode.SetPartition(nil)

	// Wait a moment for reconnection
	time.Sleep(1000 * time.Millisecond)

	// Verify original leader and term are undisturbed
	currentLeaderID := ""
	var currentTerm int64
	for id, node := range tc.nodes {
		if isLeader, _ := node.IsLeader(); isLeader {
			currentLeaderID = id
			currentTerm = node.currentTerm
			break
		}
	}

	if currentLeaderID != leaderID {
		t.Fatalf("cluster leader disrupted by rejoining node! expected leader %s, got %s", leaderID, currentLeaderID)
	}

	if currentTerm != initialTerm {
		t.Fatalf("cluster term disrupted by rejoining node! expected term %d, got %d", initialTerm, currentTerm)
	}

	t.Logf("Pre-Vote verification passed! Leader %s and term %d remained undisturbed after follower reconnected", leaderID, currentTerm)
}
